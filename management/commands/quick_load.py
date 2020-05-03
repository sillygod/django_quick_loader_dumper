"""load_game_data

This is a high performance fixture loader compared to the django's load data. Without specifying the id, it
can dynamically generate the id sequence and auto find its relation.
"""


import logging
import os
from collections import defaultdict

from django.core.management.base import (
    BaseCommand,
    CommandError
)

from django.db import (
    connection,
    IntegrityError,
    transaction,
    models,
)

from django.conf import settings
from django.core import serializers


logger = logging.getLogger('django')


def _bulk_create_from_fixture(fname):
    """Using bulk create to reduce bunch of time consuming when loading
    data.

    References: https://docs.djangoproject.com/en/2.2/topics/serialization/#deserializing-data

    Note: In order to handle foreign key forward probelm this function will return the objs with
    deferred fields for further saving
    """
    datas = serializers.deserialize('json', open(fname, 'r'), handle_forward_references=True)
    # the logic of deserialize https://github.com/django/django/blob/master/django/core/serializers/base.py#L301

    orm_class_obj_map = defaultdict(list)
    for item in datas:
        # default item is a DeserializedObject defined in django serializers base.py
        obj = item.object
        orm_class_obj_map[obj.__class__].append(item)

    objs_with_deferred_fields = []

    for cls, objs in orm_class_obj_map.items():
        try:
            with_deferred = [obj for obj in objs if obj.deferred_fields]
            not_with_deferred = [obj for obj in objs if not obj.deferred_fields]
            objs_with_deferred_fields.extend(with_deferred)

            # get the raw django instance for bulk crete
            instances = []
            for item in objs:
                # If item has deferred_fields then we hack to set a 0 integer for it.
                # NOTE: We can ignore the m2m field because it will not be applied with not null constrain
                if item.deferred_fields:
                    for field, value in item.deferred_fields.items():
                        # Need to add a no meaning id to the field when it is not nullable and is foreign key
                        if not field.null:
                            if isinstance(field.remote_field, models.ManyToOneRel):
                                setattr(item.object, field.attname, 0)

                instances.append(item.object)

            cls.objects.bulk_create(instances, ignore_conflicts=True)
            # need to check models whether contain m2m data or not
            # we need to set the m2m data manually because it seems that the relation
            # won't be create when performing bulk create

            def set_m2m(cls, objs_not_with_deferred):
                fields_to_filter = {}

                if cls._meta.unique_together:
                    for f in cls._meta.unique_together:
                        fields_to_filter[f] = None
                else:
                    for field in cls._meta.fields:
                        if field.unique and field.attname != 'id':
                            fields_to_filter[field.attname] = None
                            break

                for obj in objs_not_with_deferred:
                    if obj.m2m_data:
                        for accessor_name, object_list in obj.m2m_data.items():
                            # In order to set the m2m field we need to retrieve the id here
                            for f in fields_to_filter:
                                fields_to_filter[f] = getattr(obj.object, f)

                            obj.object = cls.objects.get(**fields_to_filter)
                            getattr(obj.object, accessor_name).set(object_list)

            transaction.on_commit(lambda: set_m2m(cls, not_with_deferred))

        except IntegrityError:
            # try to update the db. However, I think it's not usual to update db
            # when you try to eastablish a brand new environment so temporarily
            # just raise here
            raise


    return objs_with_deferred_fields


def walk_under_directory(folder):
    result = []
    excludes = ['upload', 'scripts', 'static']
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if d not in excludes]
        result.append((root, dirs, files))

    return result

_cache_walk_directories = walk_under_directory(settings.BASE_DIR)


def _get_abs_path_of(fname):
    """autodiscover the file under all the django apps
    """
    fname = os.path.splitext(fname)[0]

    for root, dirs, files in _cache_walk_directories:
        for index, base_name in enumerate([os.path.splitext(f)[0] for f in files]):
            if fname == base_name:
                return os.path.join(root, files[index])

    raise Exception(f"{fname} not found in any app'sfolder")


class DisableForeignkeyConstrain:

    """A hack way to disable foreign key constrain for postgresql
    """

    def __init__(self, connection):
        self._conn = connection
        self._cursor = self._conn.cursor()
        self._tables = None

    def __enter__(self):
        self._cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';""")
        self._tables = [row[0] for row in self._cursor.fetchall()]

        # use defferred instead because google cloud sql postgresql doesn't support
        # superuser permission.
        # for table in self._tables:
        #     self._cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")
        self._cursor.execute("""SET CONSTRAINTS ALL DEFERRED;""")

        return self

    def __exit__(self, *exc_details):
        # query all sequence id and check it's start_number. then, I need to set it
        # to a feasible number if it is not set to number greater than the last id.
        self._cursor.execute("""SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';""")
        self._seqs = [row[0] for row in self._cursor.fetchall()]

        # performance enable all triger
        for table in self._tables:

            seq_name = f'{table}_id_seq'
            if seq_name in self._seqs:
                self._cursor.execute(f"""SELECT * FROM {seq_name};""")
                d = self._cursor.fetchall()
                # check the start which number it will be
                start_num = d[0][0]
                if start_num == 1:
                    self._cursor.execute(f""" select id from {table} order by id desc""")
                    d = self._cursor.fetchall()
                    if len(d) > 0:
                        pk = d[0][0]
                        self._cursor.execute(f""" ALTER SEQUENCE {seq_name} RESTART WITH {pk+1};""")


            # self._cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL;")

        self._cursor.close()


def _auto_find_chunk_files_with_abs_path(file_name):
    if not file_name.endswith('_*'):
        return [_get_abs_path_of(file_name)]

    base_name = file_name.split('_*')[0]
    files = []
    index = 0

    while True:
        fname = f'{base_name}_{index}'

        try:
            file_path = _get_abs_path_of(fname)
            files.append(file_path)
            index += 1
        except Exception:
            break

    return files


class Command(BaseCommand):

    help = 'load large data'

    def add_arguments(self, parser):
       parser.add_argument('args', metavar='files', nargs='*', help='Specify the file name to deserialize')
       parser.add_argument('--directory', help='specify the files under which directory can help find the files quickly', dest='directory', default='')

    def handle(self, *files, **options):
        objs_with_deferred_fields = []
        # temporarily hard code to default

        objs_with_self_deferred_fields = []
        # this is for the model contain the self references

        file_paths = []
        for f in files:
            file_paths.extend(_auto_find_chunk_files_with_abs_path(f))

        with transaction.atomic():
            with DisableForeignkeyConstrain(connection):
                for file_path in file_paths:
                    logger.info(f'start processing {file_path}')
                    deferred_objs = _bulk_create_from_fixture(file_path)
                    objs_with_deferred_fields.extend(deferred_objs)

                logger.info(f'establish all deferred fields')

                for obj in objs_with_deferred_fields:
                    try:
                        with transaction.atomic():
                            obj.save_deferred_fields()
                    except IntegrityError as e:
                        # It may encounter redundant object if you load game data
                        # with lots of different model (ex. common foreign model)
                        # so I bypass this error
                        # NOTE: some models contains self-reference foreign key so
                        # we should collect them and update it later
                        objs_with_self_deferred_fields.append(obj)


        if len(objs_with_self_deferred_fields) > 0:
            with transaction.atomic():
                # query the instances needed to update
                for obj in objs_with_self_deferred_fields:
                    cls = obj.object.__class__
                    fields_to_filter = {}

                    if cls._meta.unique_together:
                        for f in cls._meta.unique_together:
                            fields_to_filter[f] = None
                    else:
                        for field in cls._meta.fields:
                            if field.unique and field.attname != 'id':
                                fields_to_filter[field.attname] = None
                                break

                    for f in fields_to_filter:
                        fields_to_filter[f] = getattr(obj.object, f)


                    obj.object = cls.objects.get(**fields_to_filter)
                    obj.save_deferred_fields()


        if transaction.get_autocommit():
            connection.close()
