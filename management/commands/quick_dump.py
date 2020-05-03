import logging
import os

from django.core.management.base import (
    BaseCommand,
    CommandError
)

from django.db import (
    connection,
    models
)

from django.conf import settings
from django.core import serializers
from django.apps import apps


logger = logging.getLogger('django')


def _get_chunk_instances_from_queryset(qs, size=5000):
    start = 0
    count = qs.count()
    qs = qs.order_by('id')
    logger.info(f'count is: {count}')

    while start < count:
        logger.info(f'chunk start from: {start}')
        yield qs[start:start+size]
        start += size

    if count >= start:
        logger.info(f'chunk start from: {start}')
        yield qs[start:]


def _gen_queryset_with_auto_prefetch_from_model(m):
    prefetch_fields = []  # an array of field descriptor

    for field in m._meta.fields:

        if isinstance(field.remote_field, models.ManyToOneRel):

            if field.remote_field.model is m:
                continue

            prefetch_fields.append(field.name)

            foreign_model = field.remote_field.model
            for f in foreign_model._meta.fields:
                if isinstance(f.remote_field, models.ManyToOneRel):
                    prefetch_fields.append(f'{field.name}__{f.name}')

    for field in m._meta.many_to_many:
        prefetch_fields.append(f'{field.name}')

    return prefetch_fields


class Command(BaseCommand):

    help = 'create game init data as fixtures'

    def add_arguments(self, parser):
        parser.add_argument('args', metavar='models', nargs='*', help='Specify the custom model(s) to serialize for (app.models.model_name)')
        parser.add_argument('--save_to', help='Specify the position to save file (game) this will auto prefix with project_base. (projdir/game/[filename])',
                            dest='save_to')

    def get_save_position(self, path, fname):
        """This returns the absolute path for storing file. According to the input path's type
        it will behaivor a little different.

        Given path is not an absolute path. ex. game/fixture then you will get a path like
        =/proj_dir/game/fixture/fname=. Otherwise, you will get =/path/fname=

        """
        if not path.startswith('/'):
            return os.path.join(settings.BASE_DIR, path, fname)
        return os.path.join(path, fname)

    def handle(self, *models, **options):
        # command [args] [options]
        # Example usage:
        #    python manage.py dump_game_data game.playsettype
        # This accepts multi model input
        # Ex.
        #    python manage.py dump_game_data game.playsettype game.play

        path = options['save_to']
        if not path:
            raise CommandError("You must specify the storing path for --save_to")

        for model in models:
            app_name, model_name = model.split('.')
            m = apps.get_model(app_label=app_name, model_name=model_name)

            prefetch_fds = _gen_queryset_with_auto_prefetch_from_model(m)

            if len(prefetch_fds) > 0:
                qs = m.objects.prefetch_related(*prefetch_fds).all()
            else:
                qs = m.objects.all()

            # start to serialize the chunk and save in the file
            for index, chunk in enumerate(_get_chunk_instances_from_queryset(qs)):

                logger.info(f'start serailize the {model_name}')
                # NOTE: there is a performance issue with a manytomany field under the
                # serizlie method
                content = serializers.serialize('json', chunk, indent=2,
                                                ensure_ascii=False,
                                                use_natural_foreign_keys=True,
                                                use_natural_primary_keys=True)

                # NOTE: it will not raise any error or warning when the function(natural_key)
                # is not defined in the foreign key references to objects

                # save file with json stream instead of whole content once
                # https://docs.python.org/3.7/library/json.html
                # However, we don't need this currently because we've chunked the file already
                fname = f'{model_name}_{index}.json'
                save_path = self.get_save_position(path, fname)

                logger.info(f'ready to save the serialized result in file {save_path}')
                with open(save_path, 'w', encoding='utf8') as f:
                    f.write(content)
