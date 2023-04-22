import logging

import psycopg2

from odoo import api, models

_logger = logging.getLogger(__name__)

from odoo.addons.base_attachment_object_storage.models.ir_attachment import \
    clean_fs


class IrAttachment(models.Model):
    _inherit = 'ir.attachment'

    @api.model
    def job_force_storage_to_object_storage_limited(self):
        location = self.env.context.get('storage_location') or self._storage()
        # ignore if we are not using an object storage
        if location not in self._get_stores():
            return
        limit = int(self.env['ir.config_parameter'].get_param(
            'ir_attachment.transfer.chunk.size',
            default='10',
        ))
        count = self._force_storage_to_object_storage_limited(limit=limit)
        if count > 0:
            self.with_delay().job_force_storage_to_object_storage_limited()
        else:
            cron_job = self.env.ref(
                'base_attachment_move_queue_job.'
                'cron_enqueue_force_storage_to_object_storage_limited'
            )
            cron_job.active = False

    @api.model
    def _force_storage_to_object_storage_limited(self, limit=10):
        storage = self.env.context.get('storage_location') or self._storage()
        if self.is_storage_disabled(storage):
            return
        # The weird "res_field = False OR res_field != False" domain
        # is required! It's because of an override of _search in ir.attachment
        # which adds ('res_field', '=', False) when the domain does not
        # contain 'res_field'.
        # https://github.com/odoo/odoo/blob/9032617120138848c63b3cfa5d1913c5e5ad76db/odoo/addons/base/ir/ir_attachment.py#L344-L347
        domain = ['!', ('store_fname', '=like', '{}://%'.format(storage)),
                  '|',
                  ('res_field', '=', False),
                  ('res_field', '!=', False)]
        # We do a copy of the environment so we can workaround the cache issue
        # below. We do not create a new cursor by default because it causes
        # serialization issues due to concurrent updates on attachments during
        # the installation
        with self.do_in_new_env() as new_env:
            model_env = new_env['ir.attachment']
            ids = model_env.search(domain, limit=limit).ids
            _logger.info(
                'migrating %s files to the object storage', len(ids)
            )
            files_to_clean = []
            for attachment_id in ids:
                try:
                    with new_env.cr.savepoint():
                        # check that no other transaction has
                        # locked the row, don't send a file to storage
                        # in that case
                        self.env.cr.execute("SELECT id "
                                            "FROM ir_attachment "
                                            "WHERE id = %s "
                                            "FOR UPDATE NOWAIT",
                                            (attachment_id,),
                                            log_exceptions=False)

                        # This is a trick to avoid having the 'datas'
                        # function fields computed for every attachment on
                        # each iteration of the loop. The former issue
                        # being that it reads the content of the file of
                        # ALL the attachments on each loop.
                        new_env.clear()
                        attachment = model_env.browse(attachment_id)
                        path = attachment._move_attachment_to_store()
                        if path:
                            files_to_clean.append(path)
                except psycopg2.OperationalError:
                    _logger.error('Could not migrate attachment %s to S3',
                                  attachment_id)

            def clean():
                clean_fs(files_to_clean)

            # delete the files from the filesystem once we know the changes
            # have been committed in ir.attachment
            if files_to_clean:
                new_env.cr.after('commit', clean)

            return len(ids)

    @api.model
    def _force_storage_to_object_storage(self, new_cr=False):
        _logger.info(
            'Not executing ir_attachment._force_storage_to_object_storage(), '
            'method disabled in this module'
        )
        return
