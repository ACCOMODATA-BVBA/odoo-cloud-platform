import logging

import psycopg2

from odoo import api, models

_logger = logging.getLogger(__name__)

from odoo.addons.base_attachment_object_storage.models.ir_attachment import \
    clean_fs

from odoo.addons.base.models.ir_attachment import IrAttachment as BaseIrAttachment


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
    def _force_storage_to_object_storage_limited(self, limit=100):
        storage = self.env.context.get('storage_location') or self._storage()
        if self.is_storage_disabled(storage):
            return
        # The weird "res_field = False OR res_field != False" domain
        # is required! It's because of an override of _search in ir.attachment
        # which adds ('res_field', '=', False) when the domain does not
        # contain 'res_field'.
        # https://github.com/odoo/odoo/blob/9032617120138848c63b3cfa5d1913c5e5ad76db/odoo/addons/base/ir/ir_attachment.py#L344-L347
        domain = [
            ('store_fname', '!=', False),
            '!', ('store_fname', '=like', f'{storage}://%'),
            '|',
            ('res_field', '=', False),
            ('res_field', '!=', False)
        ]
        # We do a copy of the environment so we can workaround the cache issue
        # below. We do not create a new cursor by default because it causes
        # serialization issues due to concurrent updates on attachments during
        # the installation
        with self.do_in_new_env() as new_env:
            model_env = new_env['ir.attachment']
            groups = model_env.read_group(
                domain,
                ['checksum', 'store_fname'],
                ['checksum', 'store_fname'],
                lazy=True,
                limit=limit
            )
            group_data = [
                (g['checksum'], g['store_fname']) for g in groups
            ]

            _logger.debug('Start moving fnames: %s', ','.join(group_data))

            for checksum, store_fname in group_data:
                _logger.debug('Started processing "%s" - "%s"', checksum, store_fname)
                if not store_fname:
                    _logger.warning(
                        'Skipping atts with empty store_fname and checksum %s',
                        checksum
                    )
                    continue
                try:
                    with new_env.cr.savepoint():
                        # check that no other transaction has
                        # locked the row, don't send a file to storage
                        # in that case
                        self.env.cr.execute("SELECT id "
                                            "FROM ir_attachment "
                                            "WHERE checksum = %s "
                                            "FOR UPDATE NOWAIT",
                                            (checksum,),
                                            log_exceptions=False)
                        new_env.clear()
                        attachments = model_env.search([
                            ('checksum', '=', checksum),
                            ('store_fname', '=', store_fname),
                            '|',
                            ('res_field', '=', False),
                            ('res_field', '!=', False)

                        ])
                        _logger.debug(
                            'found %s records with fname "%s"',
                            len(attachments),
                            checksum,
                        )
                        path = attachments[0]._move_attachment_to_store()
                        vals = {
                            'store_fname': attachments[0].store_fname,
                            'mimetype': attachments[0].mimetype,
                            'db_datas': attachments[0].db_datas,
                        }
                        _logger.debug('Writing new data: %s', vals)
                        # use super().write to circumvent write function in ir_att
                        super(BaseIrAttachment, attachments).write(vals)
                        _logger.debug('cleaning path "%s"', path)
                        clean_fs([path])
                except psycopg2.OperationalError:
                    _logger.error('Could not migrate attachment %s to S3',
                                  checksum)

            return len(group_data)

    @api.model
    def _force_storage_to_object_storage(self, new_cr=False):
        _logger.info(
            'Not executing ir_attachment._force_storage_to_object_storage(), '
            'method disabled in this module'
        )
        return
