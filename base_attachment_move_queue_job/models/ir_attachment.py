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
    def _force_storage_to_object_storage(self, new_cr=False):
        # method is called @installation time of the attachment object storage modules
        # For large file stores, this makes the module installation take forever.
        # Therefor, we install the module, and migrate attachments afterwards using
        # queue jobs
        _logger.info(
            'Not executing ir_attachment._force_storage_to_object_storage(), '
            'method disabled in this module'
        )
        return

    @api.model
    def job_force_storage_to_object_storage_limited(self):
        location = self.env.context.get('storage_location') or self._storage()
        # ignore if we are not using an object storage
        if location not in self._get_stores():
            return
        limit = int(self.env['ir.config_parameter'].get_param(
            'ir_attachment.transfer.chunk.size',
            default='100',
        ))
        count = self._force_storage_to_object_storage_limited(limit=limit)
        if count > 0:
            # launch job with low priority, should be run after other jobs
            # have completed
            self.with_delay(priority=999).job_force_storage_to_object_storage_limited()
        else:
            cron_job = self.env.ref(
                'base_attachment_move_queue_job.'
                'cron_enqueue_force_storage_to_object_storage_limited'
            )
            cron_job.active = False

    @api.model
    def _force_storage_to_object_storage_limited(self, limit=100):
        # create a batch of transfer jobs
        # This method does not take into account if jobs are already running
        # Should only be run after all transfer jobs have completed
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
        model_env = self.env['ir.attachment']
        groups = model_env.read_group(
            domain,
            ['checksum', 'store_fname'],
            ['checksum', 'store_fname'],
            lazy=False,
            limit=limit
        )
        group_data = [
            (g['checksum'], g['store_fname']) for g in groups
        ]

        for checksum, store_fname in group_data:
            # The transfer jobs get high priority, so they are all ran before
            # a new batch of transfer jobs is created
            self.with_delay(priority=5)._transfer_to_object_storage(checksum, store_fname)
        return len(group_data)

    def _transfer_to_object_storage(self, checksum, store_fname):
        _logger.debug('Started processing "%s" - "%s"', checksum, store_fname)
        if not store_fname:
            _logger.warning(
                'Skipping atts with empty store_fname and checksum %s',
                checksum
            )
            return "Skipping atts with empty store_fname and checksum %s" % checksum
        # check that no other transaction has locked the row, don't send a file to
        # storage in that case
        self.env.cr.execute("SELECT id "
                            "FROM ir_attachment "
                            "WHERE checksum = %s "
                            "FOR UPDATE NOWAIT",
                            (checksum,),
                            log_exceptions=False)
        self.env.clear()
        attachments = self.search([
            ('checksum', '=', checksum),
            ('store_fname', '=', store_fname),
            '|',
            ('res_field', '=', False),
            ('res_field', '!=', False)

        ])
        if not attachments:
            return "No attachments found to transfer"
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
            'checksum': checksum,
        }
        # use super().write to circumvent write function in ir_att
        # that write function prevents updating the store_fname field
        super(BaseIrAttachment, attachments).write(vals)
        _logger.debug('cleaning path "%s"', path)
        clean_fs([path])
        return "Transfer done, %s attachments moved" % len(attachments)
