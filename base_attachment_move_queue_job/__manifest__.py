{
    "name": "Base Attachment move queue job",
    "summary": "Base helper module for the implementation of external object "
               "store. Uses queue jobs to transfer attachments to their new "
               "location",
    "version": "15.0.0.0.2",
    "author": "Accomodata",
    "license": "AGPL-3",
    "category": "Knowledge Management",
    "depends": [
        "base_attachment_object_storage",
        "queue_job",
    ],
    "website": "https://www.accomodata.be",
    "data": [
        "data/ir_cron.xml",
        "data/queue_job_channel.xml",
        "data/queue_job_function.xml",
        "data/res_config_settings_data.xml",
    ],
    "installable": True,
    "auto_install": True,
}
