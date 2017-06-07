/var/log/elasticsearch/*.log {
        daily
        rotate 14
        copytruncate
        compress
        missingok
        notifempty
}
