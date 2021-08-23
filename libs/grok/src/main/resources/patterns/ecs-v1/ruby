RUBY_LOGLEVEL (?:DEBUG|FATAL|ERROR|WARN|INFO)
RUBY_LOGGER [DFEWI], \[%{TIMESTAMP_ISO8601:timestamp} #%{POSINT:[process][pid]:int}\] *%{RUBY_LOGLEVEL:[log][level]} -- +%{DATA:[process][name]}: %{GREEDYDATA:message}
