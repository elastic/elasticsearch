# updated Zeek log matching, for legacy matching see the patters/ecs-v1/bro

ZEEK_BOOL [TF]
ZEEK_DATA [^\t]+

# http.log - the 'new' format (compared to BRO_HTTP)
# has *version* and *origin* fields added and *filename* replaced with *orig_filenames* + *resp_filenames*
ZEEK_HTTP %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{INT:[zeek][http][trans_depth]:int}\t(?:-|%{WORD:[http][request][method]})\t(?:-|%{ZEEK_DATA:[url][domain]})\t(?:-|%{ZEEK_DATA:[url][original]})\t(?:-|%{ZEEK_DATA:[http][request][referrer]})\t(?:-|%{NUMBER:[http][version]})\t(?:-|%{ZEEK_DATA:[user_agent][original]})\t(?:-|%{ZEEK_DATA:[zeek][http][origin]})\t(?:-|%{NUMBER:[http][request][body][bytes]:int})\t(?:-|%{NUMBER:[http][response][body][bytes]:int})\t(?:-|%{POSINT:[http][response][status_code]:int})\t(?:-|%{DATA:[zeek][http][status_msg]})\t(?:-|%{POSINT:[zeek][http][info_code]:int})\t(?:-|%{DATA:[zeek][http][info_msg]})\t(?:\(empty\)|%{ZEEK_DATA:[zeek][http][tags]})\t(?:-|%{ZEEK_DATA:[url][username]})\t(?:-|%{ZEEK_DATA:[url][password]})\t(?:-|%{ZEEK_DATA:[zeek][http][proxied]})\t(?:-|%{ZEEK_DATA:[zeek][http][orig_fuids]})\t(?:-|%{ZEEK_DATA:[zeek][http][orig_filenames]})\t(?:-|%{ZEEK_DATA:[http][request][mime_type]})\t(?:-|%{ZEEK_DATA:[zeek][http][resp_fuids]})\t(?:-|%{ZEEK_DATA:[zeek][http][resp_filenames]})\t(?:-|%{ZEEK_DATA:[http][response][mime_type]})
# :long - %{NUMBER:[http][request][body][bytes]:int}
# :long - %{NUMBER:[http][response][body][bytes]:int}

# dns.log - 'updated' BRO_DNS format (added *zeek.dns.rtt*)
ZEEK_DNS %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{WORD:[network][transport]}\t(?:-|%{INT:[dns][id]:int})\t(?:-|%{NUMBER:[zeek][dns][rtt]:float})\t(?:-|%{ZEEK_DATA:[dns][question][name]})\t(?:-|%{INT:[zeek][dns][qclass]:int})\t(?:-|%{ZEEK_DATA:[zeek][dns][qclass_name]})\t(?:-|%{INT:[zeek][dns][qtype]:int})\t(?:-|%{ZEEK_DATA:[dns][question][type]})\t(?:-|%{INT:[zeek][dns][rcode]:int})\t(?:-|%{ZEEK_DATA:[dns][response_code]})\t%{ZEEK_BOOL:[zeek][dns][AA]}\t%{ZEEK_BOOL:[zeek][dns][TC]}\t%{ZEEK_BOOL:[zeek][dns][RD]}\t%{ZEEK_BOOL:[zeek][dns][RA]}\t%{NONNEGINT:[zeek][dns][Z]:int}\t(?:-|%{ZEEK_DATA:[zeek][dns][answers]})\t(?:-|%{DATA:[zeek][dns][TTLs]})\t(?:-|%{ZEEK_BOOL:[zeek][dns][rejected]})

# conn.log - the 'new' format (requires *zeek.connection.local_resp*, handles `(empty)` as `-` for tunnel_parents, and optional mac adresses)
ZEEK_CONN %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][session_id]}\t%{IP:[source][ip]}\t%{INT:[source][port]:int}\t%{IP:[destination][ip]}\t%{INT:[destination][port]:int}\t%{WORD:[network][transport]}\t(?:-|%{ZEEK_DATA:[network][protocol]})\t(?:-|%{NUMBER:[zeek][connection][duration]:float})\t(?:-|%{INT:[zeek][connection][orig_bytes]:int})\t(?:-|%{INT:[zeek][connection][resp_bytes]:int})\t(?:-|%{ZEEK_DATA:[zeek][connection][state]})\t(?:-|%{ZEEK_BOOL:[zeek][connection][local_orig]})\t(?:-|%{ZEEK_BOOL:[zeek][connection][local_resp]})\t(?:-|%{INT:[zeek][connection][missed_bytes]:int})\t(?:-|%{ZEEK_DATA:[zeek][connection][history]})\t(?:-|%{INT:[source][packets]:int})\t(?:-|%{INT:[source][bytes]:int})\t(?:-|%{INT:[destination][packets]:int})\t(?:-|%{INT:[destination][bytes]:int})\t(?:-|%{ZEEK_DATA:[zeek][connection][tunnel_parents]})(?:\t(?:-|%{COMMONMAC:[source][mac]})\t(?:-|%{COMMONMAC:[destination][mac]}))?
# :long - %{INT:[zeek][connection][orig_bytes]:int}
# :long - %{INT:[zeek][connection][resp_bytes]:int}
# :long - %{INT:[zeek][connection][missed_bytes]:int}
# :long - %{INT:[source][packets]:int}
# :long - %{INT:[source][bytes]:int}
# :long - %{INT:[destination][packets]:int}
# :long - %{INT:[destination][bytes]:int}

# files.log - updated BRO_FILES format (2 new fields added at the end)
ZEEK_FILES_TX_HOSTS (?:-|%{IP:[server][ip]})|(?<[zeek][files][tx_hosts]>%{IP:[server][ip]}(?:[\s,]%{IP})+)
ZEEK_FILES_RX_HOSTS (?:-|%{IP:[client][ip]})|(?<[zeek][files][rx_hosts]>%{IP:[client][ip]}(?:[\s,]%{IP})+)
ZEEK_FILES %{NUMBER:timestamp}\t%{NOTSPACE:[zeek][files][fuid]}\t%{ZEEK_FILES_TX_HOSTS}\t%{ZEEK_FILES_RX_HOSTS}\t(?:-|%{ZEEK_DATA:[zeek][files][session_ids]})\t(?:-|%{ZEEK_DATA:[zeek][files][source]})\t(?:-|%{INT:[zeek][files][depth]:int})\t(?:-|%{ZEEK_DATA:[zeek][files][analyzers]})\t(?:-|%{ZEEK_DATA:[file][mime_type]})\t(?:-|%{ZEEK_DATA:[file][name]})\t(?:-|%{NUMBER:[zeek][files][duration]:float})\t(?:-|%{ZEEK_DATA:[zeek][files][local_orig]})\t(?:-|%{ZEEK_BOOL:[zeek][files][is_orig]})\t(?:-|%{INT:[zeek][files][seen_bytes]:int})\t(?:-|%{INT:[file][size]:int})\t(?:-|%{INT:[zeek][files][missing_bytes]:int})\t(?:-|%{INT:[zeek][files][overflow_bytes]:int})\t(?:-|%{ZEEK_BOOL:[zeek][files][timedout]})\t(?:-|%{ZEEK_DATA:[zeek][files][parent_fuid]})\t(?:-|%{ZEEK_DATA:[file][hash][md5]})\t(?:-|%{ZEEK_DATA:[file][hash][sha1]})\t(?:-|%{ZEEK_DATA:[file][hash][sha256]})\t(?:-|%{ZEEK_DATA:[zeek][files][extracted]})(?:\t(?:-|%{ZEEK_BOOL:[zeek][files][extracted_cutoff]})\t(?:-|%{INT:[zeek][files][extracted_size]:int}))?
# :long - %{INT:[zeek][files][seen_bytes]:int}
# :long - %{INT:[file][size]:int}
# :long - %{INT:[zeek][files][missing_bytes]:int}
# :long - %{INT:[zeek][files][overflow_bytes]:int}
# :long - %{INT:[zeek][files][extracted_size]:int}
