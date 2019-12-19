BIND9_TIMESTAMP %{MONTHDAY}[-]%{MONTH}[-]%{YEAR} %{TIME}

BIND9 %{BIND9_TIMESTAMP:timestamp} queries: %{LOGLEVEL:loglevel}: client %{IP:clientip}#%{POSINT:clientport} \(%{GREEDYDATA:query}\): query: %{GREEDYDATA:query} IN %{GREEDYDATA:querytype} \(%{IP:dns}\)
