::::{tab-set}
:::{tab-item} Latest
:sync: latest
```sh subs=true
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:{{version.stack}} \
/app/bin/elastic-ingest \
-c /config/config.yml
```
% NOTCONSOLE
:::
:::{tab-item} Specific version
:sync: specific
Replace `<VERSION>` with your {{es}} version.
```sh
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:<VERSION> \
/app/bin/elastic-ingest \
-c /config/config.yml
```
% NOTCONSOLE
:::
::::
