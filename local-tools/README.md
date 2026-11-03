# Local query playground

Run these commands from `/Users/kishor/work/elasticsearch`:

```sh
# Current local branch, including uncommitted edits
python3 local-tools/query-stack.py local

# Current branch with IntelliJ debugging
python3 local-tools/query-stack.py local --debug

# Latest upstream/main in a separate checkout
python3 local-tools/query-stack.py main

# Latest upstream/main with debugging
python3 local-tools/query-stack.py main --debug
```

The launcher builds and starts one Elasticsearch node on port 9200 (transport
9300), with security disabled, a trial license, a 7 GB heap, assertions disabled,
and the EIS QA URL. It waits for cluster health, starts the downloaded Kibana
9.4.4 if port 5601 is free, waits for Kibana to become available, performs Vault
OIDC login, and runs `node scripts/eis.js` in `/Users/kishor/work/kibana`.
Complete the Vault login in your browser when prompted. Vault saves the token
through its normal token helper; the launcher suppresses token output.

The `:run` task manages the cluster name (`runTask`) and ports (9200/9300).
Do not pass `-Dtests.es.cluster.name`, `-Dtests.es.http.port`, or
`-Dtests.es.transport.port`: Testclusters rejects those reserved settings.

Open <http://localhost:5601/app/dev_tools#/console> for queries. Keep the launcher
running. Ctrl+C stops Elasticsearch and any Kibana it started. A preexisting
Kibana is reused and stays running; it must already point to localhost:9200
(the installed Kibana configuration currently does). Occupied Elasticsearch
or debug ports cause an error rather than reusing an unrelated cluster.

## IntelliJ

Create a **Remote JVM Debug** run configuration, select **Attach to remote JVM**,
host **localhost**, port **5005**, and select the module containing your query
code. Run the launcher with `--debug`, set breakpoints, then start the IntelliJ
debug configuration and send queries from Kibana. Elasticsearch starts without
waiting for the debugger. Use `--debug-port 5006` to choose another port.

This uses a Gradle init script to add JDWP only to the server JVM. The repository's
native `--debug-jvm` instead connects to an IntelliJ listener on port 5007 and
suspends until connected. Putting JDWP in `tests.jvm.argline` would also debug
the launcher CLI JVM and cause port conflicts.

When debugging `main`, open the checkout printed by the launcher in IntelliJ
to match the running code. Your existing local branch stays in place.

## Requirements and options

- Use the JDK required by the selected Elasticsearch branch through `JAVA_HOME`.
  The launcher leaves JDK selection to the Gradle wrapper and your environment.
- Install Git, Python 3, Vault, and the Node version in Kibana's `.nvmrc`.
  The launcher prefers the matching installation under `~/.nvm/versions/node`,
  then checks `node` from PATH. It does not install dependencies.
- The existing Kibana source checkout must be bootstrapped for `scripts/eis.js`.
- `--dry-run` prints the plan without fetching, starting services, or logging in.
- `--skip-eis` skips Vault and EIS setup.
- `--heap 4g` overrides the heap size.
- `--timeout 2400` changes the readiness deadline per service (default 1800s).

The `main` checkout is retained in `.gradle/query-stack/upstream-main` and updated
to a freshly fetched `upstream/main` on each run. Updates refuse to overwrite
edits in that checkout. Gradle's normal `:run` data lifecycle applies; this is a
query playground, and data is not guaranteed to persist between runs. Builds
can take several minutes on the first run. Elasticsearch and Kibana versions
must be compatible; the launcher reports failure if Kibana cannot become ready
against the selected branch.

Logs are in `.gradle/query-stack/logs/<timestamp>/`. For example:

```sh
tail -f .gradle/query-stack/logs/<timestamp>/elasticsearch.log
```

Only one launcher can run at a time. Service startup or EIS failures stop services
started by this invocation and return a nonzero exit code. No services are killed
by name, and no worktree or data directory is deleted.
