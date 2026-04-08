# Elasticsearch RBAC Reference

## Mental Model

Elasticsearch security has two halves: **authentication** (who are you?) and **authorization** (what can you do?). This document focuses on authorization — the RBAC model.

### The building blocks

**User** — an identity. Can be a native ES user, an LDAP user, a SAML-authenticated SSO user, or an API key. Every request has exactly one authenticated user.

**Role** — a named bundle of permissions. A user can have multiple roles. Defined via `PUT /_security/role/{name}` or in `roles.yml`.

**Privilege** — a named permission within a role. Privileges come in three flavors: cluster, index, and application. Each privilege is ultimately a **pattern that matches transport action names**.

**Transport action** — every operation in ES has a name like `cluster:admin/xpack/inference/put` or `indices:data/read/search`. These are the atoms of authorization — every privilege check reduces to "does this action name match any of the user's privilege patterns?"

### How it fits together

```
User "alice"
  └── has roles: ["data_analyst", "ml_viewer"]

Role "data_analyst"
  ├── cluster privileges: ["monitor"]                    ← can do cluster:monitor/*
  ├── index privileges:
  │     └── names: ["logs-*", "metrics-*"]
  │         privileges: ["read"]                         ← can do indices:data/read/* on logs-*/metrics-*
  └── application privileges:
        └── kibana: ["feature_discover.all"]              ← Kibana-specific

Role "ml_viewer"
  ├── cluster privileges: ["monitor_ml"]                 ← can do cluster:monitor/xpack/ml/*
  └── index privileges:
        └── names: [".ml-anomalies-*"]
            privileges: ["read"]                         ← can read ML anomaly results

Effective privileges = union of both roles:
  - cluster: monitor + monitor_ml
  - index: read on logs-*, metrics-*, .ml-anomalies-*
  - application: Kibana discover
```

When Alice runs `FROM logs-2026.03.* | STATS count() BY region`, ES checks:
1. The ESQL query action (`indices:data/read/esql`) — does she have `read` on `logs-2026.03.*`? Yes (matches `logs-*`). Allowed.

When Alice tries `PUT /_ml/anomaly_detectors/my_job` (create ML job):
1. The action is `cluster:admin/xpack/ml/put_job`. Does she have a cluster privilege matching this? `monitor` matches `cluster:monitor/*` — no. `monitor_ml` matches `cluster:monitor/xpack/ml/*` — no (it's `admin`, not `monitor`). Denied.

### Three privilege levels

**Cluster privileges** — gate cluster-wide operations. Not scoped to any index.

Key cluster privileges (from `ClusterPrivilegeResolver.java`):

| Privilege | What it allows | Pattern |
|-----------|---------------|---------|
| `all` | Everything including security | `*` |
| `manage` | Everything EXCEPT security | `cluster:admin/*` minus `cluster:admin/xpack/security/*` |
| `monitor` | Read-only cluster health, stats, node info, templates | `cluster:monitor/*` |
| `manage_security` | Full security admin (users, roles, API keys, tokens) | `cluster:admin/xpack/security/*` |
| `read_security` | Read-only view of users, roles, API keys | `cluster:admin/xpack/security/*/get*`, `*/has*` |
| `manage_api_key` | Create, get, invalidate any API key | `cluster:admin/xpack/security/api_key/*` |
| `manage_own_api_key` | Manage only your own API keys | Same pattern + request predicate (checks ownership) |
| `manage_ml` | Full CRUD on ML jobs, data feeds, trained models | `cluster:admin/xpack/ml/*` |
| `monitor_ml` | View ML jobs and results | `cluster:monitor/xpack/ml/*` |
| `manage_inference` | Create/delete/update inference endpoints | `cluster:admin/xpack/inference/*` |
| `monitor_inference` | Use inference endpoints, view config | `cluster:monitor/xpack/inference*` |
| `manage_transform` | Full CRUD on transforms | `cluster:admin/transform/*` |
| `monitor_transform` | View transforms | `cluster:monitor/transform/*` |
| `manage_connector` | Full CRUD on connectors (EXCLUDES secrets) | `cluster:admin/xpack/connector/*` minus `*/secret/*` |
| `monitor_connector` | View connectors | Specific get/list actions |
| `read_connector_secrets` | Read connector secrets | `cluster:admin/xpack/connector/secret/get` |
| `write_connector_secrets` | Create/update/delete connector secrets | `cluster:admin/xpack/connector/secret/post`, `*/put`, `*/delete` |
| `manage_index_templates` | CRUD on index templates, component templates | `indices:admin/template/*`, `cluster:admin/component_template/*` |
| `manage_ingest_pipelines` | CRUD on ingest pipelines | `cluster:admin/ingest/pipeline/*` |
| `read_pipeline` | View ingest pipelines | `cluster:admin/ingest/pipeline/get`, `*/simulate` |
| `manage_ilm` | Manage index lifecycle policies | `cluster:admin/ilm/*` |
| `manage_slm` | Manage snapshot lifecycle | `cluster:admin/slm/*` |
| `manage_enrich` | Manage enrich policies | `cluster:admin/xpack/enrich/*` |
| `manage_watcher` | Full CRUD on watches/alerts | `cluster:admin/xpack/watcher/*` |
| `create_snapshot` | Create snapshots, view repos | `cluster:admin/snapshot/*` |
| `manage_ccr` | Manage cross-cluster replication | `cluster:admin/xpack/ccr/*` |
| `cross_cluster_search` | Execute CCS queries | Handshake + remote cluster nodes + ESQL exchange |
| `monitor_esql` | Monitor ESQL operations | `cluster:monitor/xpack/esql/*` |
| `cancel_task` | Cancel running tasks | `cluster:admin/tasks/cancel` |

The hierarchy: `all` > `manage` (all minus security) > feature-specific `manage_*` > feature-specific `monitor_*`. The `manage_*` / `monitor_*` split is the standard pattern: manage = full CRUD, monitor = read-only.

**Index privileges** — gate per-index operations. Scoped to **index name patterns** (wildcards allowed).

Key index privileges:

| Privilege | What it allows |
|-----------|---------------|
| `all` | Everything on the matched indices |
| `manage` | Admin + monitor (create/delete index, change mappings/settings, force merge, etc.) |
| `read` | Search, get, scroll, field caps, resolve index |
| `write` | Index, update, delete, bulk |
| `create_doc` | Index with op_type=create only (no updates/deletes) |
| `index` | Index documents + bulk (no delete) |
| `delete` | Delete documents |
| `create_index` | Create indices and data streams |
| `delete_index` | Delete indices and data streams |
| `view_index_metadata` | See index names, mappings, settings, aliases (no data access) |
| `monitor` | Index stats, segments, recovery info |
| `maintenance` | Refresh, flush, force merge |
| `manage_view` | CRUD on ESQL views (feature-flagged) |

**Application privileges** — for external apps (primarily Kibana) to define their own privilege model. Stored in `.security` index. ES handles the pattern matching; the app defines what the names mean. Example: Kibana registers `feature_discover.all`, `feature_dashboard.read`, and checks them via `_has_privileges`.

### Concrete role examples

**A read-only analyst:**
```json
PUT /_security/role/analyst
{
  "cluster": ["monitor"],
  "indices": [{
    "names": ["logs-*", "metrics-*"],
    "privileges": ["read", "view_index_metadata"]
  }]
}
```
Can: search logs and metrics, view cluster health. Cannot: write data, create indices, manage ML.

**An ML admin:**
```json
PUT /_security/role/ml_admin
{
  "cluster": ["manage_ml", "monitor"],
  "indices": [
    { "names": [".ml-*"], "privileges": ["read", "manage"] },
    { "names": ["logs-*"], "privileges": ["read"] }
  ]
}
```
Can: create/delete ML jobs, read ML results, read logs as training data. Cannot: write to logs, manage security.

**An inference endpoint manager with restricted document access:**
```json
PUT /_security/role/inference_ops
{
  "cluster": ["manage_inference"],
  "indices": [{
    "names": ["customer-data-*"],
    "privileges": ["read"],
    "field_security": { "grant": ["name", "email", "category"], "except": ["ssn"] },
    "query": "{\"term\": {\"region\": \"us-east\"}}"
  }]
}
```
Can: manage inference endpoints. Can read customer data, but only in `us-east` region (DLS) and without SSN field (FLS).

**A connector admin with secret access:**
```json
PUT /_security/role/connector_admin
{
  "cluster": ["manage_connector", "write_connector_secrets", "read_connector_secrets"]
}
```
Can: full CRUD on connectors AND their secrets. Note that `manage_connector` alone does NOT include secrets — they're explicitly separated.

### Key design principles

1. **Union semantics**: Multiple roles = union of all privileges. There is no "deny" — you can't subtract permissions. If any role allows it, it's allowed.
2. **Least privilege**: The built-in roles follow admin/user splits. `inference_admin` = manage + monitor. `inference_user` = monitor only.
3. **Index patterns are wildcards**: `logs-*` matches `logs-2026.03.25`. Patterns share namespace with indices, aliases, data streams, views — and potentially datasets.
4. **Cluster privileges are coarse**: `manage_ml` grants ALL ML operations. You can't grant "create job" but deny "delete job" via named privileges (you'd need raw action patterns for that).
5. **Secrets are always separated**: The connector model (`manage_connector` excludes secrets) is the established pattern. Secret access requires explicit separate privileges.

## Architecture

Authorization is **automaton-based**. Every privilege compiles to a Lucene `Automaton` that matches transport action name strings. This is the same finite-state automaton engine Lucene uses for regex and wildcard queries — repurposed for security pattern matching.

### How Lucene Automatons Work in Security

The `Automatons` utility class (`x-pack/plugin/core/.../security/support/Automatons.java`) converts privilege patterns into deterministic finite automata:

1. **Pattern compilation**: A privilege like `manage_inference` is defined as a set of action name patterns: `"cluster:admin/xpack/inference/*"`, `"cluster:monitor/xpack/inference*"`. Each pattern is compiled into an `Automaton` via `Automatons.patterns()` (line 96).

2. **Wildcard expansion**: `*` in patterns becomes "match any string" in the automaton. `cluster:admin/xpack/inference/*` accepts `cluster:admin/xpack/inference/put`, `cluster:admin/xpack/inference/delete`, etc.

3. **Set operations**: Multiple privileges are combined via `unionAndMinimize()` (OR — any pattern matches), `minusAndMinimize()` (exclusion — match A but not B), and `intersectAndMinimize()` (AND). For example, `manage` = `all` MINUS security patterns.

4. **Predicate creation**: `Automatons.predicate(automaton)` creates a `Predicate<String>` backed by `CharacterRunAutomaton` (line 332). This is a compiled, O(n) string matcher — no regex backtracking, no hash lookups. Testing whether an action name matches a privilege is a single linear scan of the action string.

5. **Caching**: Compiled automatons are cached (configurable: `xpack.security.automata.cache.size` = 10,000, TTL = 48h) to avoid recompilation.

6. **Subset checking**: `Automatons.subsetOf(a1, a2)` tests whether one privilege is a subset of another — used for application privilege checking and privilege hierarchy validation.

### Two types of permission checks

- **`AutomatonPermissionCheck`** (`ClusterPermission.java:227`): Pure action-name matching. Tests `automaton.run(action)`. Used for most cluster privileges.
- **`ActionRequestBasedPermissionCheck`** (`ClusterPermission.java:251`): Action-name matching AND a request-level predicate. Used when the action name alone isn't sufficient — e.g., `manage_own_api_key` matches the API key action names but also checks that the request targets the authenticated user's own keys.

### The authorization flow

```
REST request
  → NodeClient dispatches transport action (e.g., "cluster:admin/xpack/inference/put")
  → SecurityActionFilter.apply() [order = Integer.MIN_VALUE — runs first]
    → AuthorizationService.authorize()
      → Is it a cluster action? (starts with "cluster:" or "indices:admin/template")
        → RBACEngine.authorizeClusterAction()
          → Role.checkClusterAction(action, request, authentication)
            → ClusterPermission.check() — iterate PermissionCheck list
              → AutomatonPermissionCheck.check(action) — automaton.run(action)
      → Is it an index action? (starts with "indices:")
        → Resolve target indices
        → RBACEngine.authorizeIndexAction()
          → IndicesPermission — match action + index name patterns
```

**Key files:**
- `Automatons.java` — pattern→automaton compilation, set operations, caching
- `ClusterPrivilegeResolver.java` — all named cluster privileges (single file, no plugin registration API)
- `ClusterPermission.java` — `AutomatonPermissionCheck` and `ActionRequestBasedPermissionCheck`
- `IndexPrivilege.java` — all named index privileges
- `RoleDescriptor.java` — role structure definition
- `AuthorizationService.java` — the authorization pipeline
- `RBACEngine.java` — cluster and index authorization logic
- `SecurityActionFilter.java` — intercepts all transport actions

## Privilege Types

### Cluster Privileges

Gate management and monitoring operations. Registered as constants in `ClusterPrivilegeResolver.java` with transport action name patterns.

| Category | Privileges | Pattern example |
|----------|-----------|-----------------|
| Global | `all`, `manage`, `monitor` | `cluster:*`, `cluster:admin/*` (minus security), `cluster:monitor/*` |
| ML | `manage_ml`, `monitor_ml` | `cluster:admin/xpack/ml/*` |
| Inference | `manage_inference`, `monitor_inference` | `cluster:admin/xpack/inference/*` |
| Transform | `manage_transform`, `monitor_transform` | `cluster:admin/transform/*` |
| Connectors | `manage_connector`, `monitor_connector` | `cluster:admin/xpack/connector/*` (minus secrets) |
| Connector secrets | `read_connector_secrets`, `write_connector_secrets` | `cluster:admin/xpack/connector/secret/*` |
| Watcher | `manage_watcher`, `monitor_watcher` | `cluster:admin/xpack/watcher/*` |
| Security | `manage_security`, `read_security`, `manage_api_key`, `manage_own_api_key` | `cluster:admin/xpack/security/*` |
| ILM/SLM | `manage_ilm`, `read_ilm`, `manage_slm`, `read_slm` | `cluster:admin/ilm/*` |
| Enrich | `manage_enrich`, `monitor_enrich` | `cluster:admin/xpack/enrich/*` |
| Templates | `manage_index_templates` | `indices:admin/template/*` |
| Pipelines | `manage_ingest_pipelines`, `read_pipeline` | `cluster:admin/ingest/pipeline/*` |
| Search | `manage_search_application`, `manage_search_synonyms`, `manage_search_query_rules` | Various |
| Snapshots | `create_snapshot`, `monitor_snapshot` | `cluster:admin/snapshot/*` |
| CCS/CCR | `cross_cluster_search`, `cross_cluster_replication`, `manage_ccr`, `read_ccr` | Various |
| ESQL | `monitor_esql` | `cluster:monitor/xpack/esql/*` |
| Fleet | `read_fleet_secrets`, `write_fleet_secrets` | `cluster:admin/fleet/secrets/*` |

### Index Privileges

Gate data access on specific index patterns.

| Privilege | What it grants |
|-----------|---------------|
| `all` | Everything (`indices:*`) |
| `manage` | `indices:admin/*` + `indices:monitor/*` |
| `read` | `indices:data/read/*` + resolve + field caps |
| `write` | `indices:data/write/*` |
| `index` | Write index docs + bulk |
| `create_doc` | Write with op_type=create only |
| `create_index` | Create index, data stream |
| `delete_index` | Delete index, data stream |
| `view_index_metadata` | Get aliases, mappings, settings |
| `monitor` | `indices:monitor/*` |
| `maintenance` | Refresh, flush, force merge |
| `manage_view` | ESQL view CRUD (feature-flagged) |
| `create_view`, `delete_view`, `read_view_metadata` | Individual ESQL view operations |

### Application Privileges

For external applications (primarily Kibana) to define their own privilege model. Stored in `.security` index via `PUT /_security/privilege/{application}/{privilege}`. Roles grant them via the `applications` block. Checked via `_has_privileges` API. The application defines what the privilege names mean — ES just does the matching.

## Role Structure

A role (`RoleDescriptor.java`) is a JSON object with these top-level fields:

```json
{
  "cluster": ["monitor", "manage_ingest_pipelines"],
  "indices": [{
    "names": ["logs-*"],
    "privileges": ["read"],
    "field_security": { "grant": ["message", "@timestamp"], "except": ["host.ip"] },
    "query": "{\"match\": {\"department\": \"engineering\"}}",
    "allow_restricted_indices": false
  }],
  "applications": [{
    "application": "kibana-.kibana",
    "privileges": ["feature_discover.all"],
    "resources": ["space:default"]
  }],
  "run_as": ["reporting_user"],
  "remote_indices": [{ "clusters": ["cluster_one"], "names": ["logs-*"], "privileges": ["read"] }],
  "remote_cluster": [{ "clusters": ["cluster_one"], "privileges": ["monitor_enrich"] }],
  "global": { ... },
  "metadata": {},
  "description": "..."
}
```

### Index Privilege Entry Structure

Each entry in the `indices` array is an `IndicesPrivileges` object (`RoleDescriptor.java:1321`). It has six fields:

```java
// RoleDescriptor.java:1325-1333
private String[] indices;                    // index name patterns (compiled to automaton)
private String[] privileges;                 // action name patterns (compiled to automaton)
private String[] grantedFields = null;       // FLS whitelist (null = all fields)
private String[] deniedFields = null;        // FLS blacklist (subset of granted)
private BytesReference query;                // DLS query (ES query DSL, null = all docs)
private boolean allowRestrictedIndices = false; // include system indices in pattern matching
```

The authorization check for an index action is: **does the action name match `privileges` AND does the target index match `indices`?** Both are automaton checks. The remaining fields (FLS, DLS, `allowRestrictedIndices`) modify what data is visible after access is granted.

### `allowRestrictedIndices`

A boolean modifier on how index name patterns are resolved. **Not a privilege and not an index name — a flag on the matching behavior.**

- **`false` (default)**: System/restricted indices (`.security`, `.kibana`, `.ml-*`, `.watches`, etc.) are excluded from wildcard matches. `"*"` does not match `.security`.
- **`true`**: System indices ARE included. `"*"` matches everything including `.security`.

From the code comment (`RoleDescriptor.java:1330-1332`):
> *"by default certain restricted indices are exempted when granting privileges, as they should generally be hidden for ordinary users. Setting this flag eliminates this special status, and any index name pattern in the permission will cover restricted indices as well."*

Only built-in roles use `allowRestrictedIndices: true` — `superuser`, `kibana_system`, `snapshot_user`, `remote_monitoring_collector`, and feature-specific roles that need access to their own system indices (`.ml-*`, `.watches`, etc.).

### Document-Level Security (DLS)

The `query` field on `IndicesPrivileges` — an ES query DSL filter. Only matching documents are visible. Applied at the Lucene reader level. Supports Mustache templates (`{{_user.username}}`).

### Field-Level Security (FLS)

The `field_security` object with `grant` (whitelist) and `except` (blacklist within granted). Applied at the Lucene `DirectoryReader` level — restricted fields are masked before reaching the application. `except` must be a subset of `grant` (validated by `checkIfExceptFieldsIsSubsetOfGrantedFields` in `RoleDescriptor.java`).

### Configurable Cluster Privileges (`global` field)

Parameterized cluster privileges — cluster-level privileges that also check the request content, not just the action name. Implemented as `ActionRequestBasedPermissionCheck` (automaton + request predicate).

Three types exist today:
- `ManageApplicationPrivileges` — manage app privilege definitions, scoped to application name patterns
- `WriteProfileDataPrivileges` — update user profile data, scoped to application names
- `ManageRolesPrivilege` — manage roles, scoped to specific index patterns and privileges

These are the only mechanism for fine-grained cluster privileges that depend on the request payload (not just the action name).

## How Features Register Privileges

**There is no plugin-level registration API.** All named privileges are hardcoded in `ClusterPrivilegeResolver.java`. The pattern for adding a new feature:

1. Choose a transport action namespace: `cluster:admin/xpack/myfeature/*`
2. Add a `NamedClusterPrivilege` constant in `ClusterPrivilegeResolver` with a wildcard pattern
3. Add it to the `VALUES` map
4. REST actions call `client.execute(MyAction.INSTANCE, request)` where `MyAction.NAME = "cluster:admin/xpack/myfeature/put"`
5. The security filter matches the action name against the privilege's automaton

### Feature privilege patterns (precedent)

| Feature | Manage privilege | Monitor privilege | Secrets handling |
|---------|-----------------|-------------------|-----------------|
| Inference | `manage_inference` | `monitor_inference` | Stored in `.secrets-inference` (plaintext) |
| Connectors | `manage_connector` (excludes secrets) | `monitor_connector` | Separate `read/write_connector_secrets` privileges |
| Transforms | `manage_transform` | `monitor_transform` | N/A |
| ML | `manage_ml` | `monitor_ml` | N/A |
| Watcher | `manage_watcher` | `monitor_watcher` | Encrypted via CryptoService |

**Notable: Connectors separate secrets from management.** `manage_connector` explicitly excludes the `CONNECTOR_SECRETS_PATTERN` (line 386-387 of `ClusterPrivilegeResolver`). Secrets require `read_connector_secrets` or `write_connector_secrets`.

## Built-in Roles (relevant examples)

| Role | Cluster | Index | Pattern |
|------|---------|-------|---------|
| `inference_admin` | `manage_inference` | — | Full CRUD on inference endpoints |
| `inference_user` | `monitor_inference` | — | Use inference, no CRUD |
| `transform_admin` | `manage_transform` | Read transform audit indices | Full CRUD on transforms |
| `transform_user` | `monitor_transform` | Read transform audit indices | View transforms, no CRUD |
| `watcher_admin` | `manage_watcher` | Read `.watches` | Full CRUD on watches |
| `enrich_user` | `manage_enrich` | Read/manage `.enrich-*` | Full CRUD on enrich policies |

The admin/user split is the standard pattern: admin = manage + monitor, user = monitor only.

## Implementation: The Call Chain

### Component tree

```
SecurityActionFilter (ActionFilter, order=MIN_VALUE)
  ├── AuthenticationService        — resolves identity (who is this?)
  └── AuthorizationService         — checks privileges (can they do this?)
        ├── IndicesAndAliasesResolver  — resolves index name patterns to concrete indices
        ├── RBACEngine (AuthorizationEngine)  — the actual privilege checker
        │     └── uses Role (SimpleRole)
        │           ├── ClusterPermission  — compiled cluster privileges
        │           │     └── List<PermissionCheck>
        │           │           ├── AutomatonPermissionCheck     — pure action-name automaton
        │           │           └── ActionRequestBasedPermissionCheck — automaton + request predicate
        │           ├── IndicesPermission   — compiled index privileges
        │           └── ApplicationPermission — compiled application privileges
        └── CompositeRolesStore    — loads and merges RoleDescriptors into a single Role
              ├── NativeRolesStore      — roles from .security index (PUT /_security/role)
              ├── FileRolesStore        — roles from roles.yml
              └── ReservedRolesStore    — built-in roles (superuser, kibana_system, etc.)
```

### Detailed call chain for a cluster action

Example: user calls `PUT /_inference/openai/my-endpoint` → transport action `cluster:admin/xpack/inference/put`.

**1. SecurityActionFilter.apply()** (`SecurityActionFilter.java:82`)

The filter intercepts every transport action (order = `Integer.MIN_VALUE` — runs before all other filters). It:
- Checks license state (line 97)
- Handles system user substitution (line 111)
- Maps the action name via `SecurityActionMapper.action()` (line 166) — usually a no-op, only remaps 3 edge cases
- Calls `authcService.authenticate()` (line 168) → resolves the user identity
- Calls `authzService.authorize()` (line 184) → checks privileges

**2. AuthorizationService.authorize()** (`AuthorizationService.java:300`)

- Extracts authentication from the request context
- Clears transient authorization headers (line 317)
- Checks operator privileges (line 342)
- If `SystemUser`, grants immediately (line 348)
- Loads the user's roles via `CompositeRolesStore.getRoles()` → produces `AuthorizationInfo` containing the merged `Role`
- Calls `authorizeAction()` (line 464/475)

**3. AuthorizationService.authorizeAction()** (`AuthorizationService.java:479`)

The critical dispatch:
- `ClusterPrivilegeResolver.isClusterAction(action)` (line 491) — tests if the action name starts with `cluster:` or matches `indices:admin/template/*`
  - **Yes** → calls `authzEngine.authorizeClusterAction()` (line 499)
- `isIndexAction(action)` (line 511) — tests if the action name starts with `indices:`
  - **Yes** → resolves target indices via `IndicesAndAliasesResolver`, then calls `authzEngine.authorizeIndexAction()` (line 541)
- **Neither** → denied (line 558)

**4. RBACEngine.authorizeClusterAction()** (`RBACEngine.java:206`)

- Extracts the `Role` from `RBACAuthorizationInfo` (line 212)
- Calls `role.checkClusterAction(action, request, authentication)` (line 213)
- If denied, falls back to same-user privilege check (line 215) — allows self-service operations like change-password
- If still denied → `AuthorizationResult.deny()` (line 222)

**5. SimpleRole.checkClusterAction()** (`SimpleRole.java:168`)

One-liner: `return cluster.check(action, request, authentication);`

Delegates to the `ClusterPermission` object, which was compiled from the role's cluster privilege strings during role loading.

**6. ClusterPermission.check()** (`ClusterPermission.java:46`)

Iterates through the `List<PermissionCheck>` (line 47-52):
```java
for (PermissionCheck permission : checks) {
    if (permission.check(action, request, authentication)) {
        return true;
    }
}
return false;
```

Two check types:
- **AutomatonPermissionCheck** (line 227): Runs `automaton.run(action)` — a single O(n) scan through the action string. Used for most privileges.
- **ActionRequestBasedPermissionCheck** (line 251): Runs the automaton check AND a `requestPredicate.test(request)` — for privileges like `manage_own_api_key` that need to inspect the request.

### Detailed call chain for an index action

Example: user runs `FROM logs-2026.03.* | LIMIT 10` → transport action `indices:data/read/esql`.

**1-3.** Same as above through `authorizeAction()`.

**4. AuthorizationService dispatches to index path** (`AuthorizationService.java:511-556`)

- Resolves the index expression `logs-2026.03.*` to concrete indices via `IndicesAndAliasesResolver`
- Calls `authzEngine.authorizeIndexAction()` (line 541)

**5. RBACEngine.authorizeIndexAction()** → delegates to `IndicesPermission`

The `IndicesPermission` for this role was compiled during role loading from the `IndicesPrivileges` entries. It holds:
- An automaton for each index name pattern (`logs-*`)
- An automaton for each privilege (`read` → `indices:data/read/*`)
- The FLS/DLS settings for each entry

The check: for each concrete index, find an `IndicesPrivileges` entry where:
1. The index name matches the `indices` pattern automaton
2. The action name matches the `privileges` pattern automaton
3. `allowRestrictedIndices` is checked if the index is restricted

If a match is found, the response includes the applicable FLS/DLS restrictions for the matched entry.

### Role loading and composition

**CompositeRolesStore** (`CompositeRolesStore.java:88`) loads roles from multiple sources and merges them:

1. **getRoles()** (line 209) — called during authorization to load the authenticated user's roles
2. Fetches `RoleDescriptor` objects from each store (native `.security` index, `roles.yml`, reserved roles, custom role providers)
3. **buildRoleFromDescriptors()** (line 481) — merges multiple `RoleDescriptor`s into a single `SimpleRole`:
   - Cluster privileges: union of all descriptors' cluster privilege patterns → one `ClusterPermission`
   - Index privileges: union of all descriptors' index privilege entries → one `IndicesPermission`
   - Application privileges: union → one `ApplicationPermission`
4. The merged `Role` is cached for the user's session

### Key implementation files

| Class | Location | Purpose |
|-------|----------|---------|
| `SecurityActionFilter` | `security/action/filter/SecurityActionFilter.java` | Entry point — intercepts all transport actions |
| `AuthorizationService` | `security/authz/AuthorizationService.java` | Orchestrates the authorization pipeline |
| `RBACEngine` | `security/authz/RBACEngine.java` | Implements `AuthorizationEngine` — the actual checker |
| `SimpleRole` | `core/security/authz/permission/SimpleRole.java` | Compiled role — holds ClusterPermission + IndicesPermission |
| `ClusterPermission` | `core/security/authz/permission/ClusterPermission.java` | Compiled cluster privileges with automaton-based checks |
| `IndicesPermission` | `core/security/authz/permission/IndicesPermission.java` | Compiled index privileges |
| `ClusterPrivilegeResolver` | `core/security/authz/privilege/ClusterPrivilegeResolver.java` | Registry of all named cluster privileges |
| `IndexPrivilege` | `core/security/authz/privilege/IndexPrivilege.java` | Registry of all named index privileges |
| `RoleDescriptor` | `core/security/authz/RoleDescriptor.java` | Serializable role definition (what the API accepts) |
| `CompositeRolesStore` | `security/authz/store/CompositeRolesStore.java` | Loads and merges roles from all sources |
| `ReservedRolesStore` | `core/security/authz/store/ReservedRolesStore.java` | Built-in role definitions |
| `Automatons` | `core/security/support/Automatons.java` | Pattern→automaton compilation, set operations, caching |
| `SecurityActionMapper` | `security/action/SecurityActionMapper.java` | Action name remapping (3 edge cases only) |
