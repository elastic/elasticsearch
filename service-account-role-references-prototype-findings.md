# Prototype Findings: Role-Backed Managed Service Accounts

## Architecture summary

Managed service accounts are stored in the `.security` index as `doc_type=service_account` documents keyed by `service_account-{namespace}/{service}`. Built-in `elastic/*` accounts remain source-controlled in `ElasticServiceAccounts` and are never written to the index.

Authentication flow:

```text
parse bearer token
  -> secret length check
  -> if namespace == elastic:
       built-in lookup in ElasticServiceAccounts.ACCOUNTS
       composite file/index token store (unchanged)
       Authentication with empty User.roles() and _elastic_service_account metadata
  -> else:
       validate managed principal grammar
       load managed account (cached by principal, invalidated on PUT/DELETE)
       reject missing/disabled/malformed
       index token auth with standard credential cache
       Authentication with role names in User.roles() and _managed_service_account metadata
```

Authorization flow:

```text
Subject.getRoleReferenceIntersection
  -> built-in: ServiceAccountRoleReference(principal)
  -> managed: NamedRoleReference(user.roles())
```

## Domain model

`ServiceAccountAuthorization` sealed hierarchy:

- `Fixed(RoleDescriptor)` — built-in accounts only
- `AssignedRoles(List<String>)` — managed accounts only

Built-in/managed distinction uses explicit user metadata (`_elastic_service_account` / `_managed_service_account`), not empty vs non-empty roles.

## User.roles() as carrier

**Result: yes, with metadata guard.**

Managed role names are stored in `User.roles()` during authentication. `Authentication` consistency checks allow roles only when `_managed_service_account=true`. Built-in accounts continue with empty roles.

Authenticate API output therefore includes assigned role names for managed accounts. Authorization-denial messages include them via the standard user role list.

## Role cache and invalidation

Managed accounts use `NamedRoleReference`, reusing the existing role-store cache and native-role invalidation paths.

Observed behavior in unit tests:

- Multiple assigned names resolve additively through one `NamedRoleReference`
- Empty assignment yields `RoleKey.ROLE_KEY_EMPTY`
- Account role-name changes take effect on the next authentication after account-cache invalidation on PUT
- Named-role create/update/delete invalidates via existing role-cache mechanisms
- Built-in accounts remain on `ServiceAccountRoleReference` and are unaffected by native role invalidation

**Demonstrated in automated tests:** internal cluster, YAML REST, multi-project, and multi-cluster (RCS 1.0 and 2.0) REST tests cover create-account → create-token → authorize, role assignment updates, delete/recreate same-service-account semantics, disabled accounts, reserved `elastic` namespace rejection, `manage_service_account` vs `manage_security` privilege split at HTTP (including managed token create/delete), native role definition changes via `put_role`, managed-account unavailability in multi-project clusters, cross-cluster search under both RCS models, and exact `elastic/*` GET without consulting the managed store.

## Document shapes (no secrets)

**Managed account (`doc_type=service_account`):**

| Field | Example |
|---|---|
| `username` | `my-team/my-service` |
| `roles` | `["role-a","role-b"]` |
| `enabled` | `true` |
| `version` | cluster version id |

**Managed index token (`doc_type=service_account_token`):**

| Field | Notes |
|---|---|
| `username`, `name`, `password`, `creation_time`, `creator` | same as built-in tokens |

## Deletion / recreation

A managed service account is identified solely by `{namespace}/{service}`. Delete and recreate restore the same logical account.

**Settled (delete guard)**: deleting an account that still has service tokens is rejected with a 400, unless `force=true`. The check is a bounded existence query (`size=0`, `terminate_after=1`); it does not enumerate tokens, and the credentials GET API is the way to list them. The default path therefore guarantees no credentials survive a decommission: tokens must be removed first through the existing token DELETE API, so nothing is left to resurrect. `force=true` deletes the account and leaves token documents in place: authentication fails while the account is absent, and recreating the same name re-enables surviving tokens (explicit suspension/restore semantics, analogous to role delete/recreate). The token check and the delete are not atomic; a token created concurrently may survive, which fails in the same direction as `force`. The `force` flag is recorded in the audit event. This follows the ES-wide reference-guarded deletion convention (component templates in use by index templates, ILM policies in use by indices, enrich policies referenced by pipelines) with the ML/transform-style `force` override.

Deleting an account removes the account document (with refresh), clears the managed account cache entry, and clears index token credential cache entries by principal prefix.

## Privilege boundary

| Action | Privilege required |
|---|---|
| PUT/DELETE managed account | `manage_security` (action prefix `.../managed_service_account/...`) |
| Create managed index token | `manage_security` |
| Delete managed index token | `manage_security` (dedicated `.../managed_service_account/token/delete` action) |
| Create/delete built-in index token | `manage_service_account` (unchanged; transport actions reject non-`elastic` namespaces) |
| GET service accounts | `read_security` (unchanged) |

The entire managed-account lifecycle (account CRUD, token create, token delete) requires `manage_security`; `manage_service_account` remains scoped to built-in `elastic/*` accounts; `read_security` sees both (account definitions and token names, no secrets).

Verified in `ManagedServiceAccountPrivilegeTests` (action-name coverage) and the `21_managed_gaps.yml` privilege-boundary test (HTTP-level, including managed token create/delete denial for `manage_service_account`).

## Project, file-token, extension, mixed-cluster

| Area | Prototype behavior |
|---|---|
| Project scope | Multi-project is out of scope. Service account credential caches are keyed by qualified token name with no project dimension, so `Security` does not construct `ManagedServiceAccountStore` when the project resolver supports multiple projects; managed CRUD returns 400 in multi-project clusters. Caches key by principal, assuming a single project. |
| File tokens | Only built-in `elastic/*`; managed path never consults file store first for non-elastic namespaces |
| Extension `ServiceAccountTokenStore` | Managed accounts disabled when extension replaces the store (same as index tokens today) |
| Mixed cluster | `managed_service_accounts` transport version gates CRUD/auth; built-ins continue to work |
| Serverless | Serverless replaces the token store via `SecurityExtension#getServiceAccountTokenStore` (elasticsearch#126612), which disables index-backed tokens and managed accounts entirely; per-project built-in tokens come from operator secrets files (elasticsearch-serverless#3759). REST handlers marked `INTERNAL`. |
| Malformed account docs | Missing or mistyped fields are rejected during parse; authentication fails without exposing document contents |
| Cross-project replay | Not applicable: managed service accounts are unavailable in multi-project clusters (see Project scope) |

## Cross-cluster search

- **RCS 2.0**: the querying cluster resolves `remote_indices` from the assigned role into inline role descriptors and forwards them in `CrossClusterAccessSubjectInfo`. Same-version behavior is tested in `RemoteClusterSecuritySpecialUserIT`. BWC is tested in `testManagedServiceAccountCcsAgainstOlderFulfillingCluster` (RCS 2.0 subclass only): fulfilling clusters before 9.6.0 fail closed with `must have no role`; 9.6.0+ fulfilling clusters authorize from the forwarded descriptors.
- **RCS 1.0**: the querying cluster authenticates the service token locally and forwards the `Authentication` object; the fulfilling cluster resolves the assigned role *names* against its own role store — the same semantics as native users, unlike built-in accounts (fixed descriptor, identical on both sides). The fulfilling cluster needs a matching role definition but no managed account document. Same-version behavior is tested in `RemoteClusterSecurityManagedServiceAccountRCS1IT`. BWC fail-closed behavior for pre-feature FCs is pinned by `testManagedServiceAccountCcsFailsClosedAgainstOlderFulfillingCluster` (RCS 1.0 subclass only).

## API compatibility

- GET `/_security/service` entries form a tagged union on a required `managed_by` discriminator (review feedback): built-in entries render `"managed_by": "elastic"` alongside their unchanged `role_descriptor`; managed entries render `"managed_by": "user"` with `"roles"` and `"enabled"`. The field is additive for built-in entries, and the API specification can model the union with an internal-tag variant defaulting to `elastic` for pre-feature responses. Requests scoped to the reserved `elastic` namespace skip managed-store lookup so built-in definitions remain available during security-index outages.
- A `managed_by` query parameter (comma-separated `elastic` and/or `user`, matching the response discriminator vocabulary) filters the listing by kind and is honored on all routes. The un-scoped GET defaults to `elastic`, protecting the pre-existing response shape, and namespace/service-scoped GETs default to `elastic,user`. `managed_by=user` lists only API-managed accounts. Documented in the REST spec and pinned in `20_managed.yml`.
- PUT/DELETE `/_security/service/{namespace}/{service}` are new routes for managed accounts.
- Token creation and deletion routes unchanged; both dispatch to managed vs built-in transport actions by namespace, and the built-in actions reject non-`elastic` namespaces.
- `ServiceAccountInfo` wire format gated by `managed_service_accounts` transport version.

## Verification commands

| Command | Result |
|---|---|
| `./gradlew generateTransportVersion` | PASS |
| `./gradlew :x-pack:plugin:core:test --tests '...ServiceAccountInfoTests' --tests '...ManagedServiceAccountSubjectTests' --tests '...ManagedServiceAccountIdValidatorTests' --tests '...PutManagedServiceAccountRequestTests'` | PASS |
| `./gradlew :x-pack:plugin:security:internalClusterTest --tests '...ManagedServiceAccountSingleNodeTests' --tests '...ServiceAccountSingleNodeTests'` | PASS |
| `./gradlew :x-pack:plugin:yamlRestTest --tests '...XPackRestIT.test {yaml=service_accounts/2*}'` (the `p0=` filter form matches nothing) | PASS |
| `./gradlew :x-pack:plugin:security:test --tests '...ManagedServiceAccountPrivilegeTests' --tests '...ManagedServiceAccountStoreTests' --tests '...ServiceAccountServiceTests' --tests '...IndexServiceAccountTokenStoreTests' --tests '...TransportGetServiceAccountActionTests' --tests '...TransportDeleteServiceAccountTokenActionTests' --tests '...TransportDeleteManagedServiceAccountTokenActionTests' --tests '...SecurityTests'` | PASS |
| `./gradlew :x-pack:plugin:security:test --tests '...LoggingAuditTrailTests'` (managed PUT/DELETE/token-create/token-delete formatting + must-log coverage) | PASS |
| `./gradlew :x-pack:plugin:security:qa:audit:javaRestTest --tests '...AuditIT.testAuditPutManagedServiceAccount'` | PASS |
| `./gradlew :x-pack:plugin:security:qa:multi-project:javaRestTest --tests '...ManagedServiceAccountMultiProjectIT'` | PASS |
| `./gradlew :x-pack:plugin:security:qa:multi-cluster:javaRestTest --tests '...RemoteClusterSecurityManagedServiceAccountRCS1IT'` | PASS (x2 seeds) |
| `./gradlew :x-pack:plugin:security:qa:multi-cluster:javaRestTest --tests '...RemoteClusterSecuritySpecialUserIT'` (RCS 2.0) | PASS (x2 seeds, after fixing an order-dependent data collision with the sibling test — the managed test now uses a dedicated `shared-managed` index) |
| `./gradlew ':x-pack:plugin:security:qa:multi-cluster:v9.5.1#bwcTest' --tests '*testManagedServiceAccountCcs*AgainstOlder*'` (RCS 1 fail-closed + RCS 2 fail-closed, old FC) | PASS |
| `./gradlew ':x-pack:plugin:security:qa:multi-cluster:v9.6.0#bwcTest' --tests '*testManagedServiceAccountCcsAgainstOlder*'` (RCS 2 success, current FC) | PASS |
| `./gradlew :x-pack:plugin:core:spotlessJavaCheck :x-pack:plugin:security:spotlessJavaCheck` | PASS |

**Not run:** rolling-upgrade IT, full `:x-pack:plugin:security:test` sweep, packaging/QA.

## Prototype questions answered

1. **NamedRoleReference path?** Yes, for managed accounts; built-ins unchanged.
2. **Role/account updates on next request?** Yes — account cache invalidated on PUT; per-request auth uses fresh account data.
3. **Reuse role cache without principal graph?** Yes — `NamedRoleReference` only.
4. **Persistence changes?** New `service_account` doc type using existing mapped fields (`username`, `roles`, `enabled`).
5. **Credential binding?** Stable principal identity. Default DELETE is guarded (tokens must be removed first, so nothing survives); `force=true` DELETE revokes via missing account + cache invalidation, and RECREATE restores access for surviving index tokens.
6. **Audit logging?** Managed account PUT/DELETE and token create/delete emit `security_config_change` events; documented in `elasticsearch-audit-events.md`.
7. **Remaining production gaps?** See below.

## Production gaps

### Correctness
- Several spec edge cases not tested (e.g. multi-role union, zero roles, file token on managed account)
- Async existence checks for token delete on managed accounts added but not fully integration-tested
- PUT update semantics are full replacement (documented); no PATCH/partial update

### Security
- Delegated bounded admin (custom role for specific namespaces) not implemented
- Audit of account DELETE and managed token create/delete not covered by REST IT (unit tests only; PUT has a REST IT)
- Rate limiting / brute-force behavior unchanged from built-in tokens
- Unauthenticated requests with fabricated principals populate the bounded negative account cache (10k entries, 20m TTL); same exposure class as other security caches, not separately mitigated

### Compatibility
- Single transport version `managed_service_accounts` for all new wire paths
- Rolling-upgrade IT omitted
- `ServiceAccountInfo` old nodes cannot deserialize managed entries (expected; gated)
- No sending-side guard when a managed-account `Authentication` is serialized to a pre-feature node under RCS 1.0; fails closed on the receiver with a non-obvious error
- GET `/_security/service/{namespace}` with a name failing the new grammar now returns 400 where it previously returned an empty 200 (deliberate; needs a changelog entry)

### Operability
- Managed account definition cache (`managed_service_account`) with cluster-wide invalidation on PUT/DELETE
- Default delete requires removing tokens first (guard); `force=true` leaves token documents in place with documented suspension/restore semantics
- No bulk account listing pagination beyond scroll size 1000

### Performance
- Per-auth managed account lookup (cached after first read until invalidation)
- Managed index tokens use the same credential cache as built-in index tokens

### Documentation
- REST API spec JSON files added for managed PUT/DELETE under `rest-api-spec/`
- Audit events reference updated for managed service accounts
- Serverless exposure plan not implemented

## Recommendation

**Proceed with revision.**

Evidence supports the core hypothesis: managed accounts can authorize through `NamedRoleReference` without weakening built-ins. Stable principal identity with cache invalidation on DELETE provides revocation without generation IDs. Multi-project is explicitly out of scope (enforced in code); the privilege boundary is uniform (`manage_security` for the whole managed lifecycle). Before production:

1. Add a rolling-upgrade integration test; consider a sending-side version guard in `Authentication#maybeRewriteForOlderVersion`.
2. Expose REST API specs publicly and decide the delegated-admin privilege model.
