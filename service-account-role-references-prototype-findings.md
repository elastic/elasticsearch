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
       load managed account (project-scoped cache keyed by project id + principal, invalidated on PUT/DELETE)
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

**Demonstrated in automated tests:** internal cluster, YAML REST, and multi-project REST tests cover create-account → create-token → authorize, role assignment updates, delete/recreate same-service-account semantics, disabled accounts, reserved `elastic` namespace rejection, `manage_service_account` vs `manage_security` privilege split at HTTP, native role definition changes via `put_role`, project isolation for identically named principals/tokens, and exact `elastic/*` GET without consulting the managed store.

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

Deleting an account removes the account document (with refresh), clears the managed account cache entry, and clears index token credential cache entries by principal prefix. Token index documents are not bulk-deleted. While the account is deleted, authentication fails because the account lookup returns missing/disabled. After recreate, surviving index token documents authenticate again for the same service account (differs from original spec assumption that old tokens stay revoked).

## Privilege boundary

| Action | Privilege required |
|---|---|
| PUT/DELETE managed account | `manage_security` (action prefix `.../managed_service_account/...`) |
| Create managed index token | `manage_security` |
| Create built-in index token | `manage_service_account` (unchanged) |
| GET service accounts | `read_security` (unchanged) |

Verified in `ManagedServiceAccountPrivilegeTests`.

## Project, file-token, extension, mixed-cluster

| Area | Prototype behavior |
|---|---|
| Project scope | Uses `securityIndex.forCurrentProject()` for accounts and tokens; account and index-token credential caches key by `{projectId}/{principal}` |
| File tokens | Only built-in `elastic/*`; managed path never consults file store first for non-elastic namespaces |
| Extension `ServiceAccountTokenStore` | Managed accounts disabled when extension replaces the store (same as index tokens today) |
| Mixed cluster | `managed_service_accounts` transport version gates CRUD/auth; built-ins continue to work |
| Serverless | REST handlers marked `INTERNAL`; no serverless CRUD exposure |

| Malformed account docs | Missing or mistyped fields are rejected during parse; authentication fails without exposing document contents |
| Cross-project replay | Bearer from project A cannot authenticate in project B even when principal/token names match |

## API compatibility

- GET `/_security/service` returns built-in `role_descriptor` unchanged; managed entries add `"managed": true`, `"roles"`, `"enabled"`. Requests scoped to the reserved `elastic` namespace skip managed-store lookup so built-in definitions remain available during security-index outages.
- PUT/DELETE `/_security/service/{namespace}/{service}` are new routes for managed accounts.
- Token creation route unchanged; dispatches to managed vs built-in action by namespace.
- `ServiceAccountInfo` wire format gated by `managed_service_accounts` transport version.

## Verification commands

| Command | Result |
|---|---|
| `./gradlew generateTransportVersion` | PASS |
| `./gradlew :x-pack:plugin:core:test --tests '...ServiceAccountInfoTests' ...ManagedServiceAccountSubjectTests' ...ManagedServiceAccountIdValidatorTests'` | PASS |
| `./gradlew :x-pack:plugin:security:internalClusterTest --tests '...ManagedServiceAccountSingleNodeTests'` | PASS |
| `./gradlew :x-pack:plugin:yamlRestTest --tests '...service_accounts/20_managed*' '...service_accounts/21_managed_gaps*'` | PASS |
| `./gradlew :x-pack:plugin:core:test --tests '...PutManagedServiceAccountRequestTests'` | PASS |
| `./gradlew :x-pack:plugin:security:test --tests '...ManagedServiceAccountPrivilegeTests' ...ServiceAccountServiceTests' ...IndexServiceAccountTokenStoreTests' ...TransportGetServiceAccountActionTests'` | PASS |
| `./gradlew :x-pack:plugin:security:test --tests '...LoggingAuditTrailTests.testSecurityConfigChangeEventFormattingForManagedServiceAccount'` | PASS |
| `./gradlew :x-pack:plugin:security:qa:audit:javaRestTest --tests '...AuditIT.testAuditPutManagedServiceAccount'` | PASS |
| `./gradlew :x-pack:plugin:core:spotlessJavaCheck :x-pack:plugin:security:spotlessJavaCheck` | PASS |

**Not run:** rolling-upgrade IT, packaging/QA.

## Prototype questions answered

1. **NamedRoleReference path?** Yes, for managed accounts; built-ins unchanged.
2. **Role/account updates on next request?** Yes — account cache invalidated on PUT; per-request auth uses fresh account data.
3. **Reuse role cache without principal graph?** Yes — `NamedRoleReference` only.
4. **Persistence changes?** New `service_account` doc type using existing mapped fields (`username`, `roles`, `enabled`).
5. **Credential binding?** Stable principal identity; DELETE revokes via missing account + cache invalidation; RECREATE restores access for surviving index tokens.
6. **Audit logging?** Managed PUT/DELETE/token-create emit `security_config_change` events; documented in `elasticsearch-audit-events.md`.
7. **Remaining production gaps?** See below.

## Production gaps

### Correctness
- Cross-project isolation not tested
- Several spec edge cases not tested (e.g. multi-role union, zero roles, file token on managed account)
- Async existence checks for token delete on managed accounts added but not fully integration-tested
- PUT update semantics are full replacement (documented); no PATCH/partial update

### Security
- Delegated bounded admin (custom role for specific namespaces) not implemented
- Audit DELETE and managed token-create not covered by REST IT (unit tests only)
- Rate limiting / brute-force behavior unchanged from built-in tokens

### Compatibility
- Single transport version `managed_service_accounts` for all new wire paths
- Rolling-upgrade IT omitted
- `ServiceAccountInfo` old nodes cannot deserialize managed entries (expected; gated)

### Operability
- Managed account definition cache (`managed_service_account`) with cluster-wide invalidation on PUT/DELETE
- Delete clears account and token credential caches; index token documents may remain
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

Evidence supports the core hypothesis: managed accounts can authorize through `NamedRoleReference` without weakening built-ins. Stable principal identity with cache invalidation on DELETE provides revocation without generation IDs. Before production:

1. Add cross-project and rolling-upgrade integration tests.
2. Expose REST API specs publicly and decide delegated-admin privilege model.
3. Document recreate semantics for operators (surviving index tokens authenticate again).
