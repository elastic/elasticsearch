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
       load managed account document (no cache)
       reject missing/disabled/malformed
       index token auth bypassing credential cache, with generation check
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
- Account role-name changes take effect on the next authentication (no account-definition cache)
- Named-role create/update/delete invalidates via existing role-cache mechanisms
- Built-in accounts remain on `ServiceAccountRoleReference` and are unaffected by native role invalidation

**Not demonstrated in automated tests yet:** full end-to-end role create/update after managed auth in a cluster (requires internal cluster test — see gaps).

## Document shapes (no secrets)

**Managed account (`doc_type=service_account`):**

| Field | Example |
|---|---|
| `username` | `my-team/my-service` |
| `roles` | `["role-a","role-b"]` |
| `enabled` | `true` |
| `account_generation_id` | UUID (immutable after create) |
| `version` | cluster version id |

**Managed index token (`doc_type=service_account_token`):**

| Field | Notes |
|---|---|
| `username`, `name`, `password`, `creation_time`, `creator` | same as built-in tokens |
| `account_generation_id` | required; must match current account |

Generation IDs are not exposed via GET APIs.

## Deletion / recreation

On create, a new `account_generation_id` is generated. Token documents store this ID; authentication rejects tokens whose generation differs. Managed token authentication bypasses the credential cache to prevent cross-generation hits.

Deleting an account removes the account document (with refresh), clears index token cache by principal prefix, and bulk-deletes index tokens. Authentication fails closed even if stale token documents remain.

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
| Project scope | Uses `securityIndex.forCurrentProject()` for accounts and tokens |
| File tokens | Only built-in `elastic/*`; managed path never consults file store first for non-elastic namespaces |
| Extension `ServiceAccountTokenStore` | Managed accounts disabled when extension replaces the store (same as index tokens today) |
| Mixed cluster | `managed_service_accounts` transport version gates CRUD/auth; built-ins continue to work |
| Serverless | REST handlers marked `INTERNAL`; no serverless CRUD exposure |

**Not tested:** cross-project token replay with identical principal/name.

## API compatibility

- GET `/_security/service` returns built-in `role_descriptor` unchanged; managed entries add `"managed": true`, `"roles"`, `"enabled"`.
- PUT/DELETE `/_security/service/{namespace}/{service}` are new routes for managed accounts.
- Token creation route unchanged; dispatches to managed vs built-in action by namespace.
- `ServiceAccountInfo` wire format gated by `managed_service_accounts` transport version.

## Verification commands

| Command | Result |
|---|---|
| `./gradlew generateTransportVersion` | PASS |
| `./gradlew :x-pack:plugin:core:test --tests '...ServiceAccountInfoTests' ...ManagedServiceAccountSubjectTests' ...ManagedServiceAccountIdValidatorTests'` | PASS |
| `./gradlew :x-pack:plugin:security:test --tests '...ManagedServiceAccountPrivilegeTests' ...ServiceAccountServiceTests' ...IndexServiceAccountTokenStoreTests' ...TransportGetServiceAccountActionTests'` | PASS |
| `./gradlew :x-pack:plugin:core:spotlessJavaCheck :x-pack:plugin:security:spotlessJavaCheck` | PASS |

**Not run:** YAML REST integration test, internal cluster test, rolling-upgrade IT, packaging/QA.

## Prototype questions answered

1. **NamedRoleReference path?** Yes, for managed accounts; built-ins unchanged.
2. **Role/account updates on next request?** Yes — no account cache; per-request index read and fresh auth object.
3. **Reuse role cache without principal graph?** Yes — `NamedRoleReference` only.
4. **Persistence changes?** New `service_account` doc type + generation field on managed tokens.
5. **Credential binding?** Generation ID on account and token docs; cache bypass for managed auth.
6. **Remaining production gaps?** See below.

## Production gaps

### Correctness
- No internal cluster / YAML REST test for full create-account → create-token → authorize flow
- Cross-project isolation not tested
- Async existence checks for token delete on managed accounts added but not fully integration-tested
- PUT update semantics are full replacement (documented); no PATCH/partial update

### Security
- Delegated bounded admin (custom role for specific namespaces) not implemented
- Audit trail fields for managed accounts not explicitly validated
- Rate limiting / brute-force behavior unchanged from built-in tokens

### Compatibility
- Single transport version `managed_service_accounts` for all new wire paths
- Rolling-upgrade IT omitted
- `ServiceAccountInfo` old nodes cannot deserialize managed entries (expected; gated)

### Operability
- No cache for managed account definitions (acceptable for prototype; may need project-scoped cache)
- Delete does best-effort token cleanup; operators may need manual cache clear on failure
- No bulk account listing pagination beyond scroll size 1000

### Performance
- Per-auth index GET for managed accounts
- Managed token auth skips cache entirely

### Documentation
- No public REST spec JSON files added under `rest-api-spec/`
- Serverless exposure plan not implemented

## Recommendation

**Proceed with revision.**

Evidence supports the core hypothesis: managed accounts can authorize through `NamedRoleReference` without weakening built-ins, and generation binding prevents credential resurrection. Before production:

1. Add internal cluster + YAML REST tests for the vertical slice.
2. Add cross-project and delete/recreate integration tests.
3. Expose REST API specs and decide delegated-admin privilege model.
4. Evaluate a project-scoped account cache with cluster-wide invalidation if read load is a concern.
