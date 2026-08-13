# Technical Design: Managed Service Accounts in Elasticsearch

| | |
|---|---|
| Author | Elliot Barlas (Elasticsearch Security) |
| Status | Draft (prototype complete on branch `managed-service-accounts-poc`) |
| Prototype PR | [elastic/elasticsearch#156257](https://github.com/elastic/elasticsearch/pull/156257) |
| Date | August 9, 2026 |
| Requirements | *Elastic Service Accounts PRD* (Alex Chalkias, Aug 2026). The PRD is authoritative for requirements; this document describes the Elasticsearch mechanism that satisfies its Elasticsearch-side requirements. |
| Companion docs | `service-account-role-references-prototype-findings.md` (prototype evidence and verification) |

## 1. Background and motivation

Kibana background jobs (alerting rules, workflows, and other scheduled workloads) have always executed under a borrowed human identity: whoever last saved the configuration. As workloads gained the ability to write, delete, and call connectors, this "last saver owns the identity" model became a real liability: silent privilege escalation when an admin touches a config, and silent breakage when the owning user is offboarded. The PRD's background section describes the problem and product goals in full; this document assumes that context.

Elastic Service Accounts give a workload a stable, scoped, auditable identity of its own. On Serverless, UIAM provides the backend. On self-managed and Elastic Cloud Hosted deployments, Elasticsearch and Kibana primitives must deliver the same experience without UIAM, and Elasticsearch today has no primitive for it:

- Built-in service accounts (`elastic/kibana`, `elastic/fleet-server`, …) are hardcoded in source with fixed privileges. Operators cannot create them or change what they can do.
- API keys are bound to their creator's identity at creation time (exactly the coupling the PRD is trying to eliminate), and their privilege snapshot does not track role changes.
- Native "robot" users require password management and pollute the human-user space.

Managed service accounts close this gap: service accounts that operators create, assign roles to, and delete through the REST API (and, above it, the Kibana management UI), living alongside the built-in accounts under the same conceptual umbrella and the same token model.

## 2. Concept overview

A managed service account is a named identity of the form `{namespace}/{service}`, for example `acme/billing-workflow`, stored in the Elasticsearch security index rather than in source code. It differs from a built-in account in one fundamental way:

> Built-in accounts have a fixed, hardcoded privilege descriptor. Managed accounts reference roles by name, and those names are resolved to privileges at authentication time.

This role-name indirection is the design center, and it directly implements the PRD's system requirement S5 ("service accounts reference roles by name; effective privileges are resolved at execution time"):

- Editing a role immediately changes the effective privileges of every service account referencing it. No re-binding, no credential rotation.
- Deleting a role removes its privileges from referencing accounts without deleting them; an account whose roles all resolve to nothing fails closed at authorization time.
- Role assignment is downscoping by construction: an account can be given any subset of the roles defined in the deployment, including reserved roles.

The `elastic` namespace remains reserved for built-in accounts. Everything under any other namespace is managed. The two kinds share the same token machinery, the same authentication realm (`_service_account`), and the same GET API, so consumers see one coherent "service accounts" surface.

Credentials are the existing service account bearer tokens: opaque secrets, stored hashed in the security index, presented as `Authorization: Bearer <token>`. A workload bound to a managed service account holds such a token and calls Elasticsearch with the account's permissions (PRD system requirement S2).

## 3. Deployment model scope

| Deployment model | Mechanism |
|---|---|
| Self-managed | Managed service accounts (this design) |
| Elastic Cloud Hosted | Managed service accounts (this design) |
| Serverless | UIAM provides service accounts on Serverless; this design does not apply there. The technical enforcement is the multi-project guard (next row): Serverless runs with multi-project support, so Elasticsearch does not construct the managed account store. The Serverless security extension (elasticsearch#126612) is orthogonal: it supplies per-project token authentication from operator secrets, and activates under the same multi-project condition. The new account-management endpoints are additionally internal-only in Serverless; the pre-existing token endpoints are public there but fail closed for managed namespaces, since no account can exist to issue tokens for. |
| Multi-project clusters | Explicitly unsupported and enforced in code. Service-account credential caches are not project-aware; rather than half-support tenant isolation, Elasticsearch does not enable the feature when the node supports multiple projects, and management APIs return a clear 400. This is consistent with Serverless (the only multi-project environment) being UIAM territory. |

Keeping the Elasticsearch feature out of Serverless is deliberate alignment with the PRD's split (assumptions 1 and 2): one UX, two backends, and no drift between them inside Elasticsearch itself.

## 4. REST API surface

### 4.1 Account management (new)

#### Create or update an account

```
PUT /_security/service/{namespace}/{service}
{
  "roles": ["billing_read", "connector_caller"],
  "enabled": true
}
```

- `roles` (required): role names to assign. Names are not validated for existence; like native users, an account may reference roles defined later. Duplicates are removed.
- `enabled` (optional, default `true`): a disabled account keeps its definition and tokens but cannot authenticate.
- Response: `{"created": true}` on first write, `{"created": false}` on update. Update is full replacement of the definition (no partial update).

#### Delete an account

```
DELETE /_security/service/{namespace}/{service}
DELETE /_security/service/{namespace}/{service}?force=true
```

Deleting an account that still has service tokens is rejected with a 400: list them with the credentials GET and delete them first through the token API, or pass `force=true` to delete the account while leaving its tokens in place (see §6 for the semantics of each path). The check is a bounded existence query, so it stays cheap regardless of how many tokens an account has. Response: `{"found": true|false}`. Deletion takes effect immediately for authentication.

Built-in accounts under the reserved `elastic` namespace (for example `elastic/kibana`) cannot be deleted through this endpoint. Requests targeting `elastic/*` fail request validation with a 400 before any store lookup, regardless of `force`. Those accounts remain defined in code and are not stored in the security index.

#### Naming rules

Namespace and service name each: 1 to 128 characters, starting alphanumeric, containing only letters, digits, hyphens, and underscores. The `elastic` namespace is reserved for built-in accounts and rejected with a 400.

### 4.2 Account retrieval (extended)

```
GET /_security/service
GET /_security/service?managed_by=elastic,user
GET /_security/service/{namespace}
GET /_security/service/{namespace}/{service}
```

Every entry carries a required `managed_by` discriminator identifying its kind: `elastic` for built-in accounts, `user` for API-managed accounts. Built-in entries render their fixed `role_descriptor` as before; managed entries render their definition. The two shapes form a tagged union that clients can parse reliably:

```json
{
  "elastic/kibana": {
    "managed_by": "elastic",
    "role_descriptor": { "...": "..." }
  },
  "acme/billing-workflow": {
    "managed_by": "user",
    "roles": ["billing_read", "connector_caller"],
    "enabled": true
  }
}
```

The `managed_by` query parameter filters the listing by kind and accepts `elastic`, `user`, or both (comma-separated). It is honored on all routes; only its default varies. The un-scoped listing defaults to `managed_by=elastic`, preserving the response existing integrations expect. A GET scoped to a namespace or service defaults to `managed_by=elastic,user`: the caller has explicitly named a namespace, so there is no pre-existing consumer to protect (non-`elastic` scoped routes returned nothing before this feature). `managed_by=user` on the un-scoped listing returns only API-managed accounts, which is what a management UI wants.

In the API specification, the entry is modeled as an internally-tagged union on `managed_by` with `elastic` as the default variant, so pre-feature responses (which lack the field and can only contain built-in accounts) remain valid, and generated clients get a clean discriminated type in every language.

Requests scoped to the `elastic` namespace never consult the security index, so built-in definitions remain readable even during a security-index outage.

### 4.3 Token management (existing routes, extended semantics)

The existing token routes now serve both kinds of account, dispatching internally by namespace:

```
PUT/POST /_security/service/{namespace}/{service}/credential/token/{name}   (create)
DELETE   /_security/service/{namespace}/{service}/credential/token/{name}   (delete)
GET      /_security/service/{namespace}/{service}/credential                (list token names)
```

Token creation for a managed account requires the account to exist and be enabled. Managed accounts support index-backed tokens only, not file-based tokens (those remain a built-in-account, operator-filesystem concept).

### 4.4 Authentication

Unchanged from built-in accounts: `Authorization: Bearer <token>`. The `_authenticate` response for a managed account surfaces the identity model directly:

```json
{
  "username": "acme/billing-workflow",
  "roles": ["billing_read", "connector_caller"],
  "authentication_realm": { "name": "_service_account", "type": "_service_account" },
  "metadata": { "_managed_service_account": true }
}
```

Built-in accounts continue to report empty `roles` and `_elastic_service_account` metadata. Authorization-denial errors name the service account and, via the standard role rendering, support the PRD's "actionable runtime error" requirement.

## 5. Privilege model and action names

Every operation maps to a distinct transport action name, which is what cluster privileges pattern-match against. The new managed-lifecycle actions deliberately live under a new prefix (`managed_service_account`) outside the existing `manage_service_account` privilege's pattern:

| Operation | Action name | Minimum privilege |
|---|---|---|
| Create/update managed account | `cluster:admin/xpack/security/managed_service_account/put` | `manage_security` |
| Delete managed account | `cluster:admin/xpack/security/managed_service_account/delete` | `manage_security` |
| Create managed account token | `cluster:admin/xpack/security/managed_service_account/token/create` | `manage_security` |
| Delete managed account token | `cluster:admin/xpack/security/managed_service_account/token/delete` | `manage_security` |
| Create/delete built-in account token | `cluster:admin/xpack/security/service_account/token/{create,delete}` | `manage_service_account` (unchanged) |
| List accounts (built-in + managed) | `cluster:admin/xpack/security/service_account/get` | `read_security` (unchanged) |
| List token names | `cluster:admin/xpack/security/service_account/credential/get` | `read_security` (unchanged) |

The boundary in one sentence: the entire managed-account lifecycle (account CRUD, token create, token delete) requires `manage_security`; `manage_service_account` remains scoped to built-in `elastic/*` accounts; `read_security` sees both (definitions and token names, never secrets).

Rationale: assigning role names to an identity and minting credentials for it is privilege-*granting*, equivalent in power to creating a user; that is `manage_security` territory. Creating a token for a built-in account grants nothing beyond that account's fixed, hardcoded scope, which is why the narrower privilege has always sufficed there. This matches the PRD's working assumption that the MVP management predicate is admin-level (`manage_security`); a delegated-admin model (e.g., namespace-scoped management by lesser roles) is a recognized north-star follow-on, and the separate action-name prefix gives a future privilege something clean to target.

Two deliberate consequences for existing privilege holders:

- `manage_service_account` holders (e.g., orchestration tooling that provisions Fleet tokens) gain no new capabilities from this feature.
- `read_security` holders see managed account definitions and token names in the GET APIs: metadata squarely within that privilege's charter, consistent with its visibility into roles, users, and API key metadata.

## 6. Lifecycle

### Create

An admin (`manage_security`) PUTs the account with role names. The account is a document in the security index; no credentials exist yet. Creation performs no check that the assigned roles fall within the creator's own privileges, because at the current privilege gate none is needed: the PUT requires `manage_security`, and `manage_security` already permits role and user management, and therefore transitively full access. Any role a `manage_security` holder assigns is by definition a subset of what that holder can already reach, so the PRD's "no escalation at creation" requirement holds by construction. This reasoning is tied to the privilege gate: if account creation is ever opened to lesser principals, an explicit enforcement mechanism moves into Elasticsearch (see §10).

### Issue credentials

The admin creates one or more named tokens. The secret is returned exactly once; Elasticsearch stores only a hash.

### Execute

The workload authenticates with the bearer token. Per request, Elasticsearch verifies the credential, checks the account exists and is enabled, and resolves the assigned role names through the standard role machinery: the same code path, caches, and semantics as native users. Role edits propagate on the role store's normal invalidation; no service-account-specific staleness.

### Drift and failure semantics (PRD S3/S5)

Missing or deleted roles silently contribute nothing; an account whose roles resolve to no privileges authenticates successfully but fails authorization closed, with an error naming the service account. This trades user-identity drift for role-definition drift; the PRD's risk register calls this out.

### Disable

`PUT` with `"enabled": false` blocks authentication immediately without destroying tokens or definition. This is the reversible off-switch.

### Delete

Deletion is reference-guarded, following the same Elasticsearch convention as component templates in use by index templates or ILM policies in use by indices: a DELETE is rejected while the account still has service tokens. A routine decommission therefore removes the credentials first (via the existing token DELETE API, with the credentials GET to enumerate them) and leaves nothing behind, so PRD security requirement 3 (deletion immediately invalidates all associated credentials) holds on this path by construction. The guard also stops an accidental delete of an in-use account before it breaks a running workload.

`force=true` overrides the guard (the same parameter contract as ML job and transform deletion): the account document is removed, caches are invalidated cluster-wide, authentication fails immediately, and the token documents remain. Because account identity is purely the name, recreating the same `{namespace}/{service}` after a forced delete re-enables the surviving tokens. This is deliberate, opt-in suspension/restore semantics (analogous to deleting and recreating a role), and it makes a forced delete recoverable without re-issuing and re-binding credentials. The `force` flag is recorded in the audit event. The token check and the delete are not atomic; a token created concurrently may survive, which fails in the same direction as `force`.

### Recreate

Same name = same logical account. There are no hidden generation IDs; auditability and revocation flow from the name, the roles, and the token set. After a default (guarded) delete, recreation starts credential-less; after a forced delete, it restores the surviving token set.

### Short-lived credentials (API keys)

Service account bearer tokens have no expiration. For workloads that need a bounded credential lifetime, a managed account can mint Elasticsearch API keys through the existing API key endpoints; no new API is required. This is opt-in: the account must be assigned a role that grants the relevant cluster privilege.

#### Self-mint

An account whose assigned roles include `manage_own_api_key` can call `POST /_security/api_key` authenticated with its bearer token. The request may set `expiration`. The key is owned by the service account principal in the `_service_account` realm and carries `_managed_service_account` metadata. Privileges are resolved from the account's assigned roles at creation time and stored as `limited_by` descriptors on the key; unlike bearer-token authentication, an API key does not live-resolve role names on each request, so later role edits do not affect existing keys.

#### Grant on behalf

A principal (including a managed service account) with `grant_api_key` can call `POST /_security/api_key/grant` to mint a key for another identity. The grant body uses `grant_type: access_token` with that identity's bearer token; service account bearer tokens are accepted alongside OAuth2 access tokens. This is the path a third party (e.g. Kibana with `grant_api_key`) uses to issue short-lived keys for a workload without holding the workload's long-lived service token. `manage_own_api_key` does not authorize grant-api-key; the separate `grant_api_key` privilege is required for the caller.

#### Operational notes

API keys and bearer tokens are separate credential systems with different management APIs, audit events, and invalidation paths. Operators need `manage_api_key` (not merely `manage_own_api_key`) to list or invalidate keys on behalf of a service account. OAuth2 access-token creation (`POST /_security/oauth2/token`) remains unsupported for service accounts.

## 7. Observability and audit

All managed-lifecycle mutations emit `security_config_change` audit events, symmetrical with existing security config auditing:

| Operation | event.action |
|---|---|
| Account create/update | `put_managed_service_account` (namespace, service, roles, enabled) |
| Account delete | `delete_managed_service_account` (namespace, service, force) |
| Token create / delete | `create_service_token` / `delete_service_token` (shared with built-in tokens) |

Authentication and access grant/denied events flow through the existing `_service_account` realm auditing. The audit events reference documentation is updated in the same change.

## 8. Cross-cluster search

- RCS 2.0 (API-key based, current model): the querying cluster authorizes remote access through `remote_indices`/`remote_cluster` sections in the assigned roles' definitions, intersected with the cross-cluster API key. This is identical to native users.
- RCS 1.0 (certificate based, legacy): the authentication is forwarded to the fulfilling cluster, which resolves the assigned role *names against its own role store*. This is again identical to native users (and unlike built-in accounts, whose fixed descriptor is the same on both sides). The fulfilling cluster needs matching role definitions but no account document.
- Older fulfilling clusters (either RCS model): fails closed. Under RCS 2.0 the fulfilling cluster's subject-info validation deterministically rejects a managed-account authentication, since pre-feature versions require service accounts to carry no roles. Under RCS 1.0 the rejection surfaces as an authentication-header verification error, or as a role-resolution error on builds without assertions. In both cases the error originates on the remote and can be masked as a skipped cluster when `skip_unavailable` is true. A querying-side version guard, following the existing pattern for cross-cluster-access subjects sent to older versions, is recommended follow-up work for both models.

Both supported models are covered by same-version integration tests, and the fails-closed behavior against older fulfilling clusters is covered by BWC multi-cluster tests for both models.

## 9. Compatibility and rollout

- All new wire formats and behaviors are gated on a single transport version (`managed_service_accounts`). In a mixed-version cluster mid-upgrade, account creation is rejected until all nodes are upgraded; built-in accounts are unaffected throughout.
- GET service account entries gain a required `managed_by` discriminator. For built-in entries this is a purely additive field next to the unchanged `role_descriptor`; pre-feature clients are further shielded because the default `managed_by=elastic` filter means they never receive a managed entry without opting in. The API specification models the entry as an internally-tagged union with `elastic` as the default variant for pre-feature responses.
- The GET service accounts API preserves pre-existing path-component behavior: a namespace or service name that cannot match a managed account (e.g., illegal characters) returns an empty 200, matching the response clients saw before managed accounts existed.
- No index mapping changes: the account document reuses existing mapped security-index fields.

## 10. Future considerations

Design directions that surfaced during this work but are not part of the MVP. They are recorded here to inform the north star, not to commit to a mechanism.

### Run-as impersonation of managed accounts

A future enhancement could allow the built-in `elastic/kibana` account to run-as a managed service account for workload execution. Kibana would authenticate with its own credential and execute with the managed account's identity and live-resolved roles. This would be a third execution option alongside direct bearer-token use and grant-based API keys, with two attractive properties: no long-lived managed-account token needs to be distributed to (or stored by) Kibana at all, and run-as preserves both identities in audit (authenticated by `elastic/kibana`, effective user the managed account), which is arguably the strongest audit story of the three options.

Elasticsearch currently blocks this in two places. A service account cannot initiate run-as, and a service account cannot be a run-as target: impersonation lookup only considers ordinary users, not identities in the `_service_account` realm. Both gates would lift for this feature.

The authorization model is the part that needs a deliberate design. Today's run-as is caller-only: Elasticsearch checks the authenticating principal's role `run_as` patterns against the target name, and the target has no say. Putting a managed-namespace pattern on the built-in `elastic/kibana` role (covering every non-`elastic/*` name) would make the Kibana service token a universal impersonation key for the managed-account namespace. That is privilege escalation. `elastic/kibana` is `kibana_system`, which is broad but closed: it is not `manage_security` and not unrestricted `all`. Managed accounts may be assigned any role, including reserved roles. A name-pattern grant would let a stolen or compromised Kibana token inhabit any of them, including an account an admin later assigns `superuser`. It is also strictly more powerful than the other two execution paths, which require possession of the target's credential (`grant_api_key` still needs the target's access token or password). Excluding `elastic/*` only protects other built-in service accounts; it does not bound the privileges of `acme/*`.

The ACL therefore lives on the target account, as a default-deny allowlist of impersonator principals, with a small caller-side capability rather than a name wildcard.

#### Target consent: `run_as_from`

```
PUT /_security/service/acme/billing-workflow
{
  "roles": ["billing_read", "connector_caller"],
  "enabled": true,
  "run_as_from": ["elastic/kibana"]
}
```

`run_as_from` is an optional list of exact principals who may impersonate this account. Absent or empty means nobody may: fail closed, and the state of every account created before the field exists. GET returns the field. The name is deliberately not `run_as`; on a role descriptor that means "who I may inhabit," and reusing it here would invert the meaning on a different document type. The security index already maps `run_as` as keyword for roles; this is a new field and a mapping addition.

Exact principals only. No `*`, no `elastic/*`. A wildcard on the target would recreate the original hole on a different document.

Direct bearer-token authentication is unchanged: holding a token does not require membership in `run_as_from`. Impersonation and credential possession stay separate. A disabled account is rejected as a run-as target the same way it fails authentication.

PUT remains full replacement, as with `roles` and `enabled`. Updating roles without resending `run_as_from` clears it and drops impersonation. That is fail closed; the management UI must always send the field.

#### Caller capability, not a name list

The authenticating principal still needs permission to *ask*. Allow service accounts to initiate run-as on this path, but do not put a managed-namespace `run_as` pattern on `kibana_system`. Give `elastic/kibana` a capability: a dedicated cluster privilege (for example `run_as_managed_service_account`) on that account's built-in role, or an equivalent special case for that principal. That answers "is this principal allowed to attempt impersonation of managed accounts." The account's `run_as_from` answers "may they inhabit *this* account." Both checks must pass.

`elastic/kibana` does not have `manage_security`, so it cannot write `run_as_from` and cannot opt itself into accounts. Consent is an admin mutation (the Kibana management UI runs as the logged-in admin); use is later impersonation by Task Manager with the Kibana service token. That split is the security property.

An admin can still assign `superuser` *and* list `elastic/kibana`. That is explicit and per-account, the same class of decision as putting `run_as: ["elastic"]` on a human role, not an implicit cluster-wide grant.

#### What this does not close

Kibana can inhabit any opted-in account. If a non-admin Kibana user can bind an alerting rule to `acme/super-workflow`, Elasticsearch only sees `elastic/kibana` run-as that principal. Binding workloads to accounts is Kibana authorization. Encoding application identifiers (`on_behalf_of: alerting:rule:123`) into the account document would leak Kibana semantics into the security index and is out of scope here.

Operator privileges already refuse to treat a run-as authentication as operator. Impersonation would drop operator status; that is existing behavior and the correct direction.

#### Authorization checks

Run-as target lookup today considers ordinary users only. For a name that parses as a managed service-account principal, resolve it as a managed account instead. Then:

- deny if the account is missing or disabled;
- deny if the authenticating principal is not in `run_as_from`;
- deny if the caller lacks the initiator capability.

The resulting authentication must be allowed to have a service-account effective identity; today run-as requires an ordinary user. Existing `run_as_granted` / `run_as_denied` audit events cover the outcome; the denial should distinguish caller-side vs target-side failure.

### Self-service account management (API key parity)

The PRD's north star includes non-admins managing service accounts. API keys set the precedent: any principal may mint credentials scoped to at most its own privileges. The analogous rule here is to allow account creation and token issuance by ordinary principals when the assigned roles' resolved privileges are a subset of the creator's. Two implementation shapes:

- Creation-time subset check: resolve the assigned roles to descriptors and verify each privilege against the creator through the has-privileges machinery (the `RoleDescriptor#parsePrivilegesToCheck` shape). This inherits that machinery's limits: only cluster, index, and application privileges are checkable; DLS and FLS are explicitly rejected; `run_as`, remote cluster/indices privileges, and configurable global privileges have no check representation. Roles using those features would have to be rejected or conservatively approximated. It is also check-once semantics: later edits to the assigned roles, or to the creator's own privileges, are not re-validated, so the subset invariant erodes over time. API keys have the same time-of-check property, but descriptor snapshotting masks it there; with live role resolution it is more visible.
- Limited-by intersection: snapshot the creator's role descriptors onto the account and enforce the intersection at authorization time, as API keys do and as the PRD's UIAM model does (assumption 3, `limited_by` in role-name terms). Enforcement stays live rather than check-once, but it couples the account's privilege ceiling to a point-in-time snapshot of its creator, reintroducing the creator coupling and snapshot drift this feature exists to eliminate.

This consideration is orthogonal to delegated administration (the north-star model noted in §5): namespace-scoped administration decides who may manage which accounts, while privilege-subset creation decides what privileges they may assign. A full self-service model likely composes both. PRD security requirement 1 (no escalation at creation) holds in the MVP because `manage_security` already implies full access (see §6); opening creation to lesser principals is exactly what would make one of the mechanisms above necessary.
