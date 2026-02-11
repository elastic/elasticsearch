# Clone API Key API — Specification

This document specifies the **Clone API Key** REST API and related backend changes so that an API key can be duplicated with a new name, id, and optional expiry while keeping the same permissions (role descriptors).

---

## 1. Overview

- **Purpose:** Allow callers with appropriate privilege to create a new API key that has the same role descriptors as an existing API key, with a new name, id, optionally a new expiry, and optional metadata. The source API key is identified by providing its credential in encoded form in the request body.
- **Endpoint:** `POST /_security/api_key/clone` (and optionally `PUT /_security/api_key/clone` for consistency with Grant API Key).
- **Authorization:** Requires the new cluster privilege `clone_api_key`, which mirrors the existing `grant_api_key` privilege (same pattern: a single cluster action).
- **Response:** Identical to the Create API Key / Grant API Key response (`CreateApiKeyResponse`).

---

## 2. Cluster Privilege: `clone_api_key`

### 2.1 Definition

- **Name:** `clone_api_key`
- **Pattern:** Same style as `grant_api_key`: a dedicated cluster action name that the privilege grants.
- **Action name:** `cluster:admin/xpack/security/api_key/clone` (or equivalent following existing naming; the action name must be consistent with `GrantApiKeyAction.NAME` style).

**Reference implementation (grant_api_key):**

- In `ClusterPrivilegeResolver` (x-pack/plugin/core):
  - `GRANT_API_KEY_PATTERN = Set.of(GrantApiKeyAction.NAME + "*")` with `GrantApiKeyAction.NAME = "cluster:admin/xpack/security/api_key/grant"`.
  - `GRANT_API_KEY = new ActionClusterPrivilege("grant_api_key", GRANT_API_KEY_PATTERN)`.
- The new privilege must be added in the same way:
  - Define `CloneApiKeyAction.NAME` (e.g. `"cluster:admin/xpack/security/api_key/clone"`).
  - Define `CLONE_API_KEY_PATTERN = Set.of(CloneApiKeyAction.NAME + "*")`.
  - Define `CLONE_API_KEY = new ActionClusterPrivilege("clone_api_key", CLONE_API_KEY_PATTERN)`.
- Register `CLONE_API_KEY` in the same place where `GRANT_API_KEY` is registered (e.g. the set/list of built-in cluster privileges in `ClusterPrivilegeResolver`).

### 2.2 Kibana reserved role

- The Kibana system/reserved role (service account) must include the new `clone_api_key` cluster privilege so Kibana can call the Clone API Key endpoint on behalf of users.
- **File:** `KibanaOwnedReservedRoleDescriptors.java` (x-pack/plugin/core), method that builds the Kibana system role (e.g. `kibanaSystem(String name)`).
- **Change:** Add `"clone_api_key"` to the cluster privileges array in the same way `"grant_api_key"` is included (e.g. next to `"grant_api_key"`).

---

## 3. REST API

### 3.1 Endpoint and methods

- **Path:** `/_security/api_key/clone`
- **Methods:** `POST` (required). Optionally support `PUT` for parity with `RestGrantApiKeyAction` (which supports both POST and PUT for `/_security/api_key/grant`).

### 3.2 Request body (required)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `api_key` | string | Yes | The existing API key credential used as the source to clone. See below. |
| `name` | string | Yes | Name for the new (cloned) API key. Same validation as Create API Key (e.g. required, max length, no leading underscore, etc.). |
| `expiration` | string or `null` | No | Expiration for the cloned API key. If **omitted**: use the same expiration as the source API key. If **`null`** (explicit): no expiration (key does not expire). If **provided**: a time value (e.g. `"30d"`, `"1h"`) interpreted from the current time; same format and validation as Create API Key. |
| `metadata` | object | No | Metadata for the cloned API key. If **omitted**: the metadata is copied from the source API key (if the source has no metadata, treat as empty; the cloned key will then have only `cloned_from`). If **provided** (field present in the request body, including as empty object `{}`): the given object completely overwrites the source API key’s metadata (same semantics as Create API Key). In either case, the implementation adds a reserved `_cloned_from` field to the metadata set to the **id** of the source API key. The `_cloned_from` field must not be accepted in the request body (reserved). |

**Source API key credential:**

- The credential must be provided in **encoded form**, i.e. the same format returned when creating an API key: Base64(id + ":" + api_key_secret).
- The implementation must decode and validate the credential (see ApiKeyService: `parseApiKey`, `getCredentialsFromHeader`, or `parseCredentialsFromApiKeyString`). Invalid or expired source keys must result in an appropriate error (e.g. 401/403 or 400 with a clear message).

### 3.3 Query parameters

- **`refresh`:** Optional. Same semantics as Create API Key / Grant API Key: `true`, `false`, or `wait_for`. Controls refresh of the index after writing the new API key document.

### 3.4 Response

- **Success (200):** Response body must be identical to the existing Create API Key / Grant API Key response.
- **Reference:** `CreateApiKeyResponse` in the codebase. It includes at least:
  - `id`: new API key id
  - `name`: the name provided in the request
  - `api_key`: the secret key (only returned at creation time)
  - `expiration`: optional; expiration time in milliseconds (epoch) if set
  - `encoded`: Base64(id + ":" + api_key) for convenience

### 3.5 Errors

- **400** – Invalid request (e.g. missing `name`, invalid `expiration`, invalid `api_key` format, reserved metadata key such as `cloned_from` in request body).
- **401/403** – Invalid or expired source API key, or caller not authorized (missing `clone_api_key` or equivalent).
- **404** – Source API key not found (if distinguishable from invalid credential; otherwise 401/403 is acceptable).
- Map existing Create/Grant API key error handling where applicable (e.g. reserved metadata, name validation).

---

## 4. Implementation model

### 4.1 Follow “Grant API Key”

- Use **RestGrantApiKeyAction** and **TransportGrantApiKeyAction** as the main references for REST handler, action registration, and transport layer.
- You **must** validate the source API key as a credential. It is used to provide the source to act as a template for the new API Key, but it also used as a proof that the caller has access to the API Key secret. The API must fail if the API key is invalid (even if the `id` part refers to a valid API Key)

### 4.2 Constraints

- The validation for the API Key name is the same as for GrantApiKey
- It is not permitted to clone an expired or invalidated API Key.
- It *is* acceptable for the cloned API Key to have an expiry that extends beyond the expiry of the original API key

---

## 5. Backend components (checklist)

Implement the following in line with the existing Grant API Key and Create API Key code paths.

### 5.1 Core (x-pack/plugin/core)

1. **CloneApiKeyAction** (new)
   - Action type returning `CreateApiKeyResponse`.
   - Action name: e.g. `"cluster:admin/xpack/security/api_key/clone"`.

2. **CloneApiKeyRequest** (new)
   - Fields: source API key credential (e.g. encoded string or id+key), name, optional expiration, optional metadata, refresh policy.
   - Validation: name required and valid; source credential required; expiration optional and valid when present; metadata optional; if metadata is present, reject reserved keys (e.g. `cloned_from`).
   - Serialization/deserialization as needed for transport.

3. **ClusterPrivilegeResolver**
   - Add `CLONE_API_KEY_PATTERN` and `CLONE_API_KEY` (same pattern as `GRANT_API_KEY`).
   - Register `CLONE_API_KEY` in the built-in cluster privileges set.

4. **KibanaOwnedReservedRoleDescriptors**
   - Add `"clone_api_key"` to the Kibana system role’s cluster privileges.

### 5.2 Security plugin (x-pack/plugin/security)

5. **RestCloneApiKeyAction** (new)
   - Routes: `POST /_security/api_key/clone` (and optionally `PUT`).
   - Parse body: `api_key` (encoded API key credential; same request body field name as Grant API Key), `name`, optional `expiration`, optional `metadata`; optional `refresh` query param.
   - Implement `RestRequestFilter` and filter sensitive fields (e.g. `api_key` credential, similar to password/access_token in Grant).
   - Require `clone_api_key` (authorization is enforced by the action’s cluster privilege).
   - Build `CloneApiKeyRequest` and execute `CloneApiKeyAction`; send response via `RestToXContentListener<CreateApiKeyResponse>`.
   - Follow `RestGrantApiKeyAction` for structure, naming, and error handling (e.g. UNAUTHORIZED → FORBIDDEN for certain failures if that pattern is used).

6. **TransportCloneApiKeyAction** (new)
   - Executes on the node that receives the request.
   - High-level flow:
     - Validate request.
     - Parse source API key credential (e.g. using `ApiKeyService.parseApiKey` or equivalent) to get id and secret.
     - Authenticate/validate the source API key (e.g. via `ApiKeyService` – ensure the key exists, is not invalidated, and the secret matches). If invalid or expired, fail with an appropriate error.
     - Load the source API key document from the security index to obtain its **role descriptors**, **metadata** (if request does not provide metadata), and optionally current expiration if `expiration` is omitted. Reuse existing logic (e.g. from `ApiKeyService` / GetApiKey or internal doc fetch by id) to get role_descriptors, metadata, and expiry.
     - **Metadata:** If the request includes `metadata` (field present, including empty object `{}`), use it as the base metadata for the new key (after validation; reject reserved keys such as `cloned_from`). If the request omits `metadata`, use the source API key’s metadata (if the source has none, use an empty map). In either case, the implementation must add a `_cloned_from` field to the metadata with the value of the **id** of the source API key (e.g. in TransportCloneApiKeyAction before calling createApiKey, or inside ApiKeyService when invoked for clone).
     - Build a `CreateApiKeyRequest` (or equivalent) with: the **new name**, the **source’s role descriptors**, the **metadata** (as above, including `cloned_from`), and expiration = request expiration if provided, else source’s expiration, else null.
     - The `creator` of the new API Key should be the creator of the source API Key. This is part of the cloning process.
     - There are existing checks in place that prevent API keys from creating other API keys. These checks should **not** apply in this case because we are explicitly cloning the API Key, _however_ those checks must remain in place for other places in which API keys are created.
     - Return `CreateApiKeyResponse` (same as Create/Grant API Key).

7. **Security plugin registration**
   - Register the new action handler: `CloneApiKeyAction.INSTANCE` → `TransportCloneApiKeyAction`.
   - Register the new REST handler: `RestCloneApiKeyAction` (with settings and license state as for other API key actions).
   - Ensure the clone action is covered by the same license/scope as Create/Grant API Key (e.g. `ServerlessScope` if applicable).

### 5.3 REST API spec and docs

8. **rest-api-spec**
   - Add `security.clone_api_key.json` (or equivalent) under `rest-api-spec/src/main/resources/rest-api-spec/api/`, following `security.grant_api_key.json`:
     - Path: `/_security/api_key/clone`, method(s): POST (and PUT if supported).
     - Params: `refresh` (optional).
     - Body: required; description mentioning `api_key`, `name`, optional `expiration`, optional `metadata`.

9. **Documentation**
   - Update security/privileges documentation to list `clone_api_key` (e.g. in `docs/reference/elasticsearch/security-privileges.md` or equivalent), mirroring `grant_api_key` (e.g. “Clone an API key” / serverless availability if applicable).

### 5.4 Tests

10. **Unit tests**
    - `RestCloneApiKeyActionTests`: parse body (`api_key` encoded, name, expiration null/omitted/value, metadata omitted/present); filtered fields; reject `cloned_from` in request metadata.
    - `TransportCloneApiKeyActionTests`: success path; invalid source credential; missing name; expiration inherited vs overridden vs null; **metadata omitted** (copied from source, assert `cloned_from` is set to source key id); **metadata provided** (assert request metadata overwrites source and `_cloned_from` is added with source key id); reject request metadata containing `_cloned_from`.
    - Cluster privilege and Kibana role: ensure `clone_api_key` is resolved and that the Kibana role includes it.

11. **Integration / REST tests**
    - Call `POST /_security/api_key/clone` with valid source (encoded), name, and optional expiration; assert response shape matches Create API Key response.
    - Test without `clone_api_key` → 403.
    - Test with `manage_security` instead of `clone_api_key` → Allowed.
    - Test with missing, invalid (incorrect `api_key` secret) or expired source credential → 401/403/400 as designed.
    - Test expiration: omitted (same as source), null (no expiry), value (new expiry).
    - **Metadata:** Create source API key with metadata (e.g. `{"foo":"bar"}`); clone without `metadata` → new key has same metadata plus `cloned_from` = source id; clone with `metadata: {"baz":"qux"}` → new key has only `{"baz":"qux","_cloned_from": "<source_id>"}`; clone with `metadata: {}` → new key has only `{"_cloned_from": "<source_id>"}`; source with no metadata, clone without `metadata` → new key has only `_cloned_from`; verify request with `metadata._cloned_from` is rejected (400).
    - Optional: yaml rest test under `x-pack/plugin/src/yamlRestTest/resources/rest-api-spec/test/api_key/` for clone (e.g. `13clone.yml` or similar), including metadata scenarios above.

---

## 6. Security and validation

- **Sensitive data:** The request body contains the source API key secret. It must be filtered from logs and audit (e.g. via `RestCloneApiKeyAction.getFilteredFields()`), similar to `password` and `access_token` in Grant API Key.
- **Validation:** Reuse Create API Key validation for `name` (length, trim, leading underscore, etc.) and for `expiration` (time value format; if present, must be in the future). Reject reserved metadata keys in the request: the same keys reserved for Create API Key (if any), plus `cloned_from` for clone; `_cloned_from` is added by the implementation only.
- **Authorization:** Only the cluster privilege `clone_api_key` is required; no separate index or document-level privilege for the source key beyond validating that the source credential is correct.

---

## 7. Summary

| Item | Detail |
|------|--------|
| **Privilege** | New cluster privilege `clone_api_key`, same pattern as `grant_api_key`; Kibana reserved role includes it. |
| **Endpoint** | `POST /_security/api_key/clone` (optional: `PUT`). |
| **Body** | `api_key` (encoded API key credential), `name` (required), `expiration` (optional; omit = same as source, `null` = no expiry, value = new expiry), `metadata` (optional; omit = copy from source, provide = overwrite; `cloned_from` reserved, added by implementation). |
| **Response** | Same as Create API Key / Grant API Key (`CreateApiKeyResponse`). |
| **Behavior** | New API key with same role descriptors as source; new id, new name, new or inherited expiry; metadata from source or request (with `cloned_from` = source id always added). Caller must have `clone_api_key`. |
| **Reference** | Follow `RestGrantApiKeyAction` and `TransportGrantApiKeyAction`; use `ApiKeyService` for credential parsing, validation, and createApiKey. |

This spec is intended to be implemented by a coding agent with no further design decisions required; any minor naming (e.g. exact action name string) should match existing conventions in the codebase (e.g. `GrantApiKeyAction.NAME`).
