# Stream GET: PR split plan

Plan for splitting the `stream-get-no-client-streaming` branch into reviewable PRs.
Written 2026-11-22. No size limit per PR: each PR is one thing a reviewer can understand in one sitting.

## The plan

| # | PR | What it adds | Depends on | Rough size | Status |
| :-- | :-- | :-- | :-- | :-- | :-- |
| 1 | [#167013](https://github.com/elastic/elasticsearch/pull/167013) | `GET` and `DELETE /_query/stream/{id}` endpoints, placeholder: every id is a 404. Security wiring, request validation, 5-minute `wait_timeout` cap | none | ~1,160 | Open |
| 2 | [#167027](https://github.com/elastic/elasticsearch/pull/167027) | `NdjsonRowWriter`: the row format shared by POST and GET | none | ~600 | Open |
| 3 | Shared header and footer | `NdjsonStreamLines`; POST switched to it, no output change | 2 | ~450 | Built locally, not pushed |
| 4 | Read path | One read of a stored stream, no wait and no ownership check: `StreamPages`, the storage read in `StreamTaskManagementService`, `GetStreamResultResponse`, `StreamResultsService` | 1 | ~1,300 | Partly built locally |
| 5 | GET response body | `StreamNdjsonBody`: writes a read as NDJSON on the shared format | 3, 4 | ~1,100 | To do |
| 6 | Security | `StreamSearchSecurity`, called from the results service. Unknown, expired and not-yours all give the same 404 | 4 | ~350 | Partly built locally |
| 7 | Polling | The wait loop; the service honors `wait_timeout` | 4 | ~500 | To do |
| 8 | Wiring and integration test | GET handler and transport action switched from the placeholder. Test plugin with in-memory storage. `StreamEsqlGetResultActionIT` | 5, 6, 7 | ~750 | To do |

Sizes are estimates from the branch's line counts. Earlier estimates ran low once tests were filled in.

## Dependencies

```
#167013 ──────────────► 4 read path ──┬─► 5 body ─────┐
#167027 ─► 3 header/footer ───────────┘               ├─► 8 wiring + IT
                         4 ──► 6 security ────────────┤
                         4 ──► 7 polling ─────────────┘
```

- PRs 5, 6 and 7 are independent of each other and can be reviewed in parallel once PR 4 merges.
- PRs 6 and 7 both change `StreamResultsService`, so whichever merges second needs a small rebase.

## Why this grouping

- **Each PR lands with its caller.** The results service arrives in PR 4, so security (PR 6) and polling (PR 7) each change a
  service that already exists. Nothing is added as a class that no one calls.
- **Wiring is last.** Until PR 8 nothing reaches the service from HTTP, so the gap between PR 4 and PR 6, where the service has
  no ownership check, is never exposed.
- **The body is its own PR.** It is the largest piece and the one where the response format is reviewed.

## What a client sees, and how each PR is validated

- A client gets a 404 from `GET /_query/stream/{id}` until real stream storage exists. That is true even after PR 8, because
  production uses `NoopStreamStore`. Only the test plugin, with an in-memory store, returns rows.
- PRs 4 to 7 are validated by unit tests with real classes: a real `InMemoryStreamStore`, real pages from a circuit-breaking
  block factory, and a check that the breaker returns to zero.
- PR 8 adds the first end-to-end test: a real node, the test plugin's in-memory store, rows read back.

What PR 4's tests can check: the rows for an `offset` and `max_rows` across pages, the status that comes back, status-only
requests, an offset beyond the end, not-found handling, the read order (metadata before pages, one page read per request),
and that every page is released.

What PR 4's tests cannot check: `max_bytes` (enforced while rows are written, so PR 5), the NDJSON text and HTTP status
(PR 5), and anything end to end (PR 8).

## Decisions already made

- **GET follows the POST format.** Header, rows and footer are shared.
- **Null-fill.** Where POST writes a field that GET has no value for yet (`took` while the stream is running, the driver
  statistics, `_clusters`), GET writes `null` in the same place. POST keeps leaving out what it has no value for.
- **`id` and `query_status`** stay in the GET header as additions to the POST format.
- **Error shape.** A stored failure is written in the POST error shape, through `NdjsonStreamLines.storedError`.
- **`wait_timeout`** is capped at 5 minutes. A larger value is a 400. Unset, `0` or `-1` means no wait.
- **Placeholder behavior.** Until storage exists, GET and DELETE answer 404 for every id.
- **Unset limits** use `NO_LIMIT = -1`, as `StreamOptions` does for the POST.
- **`max_rows=0` and `max_bytes=0`** both mean "status only" on GET. POST rejects `max_bytes=0`.
- **No YAML REST test** for the placeholder endpoints.

## Work that is more than copying from the branch

1. **PR 5, the body, must be rewritten onto the shared format.** The branch's `StreamNdjsonBody` writes its own header and
   footer. It has to:
   - write the header and footer through `NdjsonStreamLines`;
   - null-fill `took`, the statistics and `_clusters`;
   - write a stored failure through `storedError`;
   - use `RestEsqlStreamQueryAction.NDJSON_MEDIA_TYPE` (the `NDJSON_CONTENT_TYPE` constant was removed in #167027).

   `StreamNdjsonBodyTests` (33 tests) asserts the old footer, so most of its expected values change.
2. **PR 7, polling, should rely on the 5-minute cap.** The branch guards against overflow with `MAX_WAIT_NANOS`, which the
   cap from #167013 makes unnecessary.
3. **PRs 4 and 8 reconcile with #167013.** Its request class, handler and transport action differ from the branch: the cap,
   the always-404 placeholder, and `ActionResponse.Empty` as the GET response type.
4. **Moved pieces.** The `StreamMetadata` nested types become `public` in PR 5, where another package first needs them. The
   `StreamStorage.markComplete` Javadoc goes with PR 4.

## Local branches

| Branch | Holds | For |
| :-- | :-- | :-- |
| `stream-get-no-client-streaming` | The full original work | Source for all PRs |
| `stream-get-delete-endpoints` | PR 1 | #167013 |
| `stream-ndjson-2a-row-writer` | PR 2 | #167027 |
| `stream-ndjson-2b-header-footer` | PR 3, stacked on PR 2 | Open once #167027 merges, or target it at the PR 2 branch |
| `stream-get-3-single-read` | `StreamPages`, the storage read, the test base and 20 tests | Start of PR 4 |
| `stream-get-5-ownership-check` | `StreamSearchSecurity` and its tests | PR 6 |
| `stream-get-2-ndjson-row-writer` | An older copy of PR 2 | Superseded, can be deleted |
| `stream-get-1-building-blocks` | An earlier attempt at the split | Superseded |

`stream-ndjson-2b-header-footer` must be rebased whenever a new commit is pushed to `stream-ndjson-2a-row-writer`.

## Not covered by this plan

- The real DELETE implementation, on the `stream-delete` branch.
- Real stream storage (the system index). Until it exists, nothing is stored and every id is a 404.
- Storing the time zone with the stream. Until then GET writes dates in UTC, while POST uses the time zone of the request.
- The ownership check for DELETE, noted as a TODO in `RBACEngine` and in `StreamSearchSecurity`.
- An authenticated end-to-end test over HTTP, deferred from #167013's review to the storage and ownership work.
