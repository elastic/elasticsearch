/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
/**
 * Executes the Data Stream Lifecycle (DLM) transition of data stream backing indices to the frozen tier.
 *
 * <h2>Summary</h2>
 * <p>
 * A user sets {@code frozen_after} on the lifecycle of a data stream. When a backing index is older than
 * {@code frozen_after}, DLM converts it to a partially mounted searchable snapshot.
 * The mounted index replaces the original index in the data stream. Then DLM deletes the original index.
 * <p>
 * The feature has two halves:
 * <ol>
 *   <li><b>Marking</b> (module {@code data-streams}, always installed, NOT in this package). On each DLM run,
 *       {@code DataStreamLifecycleService} finds backing indices past {@code frozen_after}. If
 *       {@code repositories.default_repository} is set, {@code MarkIndicesForFrozenTask} writes a marker into the
 *       {@code data_stream_lifecycle} custom metadata of each index: {@code dlm_freeze_with: <repository name>}.
 *       This marker is the only persisted state of a transition.</li>
 *   <li><b>Execution</b> (this package, stateful only, elected master only). A periodic scan finds marked indices
 *       and submits each one to a bounded executor. A worker thread runs {@link DLMConvertToFrozen}, which does
 *       seven idempotent, blocking steps.</li>
 * </ol>
 * <p>
 * The plugin is {@code STATEFUL_ONLY}. On serverless, the default repository setting is blocked, so marking never
 * occurs. The failure store does not support {@code frozen_after}; only backing indices are candidates.
 *
 * <h2>Code map</h2>
 * <dl>
 *   <dt>{@link DLMFrozenTransitionPlugin}</dt>
 *   <dd>Wiring: the {@code dlm_frozen_transition} thread pool, the services, the settings and the health
 *       indicator.</dd>
 *   <dt>{@link AbstractDLMPeriodicMasterOnlyService}</dt>
 *   <dd>Base class for the periodic services. It starts a single-thread scheduler when the local node becomes
 *       master, and shuts it down when the node stops being master.</dd>
 *   <dt>{@link DLMFrozenTransitionService}</dt>
 *   <dd>Scans all projects for marked indices on each tick and submits them. Holds the kill switch check and
 *       the skip checks.</dd>
 *   <dt>{@link DLMFrozenTransitionExecutor}</dt>
 *   <dd>Bounded submission, one task per {@code (ProjectId, index name)}, status tracking ({@code QUEUED},
 *       {@code RUNNING}), exception classification, and the unmark task queue.</dd>
 *   <dt>{@link DLMConvertToFrozen}</dt>
 *   <dd>The main runnable that conducts the actual transition of an index, step by step</dd>
 *   <dt>{@link DLMFrozenTransitionRunnable}</dt>
 *   <dd>Interface for a transition task (index and project), as the executor sees it.</dd>
 *   <dt>{@link DLMUnrecoverableException}</dt>
 *   <dd>Signals "remove the marker, do not try again with this marker value".</dd>
 *   <dt>{@link UnmarkIndexForFrozenTask}</dt>
 *   <dd>Cluster state task that removes {@code dlm_freeze_with}.</dd>
 *   <dt>{@link DLMFrozenCleanupService}</dt>
 *   <dd>Periodic, best-effort deletion of orphan clones, mounted indices and snapshots.</dd>
 *   <dt>{@link DLMFrozenTransitionSettings}</dt>
 *   <dd>Dynamic settings: the kill switch {@code dlm.frozen_transitions.enabled} and the health stuck threshold.</dd>
 *   <dt>{@link DLMFrozenTransitionHealthInfoPublisher}, {@link DLMFrozenTransitionsHealthIndicatorService}</dt>
 *   <dd>Publish overdue counts per state to the health node, and turn them into the
 *       {@code dlm_frozen_transitions} health indicator.</dd>
 *   <dt>{@link DLMFrozenTransitionInfoProvider}</dt>
 *   <dd>SPI bridge from the explain API ({@code frozen_transition_status}) to the executor status. Registered in
 *       {@code META-INF/services/org.elasticsearch.datastreams.lifecycle.FrozenTransitionInfoProvider}.</dd>
 * </dl>
 * <p>
 * Related code outside this module:
 * <ul>
 *   <li>{@code server/.../cluster/metadata/DataStreamLifecycle}: the {@code frozen_after} lifecycle field,
 *       validation and wire format.</li>
 *   <li>{@code modules/data-streams/.../lifecycle/DataStreamLifecycleService}: candidate selection
 *       ({@code candidatesForFrozen}), {@code maybeMarkIndicesForFrozen}, {@code frozenTransitionCompleted},
 *       the constants {@code FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY} and {@code DLM_CREATED_SETTING}, and the
 *       deletion of the backing snapshot when retention deletes a mounted index.</li>
 *   <li>{@code modules/data-streams/.../lifecycle/MarkIndicesForFrozenTask}: writes the marker.</li>
 *   <li>{@code modules/data-streams/.../lifecycle/FrozenTransitionInfoProvider} and
 *       {@code TransportExplainDataStreamLifecycleAction}: explain API status.</li>
 *   <li>{@code DataStreamLifecycleErrorStore} (server): the error store that this package shares with DLM.</li>
 * </ul>
 *
 * <h2>Data flow</h2>
 * <ol>
 *   <li><b>Mark</b> ({@code data-streams} module, every {@code data_streams.lifecycle.poll_interval}).
 *       {@code DataStreamLifecycleService} selects backing indices past {@code frozen_after}. If a default
 *       repository is set, {@code MarkIndicesForFrozenTask} writes {@code dlm_freeze_with: <repository>} on each
 *       original index in the cluster state.</li>
 *   <li><b>Scan</b> (this package, master only, every {@code dlm.frozen.transition.poll_interval}, default 5m).
 *       {@link DLMFrozenTransitionService} reads the markers from the cluster state. It submits each marked index
 *       that is not already submitted, while the executor has capacity.</li>
 *   <li><b>Execute.</b> {@link DLMFrozenTransitionExecutor} runs the task on the {@code dlm_frozen_transition}
 *       pool. {@link DLMConvertToFrozen} does the seven blocking steps through client calls with the DLM
 *       origin.</li>
 *   <li><b>Finish.</b> One of these occurs:
 *     <ul>
 *       <li>Success: the original index is deleted, so its marker goes with it. The next scan finds nothing.</li>
 *       <li>{@link DLMUnrecoverableException}: {@link UnmarkIndexForFrozenTask} removes the marker. The next DLM
 *           run can mark the index again (step 1).</li>
 *       <li>Any other error: the error goes to {@code DataStreamLifecycleErrorStore}. The marker stays, so the
 *           next scan submits the index again (step 2).</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h2>Names of the artifacts</h2>
 * <ul>
 *   <li><b>Original:</b> the backing index, for example {@code .ds-logs-...-000001}. It has the marker
 *       {@code dlm_freeze_with: <repository>}.</li>
 *   <li><b>Clone:</b> {@code dlm-clone-<original>}, with 0 replicas. DLM creates it only if the original has
 *       replicas.</li>
 *   <li><b>Snapshot:</b> {@code dlm-frozen-<force merge target>}, with the user metadata
 *       {@code {"dlm-created": true}}. The force merge target is either the clone or the original, after the
 *       force merge.</li>
 *   <li><b>Mounted index:</b> {@code dlm-frozen-<original>}. It always takes its name from the ORIGINAL index.</li>
 * </ul>
 * <p>
 * <b>The snapshot name and the mounted index name are not always the same.</b> The snapshot name comes from the
 * force merge target ({@code snapshotName(forceMergeIndex)}). The mounted index name always comes from the original
 * index ({@code snapshotName(indexName)}). If the original has replicas, the snapshot is
 * {@code dlm-frozen-dlm-clone-<original>}. If it has 0 replicas, the snapshot is {@code dlm-frozen-<original>}.
 * <p>
 * DLM sets the internal index setting {@code index.dlm.frozen.created: true} on the clone and on the mounted index.
 * The scan skips every index that has it. The marking side skips an index that has it and also has a searchable
 * snapshot store.
 *
 * <h2>Core design rules</h2>
 *
 * <h3>1. Stateless execution</h3>
 * <p>
 * The only persisted state is the {@code dlm_freeze_with} marker on the original index. No step writes a progress
 * marker. Each step finds its own status from facts in the cluster state or the repository: a {@code WRITE} block,
 * a clone that exists, the segment count, {@code SnapshotsInProgress}, a snapshot in the repository, a mounted index
 * that exists, the data stream membership. Derived facts cannot become out of date relative to the real artifacts,
 * for example after a master crash between an action and a marker update.
 * <p>
 * <b>Rule:</b> every step must have a skip check that uses only the cluster state or the repository. A wrong skip
 * check causes repeated work or data loss (see the mount wait in rule 5).
 *
 * <h3>2. The original index is the source of truth</h3>
 * <ul>
 *   <li>If the original index exists (same name and UUID), the transition is not complete. A marked original is
 *       submitted again on each scan until it is gone or unmarked.</li>
 *   <li>If it does not exist, {@code checkIfEligibleForConvertToFrozen} throws {@code IndexNotFoundException} and
 *       {@code run()} exits quietly with a WARN log. No error store entry.</li>
 *   <li>The cleanup step deletes the original <b>last</b>: swap, then delete the clone, then delete the original. A
 *       crash at any earlier point leaves the marker in place, and the next master resumes.</li>
 *   <li>The marker is on the original, so it goes away with the original. No separate "done" flag exists.</li>
 * </ul>
 *
 * <h3>3. No stop after start</h3>
 * <p>
 * After DLM marks an index, a policy change does not remove the marker. A conversion is destructive (it deletes the
 * original), so a partial undo would be complex and risky.
 * <ul>
 *   <li>Removing {@code frozen_after}, or changing the default repository: no effect on a marked index.</li>
 *   <li>{@code dlm.frozen_transitions.enabled=false} (kill switch) or {@code index.lifecycle.skip=true}: new
 *       submissions stop, including resumes after a failure or a master change. A task that runs now
 *       continues. The marker stays, so work continues when the
 *       setting changes back.</li>
 *   <li>Master change: the old master interrupts its workers. The new master resumes from the marker.</li>
 *   <li><b>Only a {@link DLMUnrecoverableException} removes the marker.</b> Current triggers:
 *     <ul>
 *       <li>the marker has no repository value;</li>
 *       <li>the repository in the marker is not registered;</li>
 *       <li>the repository in the marker is read-only;</li>
 *       <li>in the cleanup step, the original is no longer in a data stream and the {@code dlm-frozen-<original>}
 *           index is not a DLM mount of this transition in a data stream (see rule 6).</li>
 *     </ul>
 *     After unmark, if the index is still a backing index of a data stream, the next DLM run can mark it again with
 *     the <b>current</b> default repository. There is no user API that removes the marker.</li>
 * </ul>
 *
 * <h3>4. The repository is fixed at mark time</h3>
 * <p>
 * Every step reads the repository name from the marker on the <b>original</b> index (also when the force merge
 * target is the clone). Never use the current default repository inside a transition. A missing license is NOT
 * unrecoverable: it goes to the error store, and the index is tried again on each scan.
 *
 * <h3>5. Never swap in an index that is not available</h3>
 * <p>
 * {@code waitForMountedIndexToBeAvailable} always runs before the cleanup, also when the mount step was skipped. A
 * mount with {@code waitForCompletion=true} adds the index to the cluster state before allocation succeeds. Without
 * the wait, a failed mount left an unallocated index that passed the mount skip check on the next tick. The cleanup
 * then swapped it in and deleted the original (data loss, #149873). An index that can never be allocated is tried
 * again forever; the original stays safe.
 *
 * <h3>6. Never delete an original whose data may not be in a data stream</h3>
 * <p>
 * If the original index is in a data stream, the cleanup swaps it with the mounted index in one
 * {@code ModifyDataStreamsAction} request (remove and add). If the original is NOT in a data stream, the cleanup
 * deletes it only if {@code dlm-frozen-<original>} is in a data stream, has {@code index.dlm.frozen.created} and a
 * searchable snapshot store, and is mounted from a snapshot of the original or its clone. Otherwise it throws
 * {@link DLMUnrecoverableException} (#167129). Example: a user removes the original from its data stream during
 * the transition. The original is kept, and the cleanup service later deletes the clone, mount and snapshot as
 * orphans.
 *
 * <h3>7. Strict snapshot validation</h3>
 * <p>
 * The snapshot request uses {@code partial=true}, because a non-partial snapshot can block cluster actions such as
 * an index delete. But the step continues only if {@code state == SUCCESS}, 0 failed shards, more than 0 successful
 * shards, and the snapshot contains the target index. An invalid snapshot is deleted and created again.
 *
 * <h3>8. Wire format compatibility</h3>
 * <p>
 * The {@code dlm_searchable_snapshots} feature flag is still read in the {@code frozen_after} wire format of
 * {@code DataStreamLifecycle}. Do not remove it without a BWC plan (#150457).
 *
 * <h2>Master ownership</h2>
 * When the node stops being master, {@link AbstractDLMPeriodicMasterOnlyService} calls {@code shutdownNow()} on the
 * scheduler and then {@code onStop()}. For the transition service, this calls {@link DLMFrozenTransitionExecutor#stop()},
 * which cancels every future with {@code cancel(true)} and so interrupts the workers. This interruption is required
 * to prevent concurrent transitions on two nodes. {@code Future#cancel} is a forbidden API, suppressed on purpose.
 * <p>
 * A request that the old master already sent (a snapshot, a force merge) continues on the cluster. The new master
 * finds it through the skip checks, for example the snapshot in {@code SnapshotsInProgress}. For a short time, two
 * masters can both think they own a transition. The idempotent steps make this safe. Keep this in mind when you add
 * or change a step.
 *
 * <h2>Exception handling</h2>
 * <p>
 * {@link DLMConvertToFrozen#run()} restores the interrupt flag on {@code InterruptedException}. It returns quietly
 * on an {@code IndexNotFoundException} for the ORIGINAL index name only. It throws all other exceptions to
 * {@code DLMFrozenTransitionExecutor.WrappedDlmFrozenTransitionRunnable}, which classifies them:
 * <ul>
 *   <li>{@link DLMUnrecoverableException}: submit {@link UnmarkIndexForFrozenTask} (queue
 *       {@code dlm-unmark-index-for-frozen}, {@code Priority.LOW}). The executor map entry is removed only when the
 *       unmark completes, so the scan cannot submit the index again before the marker is gone. If the unmark fails,
 *       record it in the error store.</li>
 *   <li>Transient master failover ({@code DLMConvertToFrozen.isTransientMasterFailoverException}): log at DEBUG,
 *       do not record. These are not real faults.</li>
 *   <li>All others: record in {@code DataStreamLifecycleErrorStore} against the original {@code Index}. The executor
 *       never clears the error on success. The DLM run clears it after the original index is gone.</li>
 * </ul>
 *
 * <h2>Executor invariants</h2>
 * <ul>
 *   <li>At most one task per {@code (ProjectId, index name)}. The key includes the project because two projects can
 *       have an index with the same name.</li>
 *   <li>Capacity is {@code pool max + queue size}. The scan stops when there is no capacity.</li>
 *   <li>{@link DLMFrozenTransitionExecutor#stop()} replaces the map with a new map. Each wrapper holds the map it was
 *       registered in, so a late completion from an old master term cannot remove an entry of a new term.</li>
 *   <li>{@code QUEUED} and {@code RUNNING} status lives only in the memory of the current master. After a master
 *       change, explain shows {@code marked} until the new master submits the index.</li>
 * </ul>
 *
 * <h2>Orphan cleanup ({@link DLMFrozenCleanupService})</h2>
 * <p>
 * Best effort: it never throws, and it logs failures at WARN. For each project it deletes:
 * <ul>
 *   <li>indices with {@code index.dlm.frozen.created} that are orphans;</li>
 *   <li>snapshots {@code dlm-frozen-*} in the CURRENT default repository only, if the user metadata has
 *       {@code dlm-created=true}, the snapshot has exactly one index, that index is an orphan, and no mounted index
 *       uses the snapshot UUID.</li>
 * </ul>
 * An index is an orphan if it is not in a data stream, and its source is missing or not in a data stream. To find
 * the source, follow {@code index.store.snapshot.index_name} (mounted index to snapshotted index), then the resize
 * source (clone to original). Snapshots in a repository that is no longer the default are never reclaimed.
 *
 * <h2>Checklist for changes</h2>
 * <ul>
 *   <li>A new step needs a skip check based only on the cluster state or the repository, and must be safe if two
 *       masters run it at the same time.</li>
 *   <li>Do not delete the original before the mounted index is available and in its data stream.</li>
 *   <li>Do not add process state (progress markers, in-memory flags that must survive a master change).</li>
 *   <li>Do not read the default repository inside a transition; use the marker.</li>
 *   <li>Throw {@link DLMUnrecoverableException} only when trying again with the same marker can never succeed. It
 *       removes the marker.</li>
 *   <li>Every blocking call must respond to interruption. Call {@code checkIfThreadInterrupted()} at the start of
 *       each step.</li>
 *   <li>Keep artifact names stable. The cleanup service, the marking side and the skip checks all depend on the
 *       {@code dlm-clone-} and {@code dlm-frozen-} prefixes and on {@code index.dlm.frozen.created}.</li>
 *   <li>Integration tests for a master change in each step are in {@code DLMFrozenTransitionDisruptionIT}.</li>
 * </ul>
 */
package org.elasticsearch.xpack.dlm.frozen;
