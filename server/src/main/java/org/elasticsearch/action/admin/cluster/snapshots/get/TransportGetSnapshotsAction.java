/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.AbstractThrottledTaskRunner;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.ResolvedRepositories;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetSnapshotsAction.class);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetSnapshotsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSnapshotsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSnapshotsRequest::new,
            indexNameExpressionResolver,
            GetSnapshotsResponse::new,
            // Execute this on the management pool because creating the response can become fairly expensive
            // for large repositories in the verbose=false case when there are a lot of indices per snapshot.
            // This is intentionally not using the snapshot_meta pool because that pool is sized rather large
            // to accommodate concurrent IO and could consume excessive CPU resources through concurrent
            // verbose=false requests that are CPU bound only.
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final GetSnapshotsRequest request,
        final ClusterState state,
        final ActionListener<GetSnapshotsResponse> listener
    ) {
        assert task instanceof CancellableTask : task + " not cancellable";

        new GetSnapshotsOperation(
            (CancellableTask) task,
            ResolvedRepositories.resolve(state, request.repositories()),
            request.isSingleRepositoryRequest() == false,
            request.snapshots(),
            request.ignoreUnavailable(),
            request.policies(),
            request.sort(),
            request.order(),
            request.fromSortValue(),
            request.offset(),
            request.after(),
            request.size(),
            SnapshotsInProgress.get(state),
            request.verbose(),
            request.includeIndexNames()
        ).getMultipleReposSnapshotInfo(listener);
    }

    /**
     * A single invocation of the get-snapshots API.
     * <p>
     * Decides which repositories to query, picks a collection of candidate {@link SnapshotId} values from each {@link RepositoryData},
     * chosen according to the request parameters, loads the relevant {@link SnapshotInfo} blobs, and finally sorts and filters the
     * results.
     */
    private class GetSnapshotsOperation {
        private final CancellableTask cancellableTask;

        // repositories
        private final List<RepositoryMetadata> repositories;
        private final boolean isMultiRepoRequest;

        // snapshots selection
        private final SnapshotNamePredicate snapshotNamePredicate;
        private final SnapshotPredicates fromSortValuePredicates;
        private final Predicate<String> slmPolicyPredicate;

        // snapshot ordering/pagination
        private final SnapshotSortKey sortBy;
        private final SortOrder order;
        @Nullable
        private final String fromSortValue;
        private final int offset;
        private final Predicate<SnapshotInfo> afterPredicate;
        private final int size;

        // current state
        private final SnapshotsInProgress snapshotsInProgress;

        // output detail
        private final boolean ignoreUnavailable;
        private final boolean verbose;
        private final boolean indices;

        // snapshot info throttling
        private final GetSnapshotInfoExecutor getSnapshotInfoExecutor;

        // results
        private final Map<String, ElasticsearchException> failuresByRepository = ConcurrentCollections.newConcurrentMap();
        private final Queue<List<SnapshotInfo>> allSnapshotInfos = ConcurrentCollections.newQueue();

        /**
         * Accumulates number of snapshots that match the name/fromSortValue/slmPolicy predicates, to be returned in the response.
         */
        private final AtomicInteger totalCount = new AtomicInteger();

        /**
         * Accumulates the number of snapshots that match the name/fromSortValue/slmPolicy/after predicates, for sizing the final result
         * list.
         */
        private final AtomicInteger resultsCount = new AtomicInteger();

        GetSnapshotsOperation(
            CancellableTask cancellableTask,
            ResolvedRepositories resolvedRepositories,
            boolean isMultiRepoRequest,
            String[] snapshots,
            boolean ignoreUnavailable,
            String[] policies,
            SnapshotSortKey sortBy,
            SortOrder order,
            String fromSortValue,
            int offset,
            SnapshotSortKey.After after,
            int size,
            SnapshotsInProgress snapshotsInProgress,
            boolean verbose,
            boolean indices
        ) {
            this.cancellableTask = cancellableTask;
            this.repositories = resolvedRepositories.repositoryMetadata();
            this.isMultiRepoRequest = isMultiRepoRequest;
            this.ignoreUnavailable = ignoreUnavailable;
            this.sortBy = sortBy;
            this.order = order;
            this.fromSortValue = fromSortValue;
            this.offset = offset;
            this.size = size;
            this.snapshotsInProgress = snapshotsInProgress;
            this.verbose = verbose;
            this.indices = indices;

            this.snapshotNamePredicate = SnapshotNamePredicate.forSnapshots(ignoreUnavailable, snapshots);
            this.fromSortValuePredicates = SnapshotPredicates.forFromSortValue(fromSortValue, sortBy, order);
            this.slmPolicyPredicate = SlmPolicyPredicate.forPolicies(policies);
            this.afterPredicate = sortBy.getAfterPredicate(after, order);

            this.getSnapshotInfoExecutor = new GetSnapshotInfoExecutor(
                threadPool.info(ThreadPool.Names.SNAPSHOT_META).getMax(),
                cancellableTask::isCancelled
            );

            for (final var missingRepo : resolvedRepositories.missing()) {
                failuresByRepository.put(missingRepo, new RepositoryMissingException(missingRepo));
            }
        }

        void getMultipleReposSnapshotInfo(ActionListener<GetSnapshotsResponse> listener) {
            SubscribableListener

                .<Void>newForked(repositoriesDoneListener -> {
                    try (var listeners = new RefCountingListener(repositoriesDoneListener)) {
                        for (final RepositoryMetadata repository : repositories) {
                            final String repoName = repository.name();
                            if (skipRepository(repoName)) {
                                continue;
                            }

                            SubscribableListener

                                .<RepositoryData>newForked(repositoryDataListener -> {
                                    if (snapshotNamePredicate == SnapshotNamePredicate.MATCH_CURRENT_ONLY) {
                                        repositoryDataListener.onResponse(null);
                                    } else {
                                        repositoriesService.repository(repoName).getRepositoryData(executor, repositoryDataListener);
                                    }
                                })

                                .<Void>andThen((l, repositoryData) -> loadSnapshotInfos(repoName, repositoryData, l))

                                .addListener(listeners.acquire().delegateResponse((l, e) -> {
                                    if (isMultiRepoRequest && e instanceof ElasticsearchException elasticsearchException) {
                                        failuresByRepository.put(repoName, elasticsearchException);
                                        l.onResponse(null);
                                    } else {
                                        l.onFailure(e);
                                    }
                                }));
                        }
                    }
                })

                .addListener(listener.map(ignored -> buildResponse()), executor, threadPool.getThreadContext());
        }

        private boolean skipRepository(String repositoryName) {
            if (sortBy == SnapshotSortKey.REPOSITORY && fromSortValue != null) {
                // If we are sorting by repository name with an offset given by fromSortValue, skip earlier repositories
                return order == SortOrder.ASC ? fromSortValue.compareTo(repositoryName) > 0 : fromSortValue.compareTo(repositoryName) < 0;
            } else {
                return false;
            }
        }

        private void loadSnapshotInfos(String repo, @Nullable RepositoryData repositoryData, ActionListener<Void> listener) {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);

            if (cancellableTask.notifyIfCancelled(listener)) {
                return;
            }

            final Set<String> unmatchedRequiredNames = new HashSet<>(snapshotNamePredicate.requiredNames());
            final Set<Snapshot> toResolve = new HashSet<>();

            for (final var snapshotInProgress : snapshotsInProgress.forRepo(repo)) {
                final var snapshotName = snapshotInProgress.snapshot().getSnapshotId().getName();
                unmatchedRequiredNames.remove(snapshotName);
                if (snapshotNamePredicate.test(snapshotName, true)) {
                    toResolve.add(snapshotInProgress.snapshot());
                }
            }

            if (repositoryData != null) {
                for (final var snapshotId : repositoryData.getSnapshotIds()) {
                    final var snapshotName = snapshotId.getName();
                    unmatchedRequiredNames.remove(snapshotName);
                    if (snapshotNamePredicate.test(snapshotName, false) && matchesPredicates(snapshotId, repositoryData)) {
                        toResolve.add(new Snapshot(repo, snapshotId));
                    }
                }
            }

            if (unmatchedRequiredNames.isEmpty() == false) {
                throw new SnapshotMissingException(repo, unmatchedRequiredNames.iterator().next());
            }

            if (verbose) {
                loadSnapshotInfos(repo, toResolve.stream().map(Snapshot::getSnapshotId).toList(), listener);
            } else {
                assert fromSortValuePredicates.isMatchAll() : "filtering is not supported in non-verbose mode";
                assert slmPolicyPredicate == SlmPolicyPredicate.MATCH_ALL_POLICIES : "filtering is not supported in non-verbose mode";

                addSimpleSnapshotInfos(
                    toResolve,
                    repo,
                    repositoryData,
                    snapshotsInProgress.forRepo(repo).stream().map(entry -> SnapshotInfo.inProgress(entry).basic()).toList()
                );
                listener.onResponse(null);
            }
        }

        private void loadSnapshotInfos(String repositoryName, Collection<SnapshotId> snapshotIds, ActionListener<Void> listener) {
            if (cancellableTask.notifyIfCancelled(listener)) {
                return;
            }
            final AtomicInteger repositoryTotalCount = new AtomicInteger();
            final List<SnapshotInfo> snapshots = new ArrayList<>(snapshotIds.size());
            final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
            // first, look at the snapshots in progress
            final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
                snapshotsInProgress,
                repositoryName,
                snapshotIdsToIterate.stream().map(SnapshotId::getName).toList()
            );
            for (SnapshotsInProgress.Entry entry : entries) {
                if (snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId())) {
                    final SnapshotInfo snapshotInfo = SnapshotInfo.inProgress(entry);
                    if (matchesPredicates(snapshotInfo)) {
                        repositoryTotalCount.incrementAndGet();
                        if (afterPredicate.test(snapshotInfo)) {
                            snapshots.add(snapshotInfo.maybeWithoutIndices(indices));
                        }
                    }
                }
            }
            // then, look in the repository if there's any matching snapshots left
            SubscribableListener

                .<Void>newForked(l -> {
                    try (var listeners = new RefCountingListener(l)) {
                        if (snapshotIdsToIterate.isEmpty()) {
                            return;
                        }

                        final Repository repository;
                        try {
                            repository = repositoriesService.repository(repositoryName);
                        } catch (RepositoryMissingException e) {
                            listeners.acquire().onFailure(e);
                            return;
                        }

                        // only need to synchronize accesses related to reading SnapshotInfo from the repo
                        final List<SnapshotInfo> syncSnapshots = Collections.synchronizedList(snapshots);

                        ThrottledIterator.run(
                            Iterators.failFast(
                                snapshotIdsToIterate.iterator(),
                                () -> cancellableTask.isCancelled() || listeners.isFailing()
                            ),
                            (ref, snapshotId) -> {
                                final var refListener = ActionListener.runBefore(listeners.acquire(), ref::close);
                                getSnapshotInfoExecutor.getSnapshotInfo(repository, snapshotId, new ActionListener<>() {
                                    @Override
                                    public void onResponse(SnapshotInfo snapshotInfo) {
                                        if (matchesPredicates(snapshotInfo)) {
                                            repositoryTotalCount.incrementAndGet();
                                            if (afterPredicate.test(snapshotInfo)) {
                                                syncSnapshots.add(snapshotInfo.maybeWithoutIndices(indices));
                                            }
                                        }
                                        refListener.onResponse(null);
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        if (ignoreUnavailable) {
                                            logger.warn(
                                                Strings.format("failed to fetch snapshot info for [%s:%s]", repository, snapshotId),
                                                e
                                            );
                                            refListener.onResponse(null);
                                        } else {
                                            refListener.onFailure(e);
                                        }
                                    }
                                });
                            },
                            getSnapshotInfoExecutor.getMaxRunningTasks(),
                            () -> {},
                            () -> {}
                        );
                    }
                })

                // no need to synchronize access to snapshots: Repository#getSnapshotInfo fails fast but we're on the success path here
                .andThenAccept(ignored -> addResults(repositoryTotalCount.get(), snapshots))

                .addListener(listener);
        }

        private void addResults(int repositoryTotalCount, List<SnapshotInfo> snapshots) {
            totalCount.addAndGet(repositoryTotalCount);
            resultsCount.addAndGet(snapshots.size());
            allSnapshotInfos.add(snapshots);
        }

        private void addSimpleSnapshotInfos(
            final Set<Snapshot> toResolve,
            final String repoName,
            final RepositoryData repositoryData,
            final List<SnapshotInfo> currentSnapshots
        ) {
            if (repositoryData == null) {
                // only want current snapshots
                addResults(currentSnapshots.size(), currentSnapshots.stream().filter(afterPredicate).toList());
                return;
            } // else want non-current snapshots as well, which are found in the repository data

            List<SnapshotInfo> snapshotInfos = new ArrayList<>(currentSnapshots.size() + toResolve.size());
            int repositoryTotalCount = 0;
            for (SnapshotInfo snapshotInfo : currentSnapshots) {
                assert snapshotInfo.startTime() == 0L && snapshotInfo.endTime() == 0L && snapshotInfo.totalShards() == 0L : snapshotInfo;
                if (toResolve.remove(snapshotInfo.snapshot())) {
                    repositoryTotalCount += 1;
                    if (afterPredicate.test(snapshotInfo)) {
                        snapshotInfos.add(snapshotInfo);
                    }
                }
            }
            Map<SnapshotId, List<String>> snapshotsToIndices = new HashMap<>();
            if (indices) {
                for (IndexId indexId : repositoryData.getIndices().values()) {
                    for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                        if (toResolve.contains(new Snapshot(repoName, snapshotId))) {
                            snapshotsToIndices.computeIfAbsent(snapshotId, (k) -> new ArrayList<>()).add(indexId.getName());
                        }
                    }
                }
            }
            for (Snapshot snapshot : toResolve) {
                final var snapshotInfo = new SnapshotInfo(
                    snapshot,
                    snapshotsToIndices.getOrDefault(snapshot.getSnapshotId(), Collections.emptyList()),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    repositoryData.getSnapshotState(snapshot.getSnapshotId())
                );
                repositoryTotalCount += 1;
                if (afterPredicate.test(snapshotInfo)) {
                    snapshotInfos.add(snapshotInfo);
                }
            }
            addResults(repositoryTotalCount, snapshotInfos);
        }

        private GetSnapshotsResponse buildResponse() {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
            cancellableTask.ensureNotCancelled();
            int remaining = 0;
            final var resultsStream = allSnapshotInfos.stream()
                .flatMap(Collection::stream)
                .peek(this::assertSatisfiesAllPredicates)
                .sorted(sortBy.getSnapshotInfoComparator(order))
                .skip(offset);
            final List<SnapshotInfo> snapshotInfos;
            if (size == GetSnapshotsRequest.NO_LIMIT || resultsCount.get() <= size) {
                snapshotInfos = resultsStream.toList();
            } else {
                snapshotInfos = new ArrayList<>(size);
                for (var iterator = resultsStream.iterator(); iterator.hasNext();) {
                    final var snapshotInfo = iterator.next();
                    if (snapshotInfos.size() < size) {
                        snapshotInfos.add(snapshotInfo);
                    } else {
                        remaining += 1;
                    }
                }
            }
            return new GetSnapshotsResponse(
                snapshotInfos,
                failuresByRepository,
                remaining > 0 ? sortBy.encodeAfterQueryParam(snapshotInfos.get(snapshotInfos.size() - 1)) : null,
                totalCount.get(),
                remaining
            );
        }

        private void assertSatisfiesAllPredicates(SnapshotInfo snapshotInfo) {
            assert matchesPredicates(snapshotInfo);
            assert afterPredicate.test(snapshotInfo);
            assert indices || snapshotInfo.indices().isEmpty();
        }

        private boolean matchesPredicates(SnapshotId snapshotId, RepositoryData repositoryData) {
            if (fromSortValuePredicates.test(snapshotId, repositoryData) == false) {
                return false;
            }

            if (slmPolicyPredicate == SlmPolicyPredicate.MATCH_ALL_POLICIES) {
                return true;
            }

            final var details = repositoryData.getSnapshotDetails(snapshotId);
            return details == null || details.getSlmPolicy() == null || slmPolicyPredicate.test(details.getSlmPolicy());
        }

        private boolean matchesPredicates(SnapshotInfo snapshotInfo) {
            if (fromSortValuePredicates.test(snapshotInfo) == false) {
                return false;
            }

            if (slmPolicyPredicate == SlmPolicyPredicate.MATCH_ALL_POLICIES) {
                return true;
            }

            final var metadata = snapshotInfo.userMetadata();
            return slmPolicyPredicate.test(
                metadata != null && metadata.get(SnapshotsService.POLICY_ID_METADATA_FIELD) instanceof String s ? s : ""
            );
        }
    }

    /**
     * A pair of predicates for the get snapshots action. The {@link #test(SnapshotId, RepositoryData)} predicate is applied to combinations
     * of snapshot id and repository data to determine which snapshots to fully load from the repository and rules out all snapshots that do
     * not match the given {@link GetSnapshotsRequest} that can be ruled out through the information in {@link RepositoryData}.
     * The predicate returned by {@link #test(SnapshotInfo)} predicate is then applied the instances of {@link SnapshotInfo} that were
     * loaded from the repository to filter out those remaining that did not match the request but could not be ruled out without loading
     * their {@link SnapshotInfo}.
     */
    private static final class SnapshotPredicates {

        private static final SnapshotPredicates MATCH_ALL = new SnapshotPredicates(null, null);

        @Nullable // if all snapshot IDs match
        private final BiPredicate<SnapshotId, RepositoryData> preflightPredicate;

        @Nullable // if all snapshots match
        private final Predicate<SnapshotInfo> snapshotPredicate;

        private SnapshotPredicates(
            @Nullable BiPredicate<SnapshotId, RepositoryData> preflightPredicate,
            @Nullable Predicate<SnapshotInfo> snapshotPredicate
        ) {
            this.snapshotPredicate = snapshotPredicate;
            this.preflightPredicate = preflightPredicate;
        }

        boolean test(SnapshotId snapshotId, RepositoryData repositoryData) {
            return preflightPredicate == null || preflightPredicate.test(snapshotId, repositoryData);
        }

        boolean isMatchAll() {
            return snapshotPredicate == null;
        }

        boolean test(SnapshotInfo snapshotInfo) {
            return snapshotPredicate == null || snapshotPredicate.test(snapshotInfo);
        }

        static SnapshotPredicates forFromSortValue(String fromSortValue, SnapshotSortKey sortBy, SortOrder order) {
            if (fromSortValue == null) {
                return MATCH_ALL;
            }

            switch (sortBy) {
                case START_TIME:
                    final long after = Long.parseLong(fromSortValue);
                    return new SnapshotPredicates(order == SortOrder.ASC ? (snapshotId, repositoryData) -> {
                        final long startTime = getStartTime(snapshotId, repositoryData);
                        return startTime == -1 || after <= startTime;
                    } : (snapshotId, repositoryData) -> {
                        final long startTime = getStartTime(snapshotId, repositoryData);
                        return startTime == -1 || after >= startTime;
                    }, filterByLongOffset(SnapshotInfo::startTime, after, order));

                case NAME:
                    return new SnapshotPredicates(
                        order == SortOrder.ASC
                            ? (snapshotId, repositoryData) -> fromSortValue.compareTo(snapshotId.getName()) <= 0
                            : (snapshotId, repositoryData) -> fromSortValue.compareTo(snapshotId.getName()) >= 0,
                        null
                    );

                case DURATION:
                    final long afterDuration = Long.parseLong(fromSortValue);
                    return new SnapshotPredicates(order == SortOrder.ASC ? (snapshotId, repositoryData) -> {
                        final long duration = getDuration(snapshotId, repositoryData);
                        return duration == -1 || afterDuration <= duration;
                    } : (snapshotId, repositoryData) -> {
                        final long duration = getDuration(snapshotId, repositoryData);
                        return duration == -1 || afterDuration >= duration;
                    }, filterByLongOffset(info -> info.endTime() - info.startTime(), afterDuration, order));

                case INDICES:
                    final int afterIndexCount = Integer.parseInt(fromSortValue);
                    return new SnapshotPredicates(
                        order == SortOrder.ASC
                            ? (snapshotId, repositoryData) -> afterIndexCount <= indexCount(snapshotId, repositoryData)
                            : (snapshotId, repositoryData) -> afterIndexCount >= indexCount(snapshotId, repositoryData),
                        null
                    );

                case REPOSITORY:
                    // already handled in #maybeFilterRepositories
                    return MATCH_ALL;

                case SHARDS:
                    return new SnapshotPredicates(
                        null,
                        filterByLongOffset(SnapshotInfo::totalShards, Integer.parseInt(fromSortValue), order)
                    );
                case FAILED_SHARDS:
                    return new SnapshotPredicates(
                        null,
                        filterByLongOffset(SnapshotInfo::failedShards, Integer.parseInt(fromSortValue), order)
                    );
                default:
                    throw new AssertionError("unexpected sort column [" + sortBy + "]");
            }
        }

        private static Predicate<SnapshotInfo> filterByLongOffset(ToLongFunction<SnapshotInfo> extractor, long after, SortOrder order) {
            return order == SortOrder.ASC ? info -> after <= extractor.applyAsLong(info) : info -> after >= extractor.applyAsLong(info);
        }

        private static long getDuration(SnapshotId snapshotId, RepositoryData repositoryData) {
            final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
            if (details == null) {
                return -1;
            }
            final long startTime = details.getStartTimeMillis();
            if (startTime == -1) {
                return -1;
            }
            final long endTime = details.getEndTimeMillis();
            if (endTime == -1) {
                return -1;
            }
            return endTime - startTime;
        }

        private static long getStartTime(SnapshotId snapshotId, RepositoryData repositoryData) {
            final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
            return details == null ? -1 : details.getStartTimeMillis();
        }

        private static int indexCount(SnapshotId snapshotId, RepositoryData repositoryData) {
            // TODO: this could be made more efficient by caching this number in RepositoryData
            int indexCount = 0;
            for (IndexId idx : repositoryData.getIndices().values()) {
                if (repositoryData.getSnapshots(idx).contains(snapshotId)) {
                    indexCount++;
                }
            }
            return indexCount;
        }
    }

    /**
     * Throttling executor for retrieving {@link SnapshotInfo} instances from the repository without spamming the SNAPSHOT_META threadpool
     * and starving other users of access to it. Similar to {@link Repository#getSnapshotInfo} but allows for finer-grained control over
     * which snapshots are retrieved.
     */
    private static class GetSnapshotInfoExecutor extends AbstractThrottledTaskRunner<ActionListener<Releasable>> {
        private final int maxRunningTasks;
        private final BooleanSupplier isCancelledSupplier;

        GetSnapshotInfoExecutor(int maxRunningTasks, BooleanSupplier isCancelledSupplier) {
            super(GetSnapshotsAction.NAME, maxRunningTasks, EsExecutors.DIRECT_EXECUTOR_SERVICE, ConcurrentCollections.newBlockingQueue());
            this.maxRunningTasks = maxRunningTasks;
            this.isCancelledSupplier = isCancelledSupplier;
        }

        int getMaxRunningTasks() {
            return maxRunningTasks;
        }

        void getSnapshotInfo(Repository repository, SnapshotId snapshotId, ActionListener<SnapshotInfo> listener) {
            enqueueTask(listener.delegateFailure((l, ref) -> {
                if (isCancelledSupplier.getAsBoolean()) {
                    l.onFailure(new TaskCancelledException("task cancelled"));
                } else {
                    repository.getSnapshotInfo(snapshotId, ActionListener.releaseAfter(l, ref));
                }
            }));
        }
    }

    /**
     * Encapsulates a filter on snapshots according to SLM policy, for the {@code ?slm_policy_filter} query parameter.
     */
    private record SlmPolicyPredicate(String[] includes, String[] excludes, boolean matchWithoutPolicy) implements Predicate<String> {

        static final Predicate<String> MATCH_ALL_POLICIES = Predicates.always();

        @Override
        public boolean test(String policy) {
            if (policy.equals("")) {
                // empty string means that snapshot was not created by an SLM policy
                return matchWithoutPolicy;
            }
            if (Regex.simpleMatch(includes, policy) == false) {
                return false;
            }
            return excludes.length == 0 || Regex.simpleMatch(excludes, policy) == false;
        }

        static Predicate<String> forPolicies(String[] slmPolicies) {
            if (slmPolicies.length == 0) {
                return MATCH_ALL_POLICIES;
            }

            final List<String> includePatterns = new ArrayList<>(slmPolicies.length);
            final List<String> excludePatterns = new ArrayList<>(slmPolicies.length);
            boolean seenWildcard = false;
            boolean matchNoPolicy = false;
            for (final var slmPolicy : slmPolicies) {
                if (seenWildcard && slmPolicy.length() > 1 && slmPolicy.startsWith("-")) {
                    excludePatterns.add(slmPolicy.substring(1));
                } else {
                    if (Regex.isSimpleMatchPattern(slmPolicy)) {
                        seenWildcard = true;
                    } else if (GetSnapshotsRequest.NO_POLICY_PATTERN.equals(slmPolicy)) {
                        matchNoPolicy = true;
                    }
                    includePatterns.add(slmPolicy);
                }
            }

            return new SlmPolicyPredicate(
                includePatterns.toArray(Strings.EMPTY_ARRAY),
                excludePatterns.toArray(Strings.EMPTY_ARRAY),
                matchNoPolicy
            );
        }
    }
}
