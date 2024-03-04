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
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {

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
            SnapshotPredicates.fromRequest(request),
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
        private final String[] snapshots;
        private final boolean ignoreUnavailable;
        private final SnapshotPredicates predicates;

        // snapshot ordering/pagination
        private final GetSnapshotsRequest.SortBy sortBy;
        private final SortOrder order;
        @Nullable
        private final String fromSortValue;
        private final int offset;
        @Nullable
        private final GetSnapshotsRequest.After after;
        private final int size;

        // current state
        private final SnapshotsInProgress snapshotsInProgress;

        // output detail
        private final boolean verbose;
        private final boolean indices;

        // results
        private final Map<String, ElasticsearchException> failuresByRepository = ConcurrentCollections.newConcurrentMap();
        private final Queue<List<SnapshotInfo>> allSnapshotInfos = ConcurrentCollections.newQueue();
        private final AtomicInteger remaining = new AtomicInteger();
        private final AtomicInteger totalCount = new AtomicInteger();

        GetSnapshotsOperation(
            CancellableTask cancellableTask,
            ResolvedRepositories resolvedRepositories,
            boolean isMultiRepoRequest,
            String[] snapshots,
            boolean ignoreUnavailable,
            SnapshotPredicates predicates,
            GetSnapshotsRequest.SortBy sortBy,
            SortOrder order,
            String fromSortValue,
            int offset,
            GetSnapshotsRequest.After after,
            int size,
            SnapshotsInProgress snapshotsInProgress,
            boolean verbose,
            boolean indices
        ) {
            this.cancellableTask = cancellableTask;
            this.repositories = resolvedRepositories.repositoryMetadata();
            this.isMultiRepoRequest = isMultiRepoRequest;
            this.snapshots = snapshots;
            this.ignoreUnavailable = ignoreUnavailable;
            this.predicates = predicates;
            this.sortBy = sortBy;
            this.order = order;
            this.fromSortValue = fromSortValue;
            this.offset = offset;
            this.after = after;
            this.size = size;
            this.snapshotsInProgress = snapshotsInProgress;
            this.verbose = verbose;
            this.indices = indices;

            for (final var missingRepo : resolvedRepositories.missing()) {
                failuresByRepository.put(missingRepo, new RepositoryMissingException(missingRepo));
            }
        }

        /**
         * Filters the list of repositories that a request will fetch snapshots from in the special case of sorting by repository
         * name and having a non-null value for {@link GetSnapshotsRequest#fromSortValue()} on the request to exclude repositories outside
         * the sort value range if possible.
         */
        private List<RepositoryMetadata> maybeFilterRepositories() {
            if (sortBy != GetSnapshotsRequest.SortBy.REPOSITORY || fromSortValue == null) {
                return repositories;
            }
            final Predicate<RepositoryMetadata> predicate = order == SortOrder.ASC
                ? repositoryMetadata -> fromSortValue.compareTo(repositoryMetadata.name()) <= 0
                : repositoryMetadata -> fromSortValue.compareTo(repositoryMetadata.name()) >= 0;
            return repositories.stream().filter(predicate).toList();
        }

        void getMultipleReposSnapshotInfo(ActionListener<GetSnapshotsResponse> listener) {
            List<RepositoryMetadata> filteredRepositories = maybeFilterRepositories();
            try (var listeners = new RefCountingListener(listener.map(ignored -> {
                cancellableTask.ensureNotCancelled();
                final var sortedSnapshotsInRepos = sortSnapshots(
                    allSnapshotInfos.stream().flatMap(Collection::stream),
                    totalCount.get(),
                    offset,
                    size
                );
                final var snapshotInfos = sortedSnapshotsInRepos.snapshotInfos();
                assert indices || snapshotInfos.stream().allMatch(snapshotInfo -> snapshotInfo.indices().isEmpty());
                final int finalRemaining = sortedSnapshotsInRepos.remaining() + remaining.get();
                return new GetSnapshotsResponse(
                    snapshotInfos,
                    failuresByRepository,
                    finalRemaining > 0
                        ? GetSnapshotsRequest.After.from(snapshotInfos.get(snapshotInfos.size() - 1), sortBy).asQueryParam()
                        : null,
                    totalCount.get(),
                    finalRemaining
                );
            }))) {
                for (final RepositoryMetadata repository : filteredRepositories) {
                    final String repoName = repository.name();
                    getSingleRepoSnapshotInfo(repoName, listeners.acquire((SnapshotsInRepo snapshotsInRepo) -> {
                        allSnapshotInfos.add(snapshotsInRepo.snapshotInfos());
                        remaining.addAndGet(snapshotsInRepo.remaining());
                        totalCount.addAndGet(snapshotsInRepo.totalCount());
                    }).delegateResponse((l, e) -> {
                        if (isMultiRepoRequest && e instanceof ElasticsearchException elasticsearchException) {
                            failuresByRepository.put(repoName, elasticsearchException);
                            l.onResponse(SnapshotsInRepo.EMPTY);
                        } else {
                            l.onFailure(e);
                        }
                    }));
                }
            }
        }

        private void getSingleRepoSnapshotInfo(String repo, ActionListener<SnapshotsInRepo> listener) {
            final Map<String, Snapshot> allSnapshotIds = new HashMap<>();
            final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
            for (final SnapshotInfo snapshotInfo : currentSnapshots(repo)) {
                Snapshot snapshot = snapshotInfo.snapshot();
                allSnapshotIds.put(snapshot.getSnapshotId().getName(), snapshot);
                currentSnapshots.add(snapshotInfo.maybeWithoutIndices(indices));
            }

            final ListenableFuture<RepositoryData> repositoryDataListener = new ListenableFuture<>();
            if (isCurrentSnapshotsOnly()) {
                repositoryDataListener.onResponse(null);
            } else {
                repositoriesService.getRepositoryData(repo, repositoryDataListener);
            }

            repositoryDataListener.addListener(
                listener.delegateFailureAndWrap(
                    (l, repositoryData) -> loadSnapshotInfos(repo, allSnapshotIds, currentSnapshots, repositoryData, l)
                )
            );
        }

        /**
         * Returns a list of currently running snapshots from repository sorted by snapshot creation date
         *
         * @param repositoryName      repository name
         * @return list of snapshots
         */
        private List<SnapshotInfo> currentSnapshots(String repositoryName) {
            List<SnapshotInfo> snapshotList = new ArrayList<>();
            List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
                snapshotsInProgress,
                repositoryName,
                Collections.emptyList()
            );
            for (SnapshotsInProgress.Entry entry : entries) {
                snapshotList.add(SnapshotInfo.inProgress(entry));
            }
            return snapshotList;
        }

        private void loadSnapshotInfos(
            String repo,
            Map<String, Snapshot> allSnapshotIds,
            List<SnapshotInfo> currentSnapshots,
            @Nullable RepositoryData repositoryData,
            ActionListener<SnapshotsInRepo> listener
        ) {
            if (cancellableTask.notifyIfCancelled(listener)) {
                return;
            }

            if (repositoryData != null) {
                for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                    if (predicates.test(snapshotId, repositoryData)) {
                        allSnapshotIds.put(snapshotId.getName(), new Snapshot(repo, snapshotId));
                    }
                }
            }

            final Set<Snapshot> toResolve = new HashSet<>();
            if (ResolvedRepositories.isMatchAll(snapshots)) {
                toResolve.addAll(allSnapshotIds.values());
            } else {
                final List<String> includePatterns = new ArrayList<>();
                final List<String> excludePatterns = new ArrayList<>();
                boolean hasCurrent = false;
                boolean seenWildcard = false;
                for (String snapshotOrPattern : snapshots) {
                    if (seenWildcard && snapshotOrPattern.length() > 1 && snapshotOrPattern.startsWith("-")) {
                        excludePatterns.add(snapshotOrPattern.substring(1));
                    } else {
                        if (Regex.isSimpleMatchPattern(snapshotOrPattern)) {
                            seenWildcard = true;
                            includePatterns.add(snapshotOrPattern);
                        } else if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                            hasCurrent = true;
                            seenWildcard = true;
                        } else {
                            if (ignoreUnavailable == false && allSnapshotIds.containsKey(snapshotOrPattern) == false) {
                                throw new SnapshotMissingException(repo, snapshotOrPattern);
                            }
                            includePatterns.add(snapshotOrPattern);
                        }
                    }
                }
                final String[] includes = includePatterns.toArray(Strings.EMPTY_ARRAY);
                final String[] excludes = excludePatterns.toArray(Strings.EMPTY_ARRAY);
                for (Map.Entry<String, Snapshot> entry : allSnapshotIds.entrySet()) {
                    final Snapshot snapshot = entry.getValue();
                    if (toResolve.contains(snapshot) == false
                        && Regex.simpleMatch(includes, entry.getKey())
                        && Regex.simpleMatch(excludes, entry.getKey()) == false) {
                        toResolve.add(snapshot);
                    }
                }
                if (hasCurrent) {
                    for (SnapshotInfo snapshotInfo : currentSnapshots) {
                        final Snapshot snapshot = snapshotInfo.snapshot();
                        if (Regex.simpleMatch(excludes, snapshot.getSnapshotId().getName()) == false) {
                            toResolve.add(snapshot);
                        }
                    }
                }
                if (toResolve.isEmpty() && ignoreUnavailable == false && isCurrentSnapshotsOnly() == false) {
                    throw new SnapshotMissingException(repo, snapshots[0]);
                }
            }

            if (verbose) {
                snapshots(repo, toResolve.stream().map(Snapshot::getSnapshotId).toList(), listener);
            } else {
                assert predicates.isMatchAll() : "filtering is not supported in non-verbose mode";
                final SnapshotsInRepo snapshotInfos;
                if (repositoryData != null) {
                    // want non-current snapshots as well, which are found in the repository data
                    snapshotInfos = buildSimpleSnapshotInfos(toResolve, repo, repositoryData, currentSnapshots);
                } else {
                    // only want current snapshots
                    snapshotInfos = sortSnapshotsWithNoOffsetOrLimit(currentSnapshots.stream().map(SnapshotInfo::basic).toList());
                }
                listener.onResponse(snapshotInfos);
            }
        }

        /**
         * Returns a list of snapshots from repository sorted by snapshot creation date
         *
         * @param repositoryName repository name
         * @param snapshotIds    snapshots for which to fetch snapshot information
         */
        private void snapshots(String repositoryName, Collection<SnapshotId> snapshotIds, ActionListener<SnapshotsInRepo> listener) {
            if (cancellableTask.notifyIfCancelled(listener)) {
                return;
            }
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
                    if (predicates.test(snapshotInfo)) {
                        snapshots.add(snapshotInfo.maybeWithoutIndices(indices));
                    }
                }
            }
            // then, look in the repository if there's any matching snapshots left
            try (
                var listeners = new RefCountingListener(
                    // no need to synchronize access to snapshots: Repository#getSnapshotInfo fails fast but we're on the success path here
                    listener.safeMap(v -> sortSnapshotsWithNoOffsetOrLimit(snapshots))
                )
            ) {
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

                repository.getSnapshotInfo(snapshotIdsToIterate, ignoreUnavailable == false, cancellableTask::isCancelled, snapshotInfo -> {
                    if (predicates.test(snapshotInfo)) {
                        syncSnapshots.add(snapshotInfo.maybeWithoutIndices(indices));
                    }
                }, listeners.acquire());
            }
        }

        private boolean isCurrentSnapshotsOnly() {
            return snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]);
        }

        private SnapshotsInRepo buildSimpleSnapshotInfos(
            final Set<Snapshot> toResolve,
            final String repoName,
            final RepositoryData repositoryData,
            final List<SnapshotInfo> currentSnapshots
        ) {
            List<SnapshotInfo> snapshotInfos = new ArrayList<>();
            for (SnapshotInfo snapshotInfo : currentSnapshots) {
                if (toResolve.remove(snapshotInfo.snapshot())) {
                    snapshotInfos.add(snapshotInfo.basic());
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
                snapshotInfos.add(
                    new SnapshotInfo(
                        snapshot,
                        snapshotsToIndices.getOrDefault(snapshot.getSnapshotId(), Collections.emptyList()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        repositoryData.getSnapshotState(snapshot.getSnapshotId())
                    )
                );
            }
            return sortSnapshotsWithNoOffsetOrLimit(snapshotInfos);
        }

        private static final Comparator<SnapshotInfo> BY_START_TIME = Comparator.comparingLong(SnapshotInfo::startTime)
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_DURATION = Comparator.<SnapshotInfo>comparingLong(
            sni -> sni.endTime() - sni.startTime()
        ).thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_INDICES_COUNT = Comparator.<SnapshotInfo>comparingInt(sni -> sni.indices().size())
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_SHARDS_COUNT = Comparator.comparingInt(SnapshotInfo::totalShards)
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_FAILED_SHARDS_COUNT = Comparator.comparingInt(SnapshotInfo::failedShards)
            .thenComparing(SnapshotInfo::snapshotId);

        private static final Comparator<SnapshotInfo> BY_NAME = Comparator.comparing(sni -> sni.snapshotId().getName());

        private static final Comparator<SnapshotInfo> BY_REPOSITORY = Comparator.comparing(SnapshotInfo::repository)
            .thenComparing(SnapshotInfo::snapshotId);

        private SnapshotsInRepo sortSnapshotsWithNoOffsetOrLimit(List<SnapshotInfo> snapshotInfos) {
            return sortSnapshots(snapshotInfos.stream(), snapshotInfos.size(), 0, GetSnapshotsRequest.NO_LIMIT);
        }

        private SnapshotsInRepo sortSnapshots(Stream<SnapshotInfo> snapshotInfoStream, int totalCount, int offset, int size) {
            final var resultsStream = snapshotInfoStream.filter(buildAfterPredicate()).sorted(buildComparator()).skip(offset);
            if (size == GetSnapshotsRequest.NO_LIMIT) {
                return new SnapshotsInRepo(resultsStream.toList(), totalCount, 0);
            } else {
                final var allocateSize = Math.min(size, 1000); // ignore excessively-large sizes in request params
                final var results = new ArrayList<SnapshotInfo>(allocateSize);
                var remaining = 0;
                for (var iterator = resultsStream.iterator(); iterator.hasNext();) {
                    final var snapshotInfo = iterator.next();
                    if (results.size() < size) {
                        results.add(snapshotInfo);
                    } else {
                        remaining += 1;
                    }
                }
                return new SnapshotsInRepo(results, totalCount, remaining);
            }
        }

        private Comparator<SnapshotInfo> buildComparator() {
            final Comparator<SnapshotInfo> comparator = switch (sortBy) {
                case START_TIME -> BY_START_TIME;
                case NAME -> BY_NAME;
                case DURATION -> BY_DURATION;
                case INDICES -> BY_INDICES_COUNT;
                case SHARDS -> BY_SHARDS_COUNT;
                case FAILED_SHARDS -> BY_FAILED_SHARDS_COUNT;
                case REPOSITORY -> BY_REPOSITORY;
            };
            return order == SortOrder.DESC ? comparator.reversed() : comparator;
        }

        private Predicate<SnapshotInfo> buildAfterPredicate() {
            if (after == null) {
                // TODO use constant when https://github.com/elastic/elasticsearch/pull/105881 merged
                return snapshotInfo -> true;
            }
            assert offset == 0 : "can't combine after and offset but saw [" + after + "] and offset [" + offset + "]";

            final String snapshotName = after.snapshotName();
            final String repoName = after.repoName();
            final String value = after.value();
            return switch (sortBy) {
                case START_TIME -> filterByLongOffset(SnapshotInfo::startTime, Long.parseLong(value), snapshotName, repoName, order);
                case NAME ->
                    // TODO: cover via pre-flight predicate
                    order == SortOrder.ASC
                        ? (info -> compareName(snapshotName, repoName, info) < 0)
                        : (info -> compareName(snapshotName, repoName, info) > 0);
                case DURATION -> filterByLongOffset(
                    info -> info.endTime() - info.startTime(),
                    Long.parseLong(value),
                    snapshotName,
                    repoName,
                    order
                );
                case INDICES ->
                    // TODO: cover via pre-flight predicate
                    filterByLongOffset(info -> info.indices().size(), Integer.parseInt(value), snapshotName, repoName, order);
                case SHARDS -> filterByLongOffset(SnapshotInfo::totalShards, Integer.parseInt(value), snapshotName, repoName, order);
                case FAILED_SHARDS -> filterByLongOffset(
                    SnapshotInfo::failedShards,
                    Integer.parseInt(value),
                    snapshotName,
                    repoName,
                    order
                );
                case REPOSITORY ->
                    // TODO: cover via pre-flight predicate
                    order == SortOrder.ASC
                        ? (info -> compareRepositoryName(snapshotName, repoName, info) < 0)
                        : (info -> compareRepositoryName(snapshotName, repoName, info) > 0);
            };
        }

        private static Predicate<SnapshotInfo> filterByLongOffset(
            ToLongFunction<SnapshotInfo> extractor,
            long after,
            String snapshotName,
            String repoName,
            SortOrder order
        ) {
            return order == SortOrder.ASC ? info -> {
                final long val = extractor.applyAsLong(info);
                return after < val || (after == val && compareName(snapshotName, repoName, info) < 0);
            } : info -> {
                final long val = extractor.applyAsLong(info);
                return after > val || (after == val && compareName(snapshotName, repoName, info) > 0);
            };
        }

        private static int compareRepositoryName(String name, String repoName, SnapshotInfo info) {
            final int res = repoName.compareTo(info.repository());
            if (res != 0) {
                return res;
            }
            return name.compareTo(info.snapshotId().getName());
        }

        private static int compareName(String name, String repoName, SnapshotInfo info) {
            final int res = name.compareTo(info.snapshotId().getName());
            if (res != 0) {
                return res;
            }
            return repoName.compareTo(info.repository());
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

        private SnapshotPredicates and(SnapshotPredicates other) {
            return this == MATCH_ALL ? other
                : other == MATCH_ALL ? this
                : new SnapshotPredicates(
                    preflightPredicate == null ? other.preflightPredicate : other.preflightPredicate == null ? preflightPredicate : null,
                    snapshotPredicate == null ? other.snapshotPredicate : other.snapshotPredicate == null ? snapshotPredicate : null
                );
        }

        static SnapshotPredicates fromRequest(GetSnapshotsRequest request) {
            return getSortValuePredicate(request.fromSortValue(), request.sort(), request.order()).and(
                getSlmPredicates(request.policies())
            );
        }

        private static SnapshotPredicates getSlmPredicates(String[] slmPolicies) {
            if (slmPolicies.length == 0) {
                return MATCH_ALL;
            }

            final List<String> includePatterns = new ArrayList<>();
            final List<String> excludePatterns = new ArrayList<>();
            boolean seenWildcard = false;
            boolean matchNoPolicy = false;
            for (String slmPolicy : slmPolicies) {
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
            final String[] includes = includePatterns.toArray(Strings.EMPTY_ARRAY);
            final String[] excludes = excludePatterns.toArray(Strings.EMPTY_ARRAY);
            final boolean matchWithoutPolicy = matchNoPolicy;
            return new SnapshotPredicates(((snapshotId, repositoryData) -> {
                final RepositoryData.SnapshotDetails details = repositoryData.getSnapshotDetails(snapshotId);
                final String policy;
                if (details == null || (details.getSlmPolicy() == null)) {
                    // no SLM policy recorded
                    return true;
                } else {
                    final String policyFound = details.getSlmPolicy();
                    // empty string means that snapshot was not created by an SLM policy
                    policy = policyFound.isEmpty() ? null : policyFound;
                }
                return matchPolicy(includes, excludes, matchWithoutPolicy, policy);
            }), snapshotInfo -> {
                final Map<String, Object> metadata = snapshotInfo.userMetadata();
                final String policy;
                if (metadata == null) {
                    policy = null;
                } else {
                    final Object policyFound = metadata.get(SnapshotsService.POLICY_ID_METADATA_FIELD);
                    policy = policyFound instanceof String ? (String) policyFound : null;
                }
                return matchPolicy(includes, excludes, matchWithoutPolicy, policy);
            });
        }

        private static boolean matchPolicy(String[] includes, String[] excludes, boolean matchWithoutPolicy, @Nullable String policy) {
            if (policy == null) {
                return matchWithoutPolicy;
            }
            if (Regex.simpleMatch(includes, policy) == false) {
                return false;
            }
            return excludes.length == 0 || Regex.simpleMatch(excludes, policy) == false;
        }

        private static SnapshotPredicates getSortValuePredicate(String fromSortValue, GetSnapshotsRequest.SortBy sortBy, SortOrder order) {
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

    private record SnapshotsInRepo(List<SnapshotInfo> snapshotInfos, int totalCount, int remaining) {
        private static final SnapshotsInRepo EMPTY = new SnapshotsInRepo(List.of(), 0, 0);
    }
}
