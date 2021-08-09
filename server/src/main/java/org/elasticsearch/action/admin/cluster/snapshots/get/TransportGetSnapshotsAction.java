/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            ThreadPool.Names.SAME
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(GetSnapshotsRequest request, ClusterState state, ActionListener<GetSnapshotsResponse> listener)
        throws Exception {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetSnapshotsRequest request,
        final ClusterState state,
        final ActionListener<GetSnapshotsResponse> listener
    ) {
        assert task instanceof CancellableTask : task + " not cancellable";

        getMultipleReposSnapshotInfo(
            request.isSingleRepositoryRequest() == false,
            state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY),
            TransportGetRepositoriesAction.getRepositories(state, request.repositories()),
            request.snapshots(),
            request.ignoreUnavailable(),
            request.verbose(),
            (CancellableTask) task,
            request.sort(),
            request.after(),
            request.size(),
            request.order(),
            listener
        );
    }

    private void getMultipleReposSnapshotInfo(
        boolean isMultiRepoRequest,
        SnapshotsInProgress snapshotsInProgress,
        List<RepositoryMetadata> repos,
        String[] snapshots,
        boolean ignoreUnavailable,
        boolean verbose,
        CancellableTask cancellableTask,
        GetSnapshotsRequest.SortBy sortBy,
        @Nullable GetSnapshotsRequest.After after,
        int size,
        SortOrder order,
        ActionListener<GetSnapshotsResponse> listener
    ) {
        // short-circuit if there are no repos, because we can not create GroupedActionListener of size 0
        if (repos.isEmpty()) {
            listener.onResponse(new GetSnapshotsResponse(Collections.emptyList(), Collections.emptyMap(), null, 0, 0));
            return;
        }
        final GroupedActionListener<Tuple<Tuple<String, ElasticsearchException>, SnapshotsInRepo>> groupedActionListener =
            new GroupedActionListener<>(listener.map(responses -> {
                assert repos.size() == responses.size();
                final List<SnapshotInfo> allSnapshots = responses.stream()
                    .map(Tuple::v2)
                    .filter(Objects::nonNull)
                    .flatMap(snapshotsInRepo -> snapshotsInRepo.snapshotInfos.stream())
                    .collect(Collectors.toList());
                final Map<String, ElasticsearchException> failures = responses.stream()
                    .map(Tuple::v1)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
                final SnapshotsInRepo snInfos = sortSnapshots(allSnapshots, sortBy, after, size, order);
                final List<SnapshotInfo> snapshotInfos = snInfos.snapshotInfos;
                final int remaining = snInfos.remaining + responses.stream()
                    .map(Tuple::v2)
                    .filter(Objects::nonNull)
                    .mapToInt(s -> s.remaining)
                    .sum();
                return new GetSnapshotsResponse(
                    snapshotInfos,
                    failures,
                    remaining > 0
                        ? GetSnapshotsRequest.After.from(snapshotInfos.get(snapshotInfos.size() - 1), sortBy).asQueryParam()
                        : null,
                    responses.stream().map(Tuple::v2).filter(Objects::nonNull).mapToInt(s -> s.totalCount).sum(),
                    remaining
                );
            }), repos.size());

        for (final RepositoryMetadata repo : repos) {
            final String repoName = repo.name();
            getSingleRepoSnapshotInfo(
                snapshotsInProgress,
                repoName,
                snapshots,
                ignoreUnavailable,
                verbose,
                cancellableTask,
                sortBy,
                after,
                size,
                order,
                groupedActionListener.delegateResponse((groupedListener, e) -> {
                    if (isMultiRepoRequest && e instanceof ElasticsearchException) {
                        groupedListener.onResponse(Tuple.tuple(Tuple.tuple(repoName, (ElasticsearchException) e), null));
                    } else {
                        groupedListener.onFailure(e);
                    }
                }).map(snInfos -> Tuple.tuple(null, snInfos))
            );
        }
    }

    private void getSingleRepoSnapshotInfo(
        SnapshotsInProgress snapshotsInProgress,
        String repo,
        String[] snapshots,
        boolean ignoreUnavailable,
        boolean verbose,
        CancellableTask task,
        GetSnapshotsRequest.SortBy sortBy,
        @Nullable final GetSnapshotsRequest.After after,
        int size,
        SortOrder order,
        ActionListener<SnapshotsInRepo> listener
    ) {
        final Map<String, Snapshot> allSnapshotIds = new HashMap<>();
        final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : currentSnapshots(snapshotsInProgress, repo)) {
            Snapshot snapshot = snapshotInfo.snapshot();
            allSnapshotIds.put(snapshot.getSnapshotId().getName(), snapshot);
            currentSnapshots.add(snapshotInfo);
        }

        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        if (isCurrentSnapshotsOnly(snapshots)) {
            repositoryDataListener.onResponse(null);
        } else {
            repositoriesService.getRepositoryData(repo, repositoryDataListener);
        }

        repositoryDataListener.whenComplete(
            repositoryData -> loadSnapshotInfos(
                snapshotsInProgress,
                repo,
                snapshots,
                ignoreUnavailable,
                verbose,
                allSnapshotIds,
                currentSnapshots,
                repositoryData,
                task,
                sortBy,
                after,
                size,
                order,
                listener
            ),
            listener::onFailure
        );
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName repository name
     * @return list of snapshots
     */
    private static List<SnapshotInfo> currentSnapshots(SnapshotsInProgress snapshotsInProgress, String repositoryName) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            repositoryName,
            Collections.emptyList()
        );
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(new SnapshotInfo(entry));
        }
        return snapshotList;
    }

    private void loadSnapshotInfos(
        SnapshotsInProgress snapshotsInProgress,
        String repo,
        String[] snapshots,
        boolean ignoreUnavailable,
        boolean verbose,
        Map<String, Snapshot> allSnapshotIds,
        List<SnapshotInfo> currentSnapshots,
        @Nullable RepositoryData repositoryData,
        CancellableTask task,
        GetSnapshotsRequest.SortBy sortBy,
        @Nullable final GetSnapshotsRequest.After after,
        int size,
        SortOrder order,
        ActionListener<SnapshotsInRepo> listener
    ) {
        if (task.notifyIfCancelled(listener)) {
            return;
        }

        if (repositoryData != null) {
            for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                allSnapshotIds.put(snapshotId.getName(), new Snapshot(repo, snapshotId));
            }
        }

        final Set<Snapshot> toResolve = new HashSet<>();
        if (isAllSnapshots(snapshots)) {
            toResolve.addAll(allSnapshotIds.values());
        } else {
            for (String snapshotOrPattern : snapshots) {
                if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                    toResolve.addAll(currentSnapshots.stream().map(SnapshotInfo::snapshot).collect(Collectors.toList()));
                } else if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                    if (allSnapshotIds.containsKey(snapshotOrPattern)) {
                        toResolve.add(allSnapshotIds.get(snapshotOrPattern));
                    } else if (ignoreUnavailable == false) {
                        throw new SnapshotMissingException(repo, snapshotOrPattern);
                    }
                } else {
                    for (Map.Entry<String, Snapshot> entry : allSnapshotIds.entrySet()) {
                        if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                            toResolve.add(entry.getValue());
                        }
                    }
                }
            }

            if (toResolve.isEmpty() && ignoreUnavailable == false && isCurrentSnapshotsOnly(snapshots) == false) {
                throw new SnapshotMissingException(repo, snapshots[0]);
            }
        }

        if (verbose) {
            snapshots(
                snapshotsInProgress,
                repo,
                Collections.unmodifiableList(toResolve.stream().map(Snapshot::getSnapshotId).collect(Collectors.toList())),
                ignoreUnavailable,
                task,
                sortBy,
                after,
                size,
                order,
                listener
            );
        } else {
            final SnapshotsInRepo snapshotInfos;
            if (repositoryData != null) {
                // want non-current snapshots as well, which are found in the repository data
                snapshotInfos = buildSimpleSnapshotInfos(toResolve, repo, repositoryData, currentSnapshots, sortBy, after, size, order);
            } else {
                // only want current snapshots
                snapshotInfos = sortSnapshots(
                    currentSnapshots.stream().map(SnapshotInfo::basic).collect(Collectors.toList()),
                    sortBy,
                    after,
                    size,
                    order
                );
            }
            listener.onResponse(snapshotInfos);
        }
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *  @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName      repository name
     * @param snapshotIds         snapshots for which to fetch snapshot information
     * @param ignoreUnavailable   if true, snapshots that could not be read will only be logged with a warning,
     */
    private void snapshots(
        SnapshotsInProgress snapshotsInProgress,
        String repositoryName,
        Collection<SnapshotId> snapshotIds,
        boolean ignoreUnavailable,
        CancellableTask task,
        GetSnapshotsRequest.SortBy sortBy,
        @Nullable GetSnapshotsRequest.After after,
        int size,
        SortOrder order,
        ActionListener<SnapshotsInRepo> listener
    ) {
        if (task.notifyIfCancelled(listener)) {
            return;
        }
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            repositoryName,
            snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList())
        );
        for (SnapshotsInProgress.Entry entry : entries) {
            if (snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId())) {
                snapshotSet.add(new SnapshotInfo(entry));
            }
        }
        // then, look in the repository if there's any matching snapshots left
        final List<SnapshotInfo> snapshotInfos;
        if (snapshotIdsToIterate.isEmpty()) {
            snapshotInfos = Collections.emptyList();
        } else {
            snapshotInfos = Collections.synchronizedList(new ArrayList<>());
        }
        final ActionListener<Void> allDoneListener = listener.delegateFailure((l, v) -> {
            final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotInfos);
            snapshotList.addAll(snapshotSet);
            listener.onResponse(sortSnapshots(snapshotList, sortBy, after, size, order));
        });
        if (snapshotIdsToIterate.isEmpty()) {
            allDoneListener.onResponse(null);
            return;
        }
        final Repository repository;
        try {
            repository = repositoriesService.repository(repositoryName);
        } catch (RepositoryMissingException e) {
            listener.onFailure(e);
            return;
        }
        repository.getSnapshotInfo(
            new GetSnapshotInfoContext(
                snapshotIdsToIterate,
                ignoreUnavailable == false,
                task::isCancelled,
                (context, snapshotInfo) -> snapshotInfos.add(snapshotInfo),
                ignoreUnavailable ? ActionListener.runAfter(new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.trace("done fetching snapshot infos [{}]", snapshotIdsToIterate);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assert false : new AssertionError("listener should always complete successfully for ignoreUnavailable=true", e);
                        logger.warn("failed to fetch snapshot info for some snapshots", e);
                    }
                }, () -> allDoneListener.onResponse(null)) : allDoneListener
            )
        );
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshotsOnly(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }

    private static SnapshotsInRepo buildSimpleSnapshotInfos(
        final Set<Snapshot> toResolve,
        final String repoName,
        final RepositoryData repositoryData,
        final List<SnapshotInfo> currentSnapshots,
        final GetSnapshotsRequest.SortBy sortBy,
        @Nullable final GetSnapshotsRequest.After after,
        final int size,
        final SortOrder order
    ) {
        List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : currentSnapshots) {
            if (toResolve.remove(snapshotInfo.snapshot())) {
                snapshotInfos.add(snapshotInfo.basic());
            }
        }
        Map<SnapshotId, List<String>> snapshotsToIndices = new HashMap<>();
        for (IndexId indexId : repositoryData.getIndices().values()) {
            for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                if (toResolve.contains(new Snapshot(repoName, snapshotId))) {
                    snapshotsToIndices.computeIfAbsent(snapshotId, (k) -> new ArrayList<>()).add(indexId.getName());
                }
            }
        }
        for (Snapshot snapshot : toResolve) {
            final List<String> indices = snapshotsToIndices.getOrDefault(snapshot.getSnapshotId(), Collections.emptyList());
            CollectionUtil.timSort(indices);
            snapshotInfos.add(
                new SnapshotInfo(
                    snapshot,
                    indices,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    repositoryData.getSnapshotState(snapshot.getSnapshotId())
                )
            );
        }
        return sortSnapshots(snapshotInfos, sortBy, after, size, order);
    }

    private static final Comparator<SnapshotInfo> BY_START_TIME = Comparator.comparingLong(SnapshotInfo::startTime)
        .thenComparing(SnapshotInfo::snapshotId);

    private static final Comparator<SnapshotInfo> BY_DURATION = Comparator.<SnapshotInfo>comparingLong(
        sni -> sni.endTime() - sni.startTime()
    ).thenComparing(SnapshotInfo::snapshotId);

    private static final Comparator<SnapshotInfo> BY_INDICES_COUNT = Comparator.<SnapshotInfo>comparingInt(sni -> sni.indices().size())
        .thenComparing(SnapshotInfo::snapshotId);

    private static final Comparator<SnapshotInfo> BY_NAME = Comparator.comparing(sni -> sni.snapshotId().getName());

    private static SnapshotsInRepo sortSnapshots(
        final List<SnapshotInfo> snapshotInfos,
        final GetSnapshotsRequest.SortBy sortBy,
        final @Nullable GetSnapshotsRequest.After after,
        final int size,
        final SortOrder order
    ) {
        final Comparator<SnapshotInfo> comparator;
        switch (sortBy) {
            case START_TIME:
                comparator = BY_START_TIME;
                break;
            case NAME:
                comparator = BY_NAME;
                break;
            case DURATION:
                comparator = BY_DURATION;
                break;
            case INDICES:
                comparator = BY_INDICES_COUNT;
                break;
            default:
                throw new AssertionError("unexpected sort column [" + sortBy + "]");
        }

        Stream<SnapshotInfo> infos = snapshotInfos.stream();

        if (after != null) {
            final Predicate<SnapshotInfo> isAfter;
            final String snapshotName = after.snapshotName();
            final String repoName = after.repoName();
            switch (sortBy) {
                case START_TIME:
                    isAfter = filterByLongOffset(SnapshotInfo::startTime, Long.parseLong(after.value()), snapshotName, repoName, order);
                    break;
                case NAME:
                    isAfter = order == SortOrder.ASC
                        ? (info -> compareName(snapshotName, repoName, info) < 0)
                        : (info -> compareName(snapshotName, repoName, info) > 0);
                    break;
                case DURATION:
                    isAfter = filterByLongOffset(
                        info -> info.endTime() - info.startTime(),
                        Long.parseLong(after.value()),
                        snapshotName,
                        repoName,
                        order
                    );
                    break;
                case INDICES:
                    isAfter = filterByLongOffset(
                        info -> info.indices().size(),
                        Integer.parseInt(after.value()),
                        snapshotName,
                        repoName,
                        order
                    );
                    break;
                default:
                    throw new AssertionError("unexpected sort column [" + sortBy + "]");
            }
            infos = infos.filter(isAfter);
        }
        infos = infos.sorted(order == SortOrder.DESC ? comparator.reversed() : comparator);
        final List<SnapshotInfo> allSnapshots = infos.collect(Collectors.toList());
        final List<SnapshotInfo> snapshots;
        if (size != GetSnapshotsRequest.NO_LIMIT) {
            snapshots = Collections.unmodifiableList(allSnapshots.stream().limit(size + 1).collect(Collectors.toList()));
        } else {
            snapshots = allSnapshots;
        }
        final List<SnapshotInfo> resultSet = size != GetSnapshotsRequest.NO_LIMIT && size < snapshots.size()
            ? snapshots.subList(0, size)
            : snapshots;
        return new SnapshotsInRepo(resultSet, snapshotInfos.size(), allSnapshots.size() - resultSet.size());
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

    private static int compareName(String name, String repoName, SnapshotInfo info) {
        final int res = name.compareTo(info.snapshotId().getName());
        if (res != 0) {
            return res;
        }
        return repoName.compareTo(info.repository());
    }

    private static final class SnapshotsInRepo {

        private final List<SnapshotInfo> snapshotInfos;

        private final int totalCount;

        private final int remaining;

        SnapshotsInRepo(List<SnapshotInfo> snapshotInfos, int totalCount, int remaining) {
            this.snapshotInfos = snapshotInfos;
            this.totalCount = totalCount;
            this.remaining = remaining;
        }
    }
}
