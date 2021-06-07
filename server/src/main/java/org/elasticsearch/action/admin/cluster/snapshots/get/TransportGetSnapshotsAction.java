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
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotException;
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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetSnapshotsAction.class);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetSnapshotsAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, RepositoriesService repositoriesService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetSnapshotsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetSnapshotsRequest::new, indexNameExpressionResolver, GetSnapshotsResponse::new, ThreadPool.Names.SAME);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(Task task, final GetSnapshotsRequest request, final ClusterState state,
                                   final ActionListener<GetSnapshotsResponse> listener) {
        assert task instanceof CancellableTask : task + " not cancellable";

        getMultipleReposSnapshotInfo(
                state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY),
                TransportGetRepositoriesAction.getRepositories(state, request.repositories()),
                request.snapshots(),
                request.ignoreUnavailable(),
                request.verbose(),
                (CancellableTask) task,
                listener
        );
    }

    private void getMultipleReposSnapshotInfo(SnapshotsInProgress snapshotsInProgress,
                                              List<RepositoryMetadata> repos,
                                              String[] snapshots,
                                              boolean ignoreUnavailable,
                                              boolean verbose,
                                              CancellableTask cancellableTask,
                                              ActionListener<GetSnapshotsResponse> listener) {
        // short-circuit if there are no repos, because we can not create GroupedActionListener of size 0
        if (repos.isEmpty()) {
            listener.onResponse(new GetSnapshotsResponse(Collections.emptyList()));
            return;
        }
        final GroupedActionListener<GetSnapshotsResponse.Response> groupedActionListener =
                new GroupedActionListener<>(
                        listener.map(responses -> {
                            assert repos.size() == responses.size();
                            return new GetSnapshotsResponse(responses);
                        }), repos.size());

        for (final RepositoryMetadata repo : repos) {
            final String repoName = repo.name();
            getSingleRepoSnapshotInfo(snapshotsInProgress, repoName, snapshots, ignoreUnavailable, verbose, cancellableTask,
                    groupedActionListener.delegateResponse((groupedListener, e) -> {
                        if (e instanceof ElasticsearchException) {
                            groupedListener.onResponse(GetSnapshotsResponse.Response.error(repoName, (ElasticsearchException) e));
                        } else {
                            groupedListener.onFailure(e);
                        }
                    }).map(snInfos -> GetSnapshotsResponse.Response.snapshots(repoName, snInfos)));
        }
    }

    private void getSingleRepoSnapshotInfo(SnapshotsInProgress snapshotsInProgress, String repo, String[] snapshots,
                                           boolean ignoreUnavailable, boolean verbose, CancellableTask task,
                                           ActionListener<List<SnapshotInfo>> listener) {
        final Map<String, SnapshotId> allSnapshotIds = new HashMap<>();
        final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : sortedCurrentSnapshots(snapshotsInProgress, repo)) {
            SnapshotId snapshotId = snapshotInfo.snapshotId();
            allSnapshotIds.put(snapshotId.getName(), snapshotId);
            currentSnapshots.add(snapshotInfo);
        }

        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        if (isCurrentSnapshotsOnly(snapshots)) {
            repositoryDataListener.onResponse(null);
        } else {
            repositoriesService.getRepositoryData(repo, repositoryDataListener);
        }

        repositoryDataListener.whenComplete(repositoryData -> loadSnapshotInfos(snapshotsInProgress, repo, snapshots,
                ignoreUnavailable, verbose, allSnapshotIds, currentSnapshots, repositoryData, task, listener), listener::onFailure);
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName repository name
     * @return list of snapshots
     */
    private static List<SnapshotInfo> sortedCurrentSnapshots(SnapshotsInProgress snapshotsInProgress, String repositoryName) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries =
            SnapshotsService.currentSnapshots(snapshotsInProgress, repositoryName, Collections.emptyList());
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(new SnapshotInfo(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return unmodifiableList(snapshotList);
    }


    private void loadSnapshotInfos(SnapshotsInProgress snapshotsInProgress, String repo, String[] snapshots,
                                   boolean ignoreUnavailable, boolean verbose, Map<String, SnapshotId> allSnapshotIds,
                                   List<SnapshotInfo> currentSnapshots, @Nullable RepositoryData repositoryData,
                                   CancellableTask task, ActionListener<List<SnapshotInfo>> listener) {
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return;
        }

        if (repositoryData != null) {
            for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                allSnapshotIds.put(snapshotId.getName(), snapshotId);
            }
        }

        final Set<SnapshotId> toResolve = new HashSet<>();
        if (isAllSnapshots(snapshots)) {
            toResolve.addAll(allSnapshotIds.values());
        } else {
            for (String snapshotOrPattern : snapshots) {
                if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                    toResolve.addAll(currentSnapshots.stream().map(SnapshotInfo::snapshotId).collect(Collectors.toList()));
                } else if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                    if (allSnapshotIds.containsKey(snapshotOrPattern)) {
                        toResolve.add(allSnapshotIds.get(snapshotOrPattern));
                    } else if (ignoreUnavailable == false) {
                        throw new SnapshotMissingException(repo, snapshotOrPattern);
                    }
                } else {
                    for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
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
            snapshots(snapshotsInProgress, repo, toResolve, ignoreUnavailable, task, listener);
        } else {
            final List<SnapshotInfo> snapshotInfos;
            if (repositoryData != null) {
                // want non-current snapshots as well, which are found in the repository data
                snapshotInfos = buildSimpleSnapshotInfos(toResolve, repositoryData, currentSnapshots);
            } else {
                // only want current snapshots
                snapshotInfos = currentSnapshots.stream().map(SnapshotInfo::basic).collect(Collectors.toList());
                CollectionUtil.timSort(snapshotInfos);
            }
            listener.onResponse(snapshotInfos);
        }
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName      repository name
     * @param snapshotIds         snapshots for which to fetch snapshot information
     * @param ignoreUnavailable   if true, snapshots that could not be read will only be logged with a warning,
     *                            if false, they will throw an error
     */
    private void snapshots(SnapshotsInProgress snapshotsInProgress,
                           String repositoryName,
                           Collection<SnapshotId> snapshotIds,
                           boolean ignoreUnavailable,
                           CancellableTask task,
                           ActionListener<List<SnapshotInfo>> listener) {
        if (task.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return;
        }
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress, repositoryName, snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList()));
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
        final ActionListener<Collection<Void>> allDoneListener = listener.delegateFailure((l, v) -> {
            final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotInfos);
            snapshotList.addAll(snapshotSet);
            CollectionUtil.timSort(snapshotList);
            listener.onResponse(unmodifiableList(snapshotList));
        });
        if (snapshotIdsToIterate.isEmpty()) {
            allDoneListener.onResponse(Collections.emptyList());
            return;
        }
        // put snapshot info downloads into a task queue instead of pushing them all into the queue to not completely monopolize the
        // snapshot meta pool for a single request
        final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT_META).getMax(), snapshotIdsToIterate.size());
        final BlockingQueue<SnapshotId> queue = new LinkedBlockingQueue<>(snapshotIdsToIterate);
        final ActionListener<Void> workerDoneListener = new GroupedActionListener<>(allDoneListener, workers).delegateResponse((l, e) -> {
            queue.clear(); // Stop fetching the remaining snapshots once we've failed fetching one since the response is an error response
                           // anyway in this case
            l.onFailure(e);
        });
        final Repository repository;
        try {
            repository = repositoriesService.repository(repositoryName);
        } catch (RepositoryMissingException e) {
            listener.onFailure(e);
            return;
        }
        for (int i = 0; i < workers; i++) {
            getOneSnapshotInfo(
                    ignoreUnavailable,
                    repository,
                    queue,
                    snapshotInfos,
                    task,
                    workerDoneListener
            );
        }
    }

    /**
     * Tries to poll a {@link SnapshotId} to load {@link SnapshotInfo} for from the given {@code queue}. If it finds one in the queue,
     * loads the snapshot info from the repository and adds it to the given {@code snapshotInfos} collection, then invokes itself again to
     * try and poll another task from the queue.
     * If the queue is empty resolves {@code} listener.
     */
    private void getOneSnapshotInfo(boolean ignoreUnavailable,
                                    Repository repository,
                                    BlockingQueue<SnapshotId> queue,
                                    Collection<SnapshotInfo> snapshotInfos,
                                    CancellableTask task,
                                    ActionListener<Void> listener) {
        final SnapshotId snapshotId = queue.poll();
        if (snapshotId == null) {
            listener.onResponse(null);
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT_META).execute(() -> {
            if (task.isCancelled()) {
                listener.onFailure(new TaskCancelledException("task cancelled"));
                return;
            }
            try {
                snapshotInfos.add(repository.getSnapshotInfo(snapshotId));
            } catch (Exception ex) {
                if (ignoreUnavailable) {
                    logger.warn(() -> new ParameterizedMessage("failed to get snapshot [{}]", snapshotId), ex);
                } else {
                    listener.onFailure(
                            ex instanceof SnapshotException
                                    ? ex
                                    : new SnapshotException(repository.getMetadata().name(), snapshotId, "Snapshot could not be read", ex)
                    );
                }
            }
            getOneSnapshotInfo(ignoreUnavailable, repository, queue, snapshotInfos, task, listener);
        });
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshotsOnly(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }

    private static List<SnapshotInfo> buildSimpleSnapshotInfos(final Set<SnapshotId> toResolve,
                                                        final RepositoryData repositoryData,
                                                        final List<SnapshotInfo> currentSnapshots) {
        List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : currentSnapshots) {
            if (toResolve.remove(snapshotInfo.snapshotId())) {
                snapshotInfos.add(snapshotInfo.basic());
            }
        }
        Map<SnapshotId, List<String>> snapshotsToIndices = new HashMap<>();
        for (IndexId indexId : repositoryData.getIndices().values()) {
            for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                if (toResolve.contains(snapshotId)) {
                    snapshotsToIndices.computeIfAbsent(snapshotId, (k) -> new ArrayList<>())
                            .add(indexId.getName());
                }
            }
        }
        for (SnapshotId snapshotId : toResolve) {
            final List<String> indices = snapshotsToIndices.getOrDefault(snapshotId, Collections.emptyList());
            CollectionUtil.timSort(indices);
            snapshotInfos.add(new SnapshotInfo(snapshotId, indices, Collections.emptyList(), Collections.emptyList(),
                repositoryData.getSnapshotState(snapshotId)));
        }
        CollectionUtil.timSort(snapshotInfos);
        return Collections.unmodifiableList(snapshotInfos);
    }
}
