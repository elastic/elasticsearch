/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            GetSnapshotsRequest::new, indexNameExpressionResolver);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSnapshotsResponse read(StreamInput in) throws IOException {
        return new GetSnapshotsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(Task task, final GetSnapshotsRequest request, final ClusterState state,
                                   final ActionListener<GetSnapshotsResponse> listener) {
        getMultipleReposSnapshotInfo(state.custom(SnapshotsInProgress.TYPE), TransportGetRepositoriesAction.getRepositories(
                state, request.repositories()), request.snapshots(), request.ignoreUnavailable(), request.verbose(), listener);
    }

    private void getMultipleReposSnapshotInfo(@Nullable SnapshotsInProgress snapshotsInProgress, List<RepositoryMetadata> repos,
                                              String[] snapshots, boolean ignoreUnavailable, boolean verbose,
                                              ActionListener<GetSnapshotsResponse> listener) {
        // short-circuit if there are no repos, because we can not create GroupedActionListener of size 0
        if (repos.isEmpty()) {
            listener.onResponse(new GetSnapshotsResponse(Collections.emptyList()));
            return;
        }
        final GroupedActionListener<GetSnapshotsResponse.Response> groupedActionListener =
                new GroupedActionListener<>(
                        ActionListener.map(listener, responses -> {
                            assert repos.size() == responses.size();
                            return new GetSnapshotsResponse(responses);
                        }), repos.size());

        // run concurrently for all repos on GENERIC thread pool
        for (final RepositoryMetadata repo : repos) {
            final String repoName = repo.name();
            threadPool.generic().execute(ActionRunnable.wrap(
                ActionListener.delegateResponse(groupedActionListener, (groupedListener, e) -> {
                    if (e instanceof ElasticsearchException) {
                        groupedListener.onResponse(GetSnapshotsResponse.Response.error(repoName, (ElasticsearchException) e));
                    } else {
                        groupedListener.onFailure(e);
                    }
                }), wrappedListener -> getSingleRepoSnapshotInfo(snapshotsInProgress, repoName, snapshots, ignoreUnavailable, verbose,
                    ActionListener.map(wrappedListener, snInfos -> GetSnapshotsResponse.Response.snapshots(repoName, snInfos)))));
        }
    }

    private void getSingleRepoSnapshotInfo(@Nullable SnapshotsInProgress snapshotsInProgress, String repo, String[] snapshots,
                                           boolean ignoreUnavailable, boolean verbose, ActionListener<List<SnapshotInfo>> listener) {
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

        repositoryDataListener.whenComplete(repositoryData -> listener.onResponse(loadSnapshotInfos(snapshotsInProgress, repo, snapshots,
            ignoreUnavailable, verbose, allSnapshotIds, currentSnapshots, repositoryData)),
            listener::onFailure);
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName repository name
     * @return list of snapshots
     */
    private static List<SnapshotInfo> sortedCurrentSnapshots(@Nullable SnapshotsInProgress snapshotsInProgress, String repositoryName) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries =
            SnapshotsService.currentSnapshots(snapshotsInProgress, repositoryName, Collections.emptyList());
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(new SnapshotInfo(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return unmodifiableList(snapshotList);
    }


    private List<SnapshotInfo> loadSnapshotInfos(@Nullable SnapshotsInProgress snapshotsInProgress, String repo, String[] snapshots,
                                                 boolean ignoreUnavailable, boolean verbose, Map<String, SnapshotId> allSnapshotIds,
                                                 List<SnapshotInfo> currentSnapshots, @Nullable RepositoryData repositoryData) {
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

        final List<SnapshotInfo> snapshotInfos;
        if (verbose) {
            snapshotInfos = snapshots(snapshotsInProgress, repo, new ArrayList<>(toResolve), ignoreUnavailable);
        } else {
            if (repositoryData != null) {
                // want non-current snapshots as well, which are found in the repository data
                snapshotInfos = buildSimpleSnapshotInfos(toResolve, repositoryData, currentSnapshots);
            } else {
                // only want current snapshots
                snapshotInfos = currentSnapshots.stream().map(SnapshotInfo::basic).collect(Collectors.toList());
                CollectionUtil.timSort(snapshotInfos);
            }
        }

        return snapshotInfos;
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName      repository name
     * @param snapshotIds         snapshots for which to fetch snapshot information
     * @param ignoreUnavailable   if true, snapshots that could not be read will only be logged with a warning,
     *                            if false, they will throw an error
     * @return list of snapshots
     */
    private List<SnapshotInfo> snapshots(@Nullable SnapshotsInProgress snapshotsInProgress, String repositoryName,
                                        List<SnapshotId> snapshotIds, boolean ignoreUnavailable) {
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress, repositoryName, snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList()));
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotSet.add(new SnapshotInfo(entry));
            snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId());
        }
        // then, look in the repository
        final Repository repository = repositoriesService.repository(repositoryName);
        for (SnapshotId snapshotId : snapshotIdsToIterate) {
            try {
                snapshotSet.add(repository.getSnapshotInfo(snapshotId));
            } catch (Exception ex) {
                if (ignoreUnavailable) {
                    logger.warn(() -> new ParameterizedMessage("failed to get snapshot [{}]", snapshotId), ex);
                } else {
                    if (ex instanceof SnapshotException) {
                        throw ex;
                    }
                    throw new SnapshotException(repositoryName, snapshotId, "Snapshot could not be read", ex);
                }
            }
        }
        final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotSet);
        CollectionUtil.timSort(snapshotList);
        return unmodifiableList(snapshotList);
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
            snapshotInfos.add(new SnapshotInfo(snapshotId, indices, repositoryData.getSnapshotState(snapshotId)));
        }
        CollectionUtil.timSort(snapshotInfos);
        return Collections.unmodifiableList(snapshotInfos);
    }
}
