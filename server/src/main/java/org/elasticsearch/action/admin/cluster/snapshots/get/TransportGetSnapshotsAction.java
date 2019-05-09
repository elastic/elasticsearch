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

import org.apache.logging.log4j.core.util.Throwables;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportGetSnapshotsAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, SnapshotsService snapshotsService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetSnapshotsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetSnapshotsRequest::new, indexNameExpressionResolver);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected GetSnapshotsResponse newResponse() {
        return new GetSnapshotsResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(final GetSnapshotsRequest request, final ClusterState state,
                                   final ActionListener<GetSnapshotsResponse> listener) {
        final String[] repositories = request.repositories();
        transportService.sendRequest(transportService.getLocalNode(), GetRepositoriesAction.NAME,
                new GetRepositoriesRequest(repositories),
                new ActionListenerResponseHandler<>(
                        ActionListener.wrap(
                                response ->
                                    // switch to GENERIC thread pool because it might be long running operation
                                    threadPool.executor(ThreadPool.Names.GENERIC).execute(
                                            () -> getMultipleReposSnapshotInfo(response.repositories(), request.snapshots(),
                                                    request.ignoreUnavailable(), request.verbose(), listener)),
                                listener::onFailure),
                        GetRepositoriesResponse::new));
    }

    private void getMultipleReposSnapshotInfo(List<RepositoryMetaData> repos, String[] snapshots, boolean ignoreUnavailable,
                                              boolean verbose, ActionListener<GetSnapshotsResponse> listener) {
        List<Future<List<SnapshotInfo>>> futures = new ArrayList<>(repos.size());

        // run concurrently for all repos on SNAPSHOT thread pool
        for (final RepositoryMetaData repo : repos) {
            futures.add(threadPool.executor(ThreadPool.Names.SNAPSHOT).submit(
                    () -> getSingleRepoSnapshotInfo(repo.name(), snapshots, ignoreUnavailable, verbose)));
        }
        List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        List<GetSnapshotResponseFailureItem> failures = new ArrayList<>();
        assert repos.size() == futures.size();

        for (int i = 0; i < repos.size(); i++) {
            try {
                snapshotInfos.addAll(futures.get(i).get());
            } catch (InterruptedException e) {
                Throwables.rethrow(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ElasticsearchException) {
                    failures.add(new GetSnapshotResponseFailureItem(repos.get(i).name(), (ElasticsearchException) e.getCause()));
                } else {
                    Throwables.rethrow(e);
                }
            }
        }
        CollectionUtil.timSort(snapshotInfos);
        CollectionUtil.timSort(failures);

        // We need to preserve pre 7.2 BwC - if there is a single repo and it has failed, fail the whole request
        if (snapshotInfos.size() == 0 && failures.size() == 1) {
            listener.onFailure(failures.get(0).getError());
        } else {
            listener.onResponse(new GetSnapshotsResponse(snapshotInfos, failures));
        }
    }

    private List<SnapshotInfo> getSingleRepoSnapshotInfo(String repo, String[] snapshots, boolean ignoreUnavailable, boolean verbose) {
        final Map<String, SnapshotId> allSnapshotIds = new HashMap<>();
        final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : snapshotsService.currentSnapshots(repo)) {
            SnapshotId snapshotId = snapshotInfo.snapshotId();
            allSnapshotIds.put(snapshotId.getName(), snapshotId);
            currentSnapshots.add(snapshotInfo);
        }

        final RepositoryData repositoryData;
        if (isCurrentSnapshotsOnly(snapshots) == false) {
            repositoryData = snapshotsService.getRepositoryData(repo);
            for (SnapshotId snapshotId : repositoryData.getAllSnapshotIds()) {
                allSnapshotIds.put(snapshotId.getName(), snapshotId);
            }
        } else {
            repositoryData = null;
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
            final Set<SnapshotId> incompatibleSnapshots = repositoryData != null ?
                    new HashSet<>(repositoryData.getIncompatibleSnapshotIds()) : Collections.emptySet();
            snapshotInfos = snapshotsService.snapshots(repo, new ArrayList<>(toResolve),
                    incompatibleSnapshots, ignoreUnavailable);
        } else {
            if (repositoryData != null) {
                // want non-current snapshots as well, which are found in the repository data
                snapshotInfos = buildSimpleSnapshotInfos(repo, toResolve, repositoryData, currentSnapshots);
            } else {
                // only want current snapshots
                snapshotInfos = currentSnapshots.stream().map(SnapshotInfo::basic).collect(Collectors.toList());
            }
        }
        return snapshotInfos;
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshotsOnly(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }

    private List<SnapshotInfo> buildSimpleSnapshotInfos(final String repository, final Set<SnapshotId> toResolve,
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
        for (Map.Entry<SnapshotId, List<String>> entry : snapshotsToIndices.entrySet()) {
            final List<String> indices = entry.getValue();
            CollectionUtil.timSort(indices);
            final SnapshotId snapshotId = entry.getKey();
            snapshotInfos.add(new SnapshotInfo(snapshotId, repository, indices, repositoryData.getSnapshotState(snapshotId)));
        }
        return Collections.unmodifiableList(snapshotInfos);
    }
}
