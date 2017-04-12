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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportGetSnapshotsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, SnapshotsService snapshotsService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetSnapshotsAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
              GetSnapshotsRequest::new);
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
    protected void masterOperation(final GetSnapshotsRequest request, ClusterState state,
                                   final ActionListener<GetSnapshotsResponse> listener) {
        try {
            final String repository = request.repository();
            List<SnapshotInfo> snapshotInfoBuilder = new ArrayList<>();
            final Map<String, SnapshotId> allSnapshotIds = new HashMap<>();
            final List<SnapshotId> currentSnapshotIds = new ArrayList<>();
            for (SnapshotInfo snapshotInfo : snapshotsService.currentSnapshots(repository)) {
                SnapshotId snapshotId = snapshotInfo.snapshotId();
                allSnapshotIds.put(snapshotId.getName(), snapshotId);
                currentSnapshotIds.add(snapshotId);
            }
            if (isCurrentSnapshotsOnly(request.snapshots()) == false) {
                for (SnapshotId snapshotId : snapshotsService.getRepositoryData(repository).getAllSnapshotIds()) {
                    allSnapshotIds.put(snapshotId.getName(), snapshotId);
                }
            }
            final Set<SnapshotId> toResolve = new HashSet<>();
            if (isAllSnapshots(request.snapshots())) {
                toResolve.addAll(allSnapshotIds.values());
            } else {
                for (String snapshotOrPattern : request.snapshots()) {
                    if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                        toResolve.addAll(currentSnapshotIds);
                    } else if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                        if (allSnapshotIds.containsKey(snapshotOrPattern)) {
                            toResolve.add(allSnapshotIds.get(snapshotOrPattern));
                        } else if (request.ignoreUnavailable() == false) {
                            throw new SnapshotMissingException(repository, snapshotOrPattern);
                        }
                    } else {
                        for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
                            if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                                toResolve.add(entry.getValue());
                            }
                        }
                    }
                }

                if (toResolve.isEmpty() && request.ignoreUnavailable() == false && isCurrentSnapshotsOnly(request.snapshots()) == false) {
                    throw new SnapshotMissingException(repository, request.snapshots()[0]);
                }
            }

            snapshotInfoBuilder.addAll(snapshotsService.snapshots(repository, new ArrayList<>(toResolve), request.ignoreUnavailable()));
            listener.onResponse(new GetSnapshotsResponse(snapshotInfoBuilder));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshotsOnly(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }
}
