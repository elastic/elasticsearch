/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MetadataMigrateToDataStreamService {

    private static final Logger logger = LogManager.getLogger(MetadataMigrateToDataStreamService.class);

    private final ClusterService clusterService;
    private final ActiveShardsObserver activeShardsObserver;
    private final MetadataCreateIndexService metadataCreateIndexService;

    @Inject
    public MetadataMigrateToDataStreamService(ThreadPool threadPool,
                                              ClusterService clusterService,
                                              MetadataCreateIndexService metadataCreateIndexService) {
        this.clusterService = clusterService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.metadataCreateIndexService = metadataCreateIndexService;
    }

    public void migrateToDataStream(MigrateToDataStreamClusterStateUpdateRequest request,
                                    ActionListener<AcknowledgedResponse> finalListener) {
        AtomicReference<String> writeIndexRef = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = ActionListener.wrap(
            response -> {
                if (response.isAcknowledged()) {
                    String writeIndexName = writeIndexRef.get();
                    assert writeIndexName != null;
                    activeShardsObserver.waitForActiveShards(
                        new String[]{writeIndexName},
                        ActiveShardCount.DEFAULT,
                        request.masterNodeTimeout(),
                        shardsAcked -> {
                            finalListener.onResponse(AcknowledgedResponse.TRUE);
                        },
                        finalListener::onFailure);
                } else {
                    finalListener.onResponse(AcknowledgedResponse.FALSE);
                }
            },
            finalListener::onFailure
        );
        clusterService.submitStateUpdateTask("migrate-to-data-stream [" + request.aliasName + "]",
            new AckedClusterStateUpdateTask<>(Priority.HIGH, request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState clusterState = migrateToDataStream(metadataCreateIndexService, currentState, request);
                    writeIndexRef.set(clusterState.metadata().dataStreams().get(request.aliasName).getWriteIndex().getName());
                    return clusterState;
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }
            });
    }

    static ClusterState migrateToDataStream(MetadataCreateIndexService metadataCreateIndexService,
                                            ClusterState currentState,
                                            MigrateToDataStreamClusterStateUpdateRequest request) throws Exception {
        validateRequest(currentState, request);
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) currentState.metadata().getIndicesLookup().get(request.aliasName);
        IndexMetadata writeIndex = alias.getWriteIndex();

        List<IndexMetadata> backingIndices = alias.getIndices()
            .stream()
            .filter(x -> writeIndex == null || x.getIndex().getName().equals(writeIndex.getIndex().getName()) == false)
            .collect(Collectors.toList());

        return MetadataCreateDataStreamService.createDataStream(
            metadataCreateIndexService, currentState, request.aliasName, backingIndices, writeIndex);
    }

    // package-visible for testing
    static void validateRequest(ClusterState currentState, MigrateToDataStreamClusterStateUpdateRequest request) {
        IndexAbstraction ia = currentState.metadata().getIndicesLookup().get(request.aliasName);
        if (ia == null || ia.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] does not exist");
        }
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) ia;

        if (alias.getWriteIndex() == null) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] must specify a write index");
        }

        // check for "clean" alias without routing or filter query
        AliasMetadata aliasMetadata = alias.getFirstAliasMetadata();
        assert aliasMetadata != null : "alias metadata may not be null";
        if (aliasMetadata.filteringRequired() || aliasMetadata.getIndexRouting() != null || aliasMetadata.getSearchRouting() != null) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] may not have custom filtering or routing");
        }
    }

    public static final class MigrateToDataStreamClusterStateUpdateRequest extends ClusterStateUpdateRequest {

        private final String aliasName;

        public MigrateToDataStreamClusterStateUpdateRequest(String aliasName,
                                                            TimeValue masterNodeTimeout,
                                                            TimeValue timeout) {
            this.aliasName = aliasName;
            masterNodeTimeout(masterNodeTimeout);
            ackTimeout(timeout);
        }
    }

}
