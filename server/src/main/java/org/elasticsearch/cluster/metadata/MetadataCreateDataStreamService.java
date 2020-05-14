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
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

public class MetadataCreateDataStreamService {

    private static final Logger logger = LogManager.getLogger(MetadataCreateDataStreamService.class);

    private final ClusterService clusterService;
    private final ActiveShardsObserver activeShardsObserver;
    private final MetadataCreateIndexService metadataCreateIndexService;

    public MetadataCreateDataStreamService(ThreadPool threadPool,
                                           ClusterService clusterService,
                                           MetadataCreateIndexService metadataCreateIndexService) {
        this.clusterService = clusterService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.metadataCreateIndexService = metadataCreateIndexService;
    }

    public void createDataStream(CreateDataStreamClusterStateUpdateRequest request,
                                 ActionListener<AcknowledgedResponse> finalListener) {
        AtomicReference<String> firstBackingIndexRef = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = ActionListener.wrap(
            response -> {
                if (response.isAcknowledged()) {
                    String firstBackingIndexName = firstBackingIndexRef.get();
                    assert firstBackingIndexName != null;
                    activeShardsObserver.waitForActiveShards(
                        new String[]{firstBackingIndexName},
                        ActiveShardCount.DEFAULT,
                        request.masterNodeTimeout(),
                        shardsAcked -> {
                            finalListener.onResponse(new AcknowledgedResponse(true));
                        },
                        finalListener::onFailure);
                } else {
                    finalListener.onResponse(new AcknowledgedResponse(false));
                }
            },
            finalListener::onFailure
        );
        clusterService.submitStateUpdateTask("create-data-stream [" + request.name + "]",
            new AckedClusterStateUpdateTask<>(Priority.HIGH, request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState clusterState = createDataStream(metadataCreateIndexService, currentState, request);
                    firstBackingIndexRef.set(clusterState.metadata().dataStreams().get(request.name).getIndices().get(0).getName());
                    return clusterState;
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }
            });
    }

    public ClusterState createDataStream(CreateDataStreamClusterStateUpdateRequest request, ClusterState current) throws Exception {
        return createDataStream(metadataCreateIndexService, current, request);
    }

    public static final class CreateDataStreamClusterStateUpdateRequest extends ClusterStateUpdateRequest {

        private final String name;
        private final String timestampFieldName;

        public CreateDataStreamClusterStateUpdateRequest(String name,
                                                         String timestampFieldName,
                                                         TimeValue masterNodeTimeout,
                                                         TimeValue timeout) {
            this.name = name;
            this.timestampFieldName = timestampFieldName;
            masterNodeTimeout(masterNodeTimeout);
            ackTimeout(timeout);
        }
    }

    static ClusterState createDataStream(MetadataCreateIndexService metadataCreateIndexService,
                                         ClusterState currentState,
                                         CreateDataStreamClusterStateUpdateRequest request) throws Exception {
        if (currentState.metadata().dataStreams().containsKey(request.name)) {
            throw new IllegalArgumentException("data_stream [" + request.name + "] already exists");
        }

        MetadataCreateIndexService.validateIndexOrAliasName(request.name,
            (s1, s2) -> new IllegalArgumentException("data_stream [" + s1 + "] " + s2));

        if (request.name.toLowerCase(Locale.ROOT).equals(request.name) == false) {
            throw new IllegalArgumentException("data_stream [" + request.name + "] must be lowercase");
        }
        if (request.name.startsWith(".")) {
            throw new IllegalArgumentException("data_stream [" + request.name + "] must not start with '.'");
        }

        String firstBackingIndexName = DataStream.getBackingIndexName(request.name, 1);
        CreateIndexClusterStateUpdateRequest createIndexRequest =
            new CreateIndexClusterStateUpdateRequest("initialize_data_stream", firstBackingIndexName, firstBackingIndexName)
                .settings(Settings.builder().put("index.hidden", true).build());
        currentState = metadataCreateIndexService.applyCreateIndexRequest(currentState, createIndexRequest, false);
        IndexMetadata firstBackingIndex = currentState.metadata().index(firstBackingIndexName);
        assert firstBackingIndex != null;

        Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(
            new DataStream(request.name, request.timestampFieldName, List.of(firstBackingIndex.getIndex())));
        logger.info("adding data stream [{}]", request.name);
        return ClusterState.builder(currentState).metadata(builder).build();
    }

}
