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

package org.elasticsearch.cluster.action.index;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
 * in the cluster state meta data (and broadcast to all members).
 */
public class MappingUpdatedAction extends TransportMasterNodeOperationAction<MappingUpdatedAction.MappingUpdatedRequest, MappingUpdatedAction.MappingUpdatedResponse> {

    public static final String INDICES_MAPPING_DYNAMIC_TIMEOUT = "indices.mapping.dynamic_timeout";
    public static final String ACTION_NAME = "internal:cluster/mapping_updated";

    private final MetaDataMappingService metaDataMappingService;

    private volatile MasterMappingUpdater masterMappingUpdater;

    private volatile TimeValue dynamicMappingUpdateTimeout;

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue current = MappingUpdatedAction.this.dynamicMappingUpdateTimeout;
            TimeValue newValue = settings.getAsTime(INDICES_MAPPING_DYNAMIC_TIMEOUT, current);
            if (!current.equals(newValue)) {
                logger.info("updating " + INDICES_MAPPING_DYNAMIC_TIMEOUT + " from [{}] to [{}]", current, newValue);
                MappingUpdatedAction.this.dynamicMappingUpdateTimeout = newValue;
            }
        }
    }

    @Inject
    public MappingUpdatedAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                MetaDataMappingService metaDataMappingService, NodeSettingsService nodeSettingsService, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters);
        this.metaDataMappingService = metaDataMappingService;
        this.dynamicMappingUpdateTimeout = settings.getAsTime(INDICES_MAPPING_DYNAMIC_TIMEOUT, TimeValue.timeValueSeconds(30));
        nodeSettingsService.addListener(new ApplySettings());
    }

    public void start() {
        this.masterMappingUpdater = new MasterMappingUpdater(EsExecutors.threadName(settings, "master_mapping_updater"));
        this.masterMappingUpdater.start();
    }

    public void stop() {
        this.masterMappingUpdater.close();
        this.masterMappingUpdater = null;
    }

    public void updateMappingOnMaster(String index, String indexUUID, String type, Mapping mappingUpdate, MappingUpdateListener listener) {
        if (type.equals(MapperService.DEFAULT_MAPPING)) {
            throw new ElasticsearchIllegalArgumentException("_default_ mapping should not be updated");
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            mappingUpdate.toXContent(builder, new ToXContent.MapParams(ImmutableMap.<String, String>of()));
            final CompressedString mappingSource = new CompressedString(builder.endObject().bytes());
            masterMappingUpdater.add(new MappingChange(index, indexUUID, type, mappingSource, listener));
        } catch (IOException bogus) {
            throw new AssertionError("Cannot happen", bogus);
        }
    }

    /**
     * Same as {@link #updateMappingOnMasterSynchronously(String, String, String, Mapping, TimeValue)}
     * using the default timeout.
     */
    public void updateMappingOnMasterSynchronously(String index, String indexUUID, String type, Mapping mappingUpdate) throws Throwable {
        updateMappingOnMasterSynchronously(index, indexUUID, type, mappingUpdate, dynamicMappingUpdateTimeout);
    }

    /**
     * Update mappings synchronously on the master node, waiting for at most
     * {@code timeout}. When this method returns successfully mappings have
     * been applied to the master node and propagated to data nodes.
     */
    public void updateMappingOnMasterSynchronously(String index, String indexUUID, String type, Mapping mappingUpdate, TimeValue timeout) throws Throwable {
        final CountDownLatch latch = new CountDownLatch(1);
        final Throwable[] cause = new Throwable[1];
        final MappingUpdateListener listener = new MappingUpdateListener() {

            @Override
            public void onMappingUpdate() {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                cause[0] = t;
                latch.countDown();
            }

        };

        updateMappingOnMaster(index, indexUUID, type, mappingUpdate, listener);
        if (!latch.await(timeout.getMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Time out while waiting for the master node to validate a mapping update for type [" + type + "]");
        }
        if (cause[0] != null) {
            throw cause[0];
        }
    }

    @Override
    protected ClusterBlockException checkBlock(MappingUpdatedRequest request, ClusterState state) {
        // internal call by other nodes, no need to check for blocks
        return null;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected MappingUpdatedRequest newRequest() {
        return new MappingUpdatedRequest();
    }

    @Override
    protected MappingUpdatedResponse newResponse() {
        return new MappingUpdatedResponse();
    }

    @Override
    protected void masterOperation(final MappingUpdatedRequest request, final ClusterState state, final ActionListener<MappingUpdatedResponse> listener) throws ElasticsearchException {
        metaDataMappingService.updateMapping(request.index(), request.indexUUID(), request.type(), request.mappingSource(), request.nodeId, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new MappingUpdatedResponse());
            }

            @Override
            public void onFailure(Throwable t) {
                logger.warn("[{}] update-mapping [{}] failed to dynamically update the mapping in cluster_state from shard", t, request.index(), request.type());
                listener.onFailure(t);
            }
        });
    }

    public static class MappingUpdatedResponse extends ActionResponse {
        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class MappingUpdatedRequest extends MasterNodeOperationRequest<MappingUpdatedRequest> implements IndicesRequest {

        private String index;
        private String indexUUID = IndexMetaData.INDEX_UUID_NA_VALUE;
        private String type;
        private CompressedString mappingSource;
        private String nodeId = null; // null means not set

        MappingUpdatedRequest() {
        }

        public MappingUpdatedRequest(String index, String indexUUID, String type, CompressedString mappingSource, String nodeId) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.type = type;
            this.mappingSource = mappingSource;
            this.nodeId = nodeId;
        }

        public String index() {
            return index;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        public String indexUUID() {
            return indexUUID;
        }

        public String type() {
            return type;
        }

        public CompressedString mappingSource() {
            return mappingSource;
        }

        /**
         * Returns null for not set.
         */
        public String nodeId() {
            return this.nodeId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
            type = in.readString();
            mappingSource = CompressedString.readCompressedString(in);
            indexUUID = in.readString();
            nodeId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(type);
            mappingSource.writeTo(out);
            out.writeString(indexUUID);
            out.writeOptionalString(nodeId);
        }

        @Override
        public String toString() {
            return "index [" + index + "], indexUUID [" + indexUUID + "], type [" + type + "] and source [" + mappingSource + "]";
        }
    }

    private static class MappingChange {
        public final String index;
        public final String indexUUID;
        public final String type;
        public final CompressedString mappingSource;
        public final MappingUpdateListener listener;

        MappingChange(String index, String indexUUID, String type, CompressedString mappingSource, MappingUpdateListener listener) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.type = type;
            this.mappingSource = mappingSource;
            this.listener = listener;
        }
    }

    /**
     * A listener to be notified when the mappings were updated
     */
    public static interface MappingUpdateListener {

        void onMappingUpdate();

        void onFailure(Throwable t);
    }

    /**
     * The master mapping updater removes the overhead of refreshing the mapping (refreshSource) on the
     * indexing thread.
     * <p/>
     * It also allows to reduce multiple mapping updates on the same index(UUID) and type into one update
     * (refreshSource + sending to master), which allows to offload the number of times mappings are updated
     * and sent to master for heavy single index requests that each introduce a new mapping, and when
     * multiple shards exists on the same nodes, allowing to work on the index level in this case.
     */
    private class MasterMappingUpdater extends Thread {

        private volatile boolean running = true;
        private final BlockingQueue<MappingChange> queue = ConcurrentCollections.newBlockingQueue();

        public MasterMappingUpdater(String name) {
            super(name);
        }

        public void add(MappingChange change) {
            queue.add(change);
        }

        public void close() {
            running = false;
            this.interrupt();
        }

        @Override
        public void run() {
            while (running) {
                MappingUpdateListener listener = null;
                try {
                    final MappingChange change = queue.poll(10, TimeUnit.MINUTES);
                    if (change == null) {
                        continue;
                    }
                    listener = change.listener;

                    final MappingUpdatedAction.MappingUpdatedRequest mappingRequest;
                    try {
                        DiscoveryNode node = clusterService.localNode();
                        mappingRequest = new MappingUpdatedAction.MappingUpdatedRequest(
                                change.index, change.indexUUID, change.type, change.mappingSource, node != null ? node.id() : null
                        );
                    } catch (Throwable t) {
                        logger.warn("Failed to update master on updated mapping for index [" + change.index + "], type [" + change.type + "]", t);
                        if (change.listener != null) {
                            change.listener.onFailure(t);
                        }
                        continue;
                    }
                    logger.trace("sending mapping updated to master: {}", mappingRequest);
                    execute(mappingRequest, new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                        @Override
                        public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                            logger.debug("successfully updated master with mapping update: {}", mappingRequest);
                            if (change.listener != null) {
                                change.listener.onMappingUpdate();
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.warn("failed to update master on updated mapping for {}", e, mappingRequest);
                            if (change.listener != null) {
                                change.listener.onFailure(e);
                            }
                        }
                    });
                } catch (Throwable t) {
                    if (listener != null) {
                        // even if the failure is expected, eg. if we got interrupted,
                        // we need to notify the listener as there might be a latch
                        // waiting for it to be called
                        listener.onFailure(t);
                    }
                    if (t instanceof InterruptedException && !running) {
                        // all is well, we are shutting down
                    } else {
                        logger.warn("failed to process mapping update", t);
                    }
                }
            }
        }
    }
}