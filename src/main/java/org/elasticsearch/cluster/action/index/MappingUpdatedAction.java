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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
 * in the cluster state meta data (and broadcast to all members).
 */
public class MappingUpdatedAction extends TransportMasterNodeOperationAction<MappingUpdatedAction.MappingUpdatedRequest, MappingUpdatedAction.MappingUpdatedResponse> {

    public static final String INDICES_MAPPING_ADDITIONAL_MAPPING_CHANGE_TIME = "indices.mapping.additional_mapping_change_time";
    public static final String ACTION_NAME = "internal:cluster/mapping_updated";

    private final AtomicLong mappingUpdateOrderGen = new AtomicLong();
    private final MetaDataMappingService metaDataMappingService;

    private volatile MasterMappingUpdater masterMappingUpdater;

    private volatile TimeValue additionalMappingChangeTime;

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            final TimeValue current = MappingUpdatedAction.this.additionalMappingChangeTime;
            final TimeValue newValue = settings.getAsTime(INDICES_MAPPING_ADDITIONAL_MAPPING_CHANGE_TIME, current);
            if (!current.equals(newValue)) {
                logger.info("updating " + INDICES_MAPPING_ADDITIONAL_MAPPING_CHANGE_TIME + " from [{}] to [{}]", current, newValue);
                MappingUpdatedAction.this.additionalMappingChangeTime = newValue;
            }
        }
    }

    @Inject
    public MappingUpdatedAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                MetaDataMappingService metaDataMappingService, NodeSettingsService nodeSettingsService, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters);
        this.metaDataMappingService = metaDataMappingService;
        // this setting should probably always be 0, just add the option to wait for more changes within a time window
        this.additionalMappingChangeTime = settings.getAsTime(INDICES_MAPPING_ADDITIONAL_MAPPING_CHANGE_TIME, TimeValue.timeValueMillis(0));
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

    public void updateMappingOnMaster(String index, DocumentMapper documentMapper, String indexUUID) {
        updateMappingOnMaster(index, documentMapper, indexUUID, null);
    }

    public void updateMappingOnMaster(String index, DocumentMapper documentMapper, String indexUUID, MappingUpdateListener listener) {
        assert !documentMapper.type().equals(MapperService.DEFAULT_MAPPING) : "_default_ mapping should not be updated";
        masterMappingUpdater.add(new MappingChange(documentMapper, index, indexUUID, listener));
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
        metaDataMappingService.updateMapping(request.index(), request.indexUUID(), request.type(), request.mappingSource(), request.order, request.nodeId, new ActionListener<ClusterStateUpdateResponse>() {
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
        private long order = -1; // -1 means not set...
        private String nodeId = null; // null means not set

        MappingUpdatedRequest() {
        }

        public MappingUpdatedRequest(String index, String indexUUID, String type, CompressedString mappingSource, long order, String nodeId) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.type = type;
            this.mappingSource = mappingSource;
            this.order = order;
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
         * Returns -1 if not set...
         */
        public long order() {
            return this.order;
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
            order = in.readLong();
            nodeId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(type);
            mappingSource.writeTo(out);
            out.writeString(indexUUID);
            out.writeLong(order);
            out.writeOptionalString(nodeId);
        }

        @Override
        public String toString() {
            return "index [" + index + "], indexUUID [" + indexUUID + "], type [" + type + "] and source [" + mappingSource + "]";
        }
    }

    private static class MappingChange {
        public final DocumentMapper documentMapper;
        public final String index;
        public final String indexUUID;
        public final MappingUpdateListener listener;

        MappingChange(DocumentMapper documentMapper, String index, String indexUUID, MappingUpdateListener listener) {
            this.documentMapper = documentMapper;
            this.index = index;
            this.indexUUID = indexUUID;
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

        class UpdateKey {
            public final String indexUUID;
            public final String type;

            UpdateKey(String indexUUID, String type) {
                this.indexUUID = indexUUID;
                this.type = type;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                UpdateKey updateKey = (UpdateKey) o;

                if (!indexUUID.equals(updateKey.indexUUID)) {
                    return false;
                }
                if (!type.equals(updateKey.type)) {
                    return false;
                }

                return true;
            }

            @Override
            public int hashCode() {
                int result = indexUUID.hashCode();
                result = 31 * result + type.hashCode();
                return result;
            }
        }

        class UpdateValue {
            public final MappingChange mainChange;
            public final List<MappingUpdateListener> listeners = Lists.newArrayList();

            UpdateValue(MappingChange mainChange) {
                this.mainChange = mainChange;
            }

            public void notifyListeners(@Nullable Throwable t) {
                for (MappingUpdateListener listener : listeners) {
                    try {
                        if (t == null) {
                            listener.onMappingUpdate();
                        } else {
                            listener.onFailure(t);
                        }
                    } catch (Throwable lisFailure) {
                        logger.warn("unexpected failure on mapping update listener callback [{}]", lisFailure, listener);
                    }
                }
            }
        }

        @Override
        public void run() {
            Map<UpdateKey, UpdateValue> pendingUpdates = Maps.newHashMap();
            while (running) {
                try {
                    MappingChange polledChange = queue.poll(10, TimeUnit.MINUTES);
                    if (polledChange == null) {
                        continue;
                    }
                    List<MappingChange> changes = Lists.newArrayList(polledChange);
                    if (additionalMappingChangeTime.millis() > 0) {
                        Thread.sleep(additionalMappingChangeTime.millis());
                    }
                    queue.drainTo(changes);
                    Collections.reverse(changes); // process then in newest one to oldest
                    // go over and add to pending updates map
                    for (MappingChange change : changes) {
                        UpdateKey key = new UpdateKey(change.indexUUID, change.documentMapper.type());
                        UpdateValue updateValue = pendingUpdates.get(key);
                        if (updateValue == null) {
                            updateValue = new UpdateValue(change);
                            pendingUpdates.put(key, updateValue);
                        }
                        if (change.listener != null) {
                            updateValue.listeners.add(change.listener);
                        }
                    }

                    for (Iterator<UpdateValue> iterator = pendingUpdates.values().iterator(); iterator.hasNext(); ) {
                        final UpdateValue updateValue = iterator.next();
                        iterator.remove();
                        MappingChange change = updateValue.mainChange;

                        final MappingUpdatedAction.MappingUpdatedRequest mappingRequest;
                        try {
                            // we generate the order id before we get the mapping to send and refresh the source, so
                            // if 2 happen concurrently, we know that the later order will include the previous one
                            long orderId = mappingUpdateOrderGen.incrementAndGet();
                            change.documentMapper.refreshSource();
                            DiscoveryNode node = clusterService.localNode();
                            mappingRequest = new MappingUpdatedAction.MappingUpdatedRequest(
                                    change.index, change.indexUUID, change.documentMapper.type(), change.documentMapper.mappingSource(), orderId, node != null ? node.id() : null
                            );
                        } catch (Throwable t) {
                            logger.warn("Failed to update master on updated mapping for index [" + change.index + "], type [" + change.documentMapper.type() + "]", t);
                            updateValue.notifyListeners(t);
                            continue;
                        }
                        logger.trace("sending mapping updated to master: {}", mappingRequest);
                        execute(mappingRequest, new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                            @Override
                            public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                                logger.debug("successfully updated master with mapping update: {}", mappingRequest);
                                updateValue.notifyListeners(null);
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                logger.warn("failed to update master on updated mapping for {}", e, mappingRequest);
                                updateValue.notifyListeners(e);
                            }
                        });

                    }
                } catch (Throwable t) {
                    if (t instanceof InterruptedException && !running) {
                        // all is well, we are shutting down
                    } else {
                        logger.warn("failed to process mapping updates", t);
                    }
                    // cleanup all pending update callbacks that were not processed due to a global failure...
                    for (Iterator<Map.Entry<UpdateKey, UpdateValue>> iterator = pendingUpdates.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<UpdateKey, UpdateValue> entry = iterator.next();
                        iterator.remove();
                        entry.getValue().notifyListeners(t);
                    }
                }
            }
        }
    }
}