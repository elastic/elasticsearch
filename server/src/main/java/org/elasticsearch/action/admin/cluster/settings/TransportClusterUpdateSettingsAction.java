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

package org.elasticsearch.action.admin.cluster.settings;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TransportClusterUpdateSettingsAction extends
    TransportMasterNodeAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    private final AllocationService allocationService;
    private final ClusterSettings clusterSettings;
    private final TransportService transportService;
    private final Environment environment;
    private final String transportNodeActionName = "cluster:admin/settings/update_secure[n]";

    @Inject
    public TransportClusterUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                ThreadPool threadPool, AllocationService allocationService, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver, ClusterSettings clusterSettings,
                                                Environment environment) {
        super(settings, ClusterUpdateSettingsAction.NAME, false, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, ClusterUpdateSettingsRequest::new);
        this.allocationService = allocationService;
        this.clusterSettings = clusterSettings;
        this.transportService = transportService;
        this.environment = environment;
        transportService.registerRequestHandler(transportNodeActionName, NodeRequest::new, ThreadPool.Names.GENERIC,
                new NodeTransportHandler());
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterUpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        if (request.transientSettings().size() + request.persistentSettings().size() == 1) {
            // only one setting
            if (MetaData.SETTING_READ_ONLY_SETTING.exists(request.persistentSettings())
                || MetaData.SETTING_READ_ONLY_SETTING.exists(request.transientSettings())
                || MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.transientSettings())
                || MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.persistentSettings())) {
                // one of the settings above as the only setting in the request means - resetting the block!
                return null;
            }
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }


    @Override
    protected ClusterUpdateSettingsResponse newResponse() {
        return new ClusterUpdateSettingsResponse();
    }

    @Override
    protected void masterOperation(final ClusterUpdateSettingsRequest request, final ClusterState state,
                                   final ActionListener<ClusterUpdateSettingsResponse> listener) {
        final SettingsUpdater updater = new SettingsUpdater(clusterSettings);
        clusterService.submitStateUpdateTask("cluster_update_settings",
                new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(Priority.IMMEDIATE, request, listener) {

            private volatile boolean changed = false;

            @Override
            protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                return new ClusterUpdateSettingsResponse(acknowledged, updater.getTransientUpdates(), updater.getPersistentUpdate());
            }

            @Override
            public void onAllNodesAcked(@Nullable Exception e) {
                if (changed) {
                    reroute(true);
                } else {
                    super.onAllNodesAcked(e);
                }
            }

            @Override
            public void onAckTimeout() {
                if (changed) {
                    reroute(false);
                } else {
                    super.onAckTimeout();
                }
            }

            private void reroute(final boolean updateSettingsAcked) {
                // We're about to send a second update task, so we need to check if we're still the elected master
                // For example the minimum_master_node could have been breached and we're no longer elected master,
                // so we should *not* execute the reroute.
                if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
                    logger.debug("Skipping reroute after cluster update settings, because node is no longer master");
                    maybeTriggerKeystoreReload(request, updateSettingsAcked);
                    return;
                }

                // The reason the reroute needs to be send as separate update task, is that all the *cluster* settings are encapsulate
                // in the components (e.g. FilterAllocationDecider), so the changes made by the first call aren't visible
                // to the components until the ClusterStateListener instances have been invoked, but are visible after
                // the first update task has been completed.
                clusterService.submitStateUpdateTask("reroute_after_cluster_update_settings",
                        new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(Priority.URGENT, request, listener) {

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        //we wait for the reroute ack only if the update settings was acknowledged
                        return updateSettingsAcked;
                    }

                    @Override
                    // we return when the cluster reroute is acked or it times out but the acknowledged flag depends on whether the
                    // update settings was acknowledged
                    protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                        return new ClusterUpdateSettingsResponse(updateSettingsAcked && acknowledged, updater.getTransientUpdates(),
                            updater.getPersistentUpdate());
                    }
                    
                    @Override
                    public void onAllNodesAcked(@Nullable Exception e) {
                        maybeTriggerKeystoreReload(request, updateSettingsAcked);
                    }

                    @Override
                    public void onAckTimeout() {
                        maybeTriggerKeystoreReload(request, false);
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.debug("failed to perform reroute after cluster settings were updated - current node is no longer a master");
                        maybeTriggerKeystoreReload(request, updateSettingsAcked);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        //if the reroute fails we only log
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to perform [{}]", source), e);
                        listener.onFailure(new ElasticsearchException("reroute after update settings failed", e));
                    }

                    @Override
                    public ClusterState execute(final ClusterState currentState) {
                        // now, reroute in case things that require it changed (e.g. number of replicas)
                        return allocationService.reroute(currentState, "reroute after cluster update settings");
                    }
                });
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to perform [{}]", source), e);
                super.onFailure(source, e);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                final ClusterState clusterState = updater.updateSettings(currentState, request.transientSettings(), request.persistentSettings());
                changed = clusterState != currentState;
                return clusterState;
            }

            private void maybeTriggerKeystoreReload(ClusterUpdateSettingsRequest request, boolean updateSettingsAcked) {
                if (request.secretStorePassword() == null) {
                    listener.onResponse(new ClusterUpdateSettingsResponse(updateSettingsAcked, updater.getTransientUpdates(),
                            updater.getPersistentUpdate()));
                    return;
                }
                final TransportRequestOptions.Builder optionsBuilder = TransportRequestOptions.builder();
                if (request.timeout() != null) {
                    optionsBuilder.withTimeout(request.timeout());
                }
                final DiscoveryNodes nodes = clusterService.state().nodes();
                final AtomicReference<Map<String, byte[]>> secretSettingHashes = new AtomicReference<>();
                final AtomicBoolean success = new AtomicBoolean(true);
                final AtomicInteger counter = new AtomicInteger(nodes.getSize());
                for (final DiscoveryNode node : nodes) {
                    final TransportRequest nodeRequest = new NodeRequest(node.getId(), request.secretStorePassword());
                    transportService.sendRequest(node, transportNodeActionName, nodeRequest, optionsBuilder.build(),
                            new TransportResponseHandler<NodeResponse>() {
                        @Override
                        public NodeResponse newInstance() {
                            return new NodeResponse();
                        }

                        @Override
                        public void handleResponse(NodeResponse response) {
                            secretSettingHashes.compareAndSet(null, response.secretSettingHashes);
                            if (compareSecretSettingHashes(secretSettingHashes.get(), response.secretSettingHashes) == false) {
                                success.set(false);
                            }
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(new ClusterUpdateSettingsResponse(updateSettingsAcked && success.get(),
                                    updater.getTransientUpdates(), updater.getPersistentUpdate()));
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            success.set(false);
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(new ClusterUpdateSettingsResponse(false, updater.getTransientUpdates(),
                                    updater.getPersistentUpdate()));
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
                }
            }

            private boolean compareSecretSettingHashes(Map<String, byte[]> m1, Map<String, byte[]> m2) {
                assert m1 != null && m2 != null;
                if (m1.size() != m2.size()) {
                    return false;
                }
                for (final Map.Entry<String, byte[]> e : m1.entrySet()) {
                    if (Arrays.equals(e.getValue(), m2.get(e.getKey())) == false) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    private class NodeTransportHandler implements TransportRequestHandler<NodeRequest> {

        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel, Task task) throws Exception {
            messageReceived(request, channel);
        }

        @Override
        public void messageReceived(NodeRequest request, TransportChannel channel) throws Exception {
            final Map<String, byte[]> secretHashes = new HashMap<>();
            // TODO Search for abstract components using SecureSetting s and pass them the
            // newly decrypted keystore
            KeyStoreWrapper keystore = null;
            try {
                keystore = KeyStoreWrapper.load(environment.configFile());
                keystore.decrypt(new char[0] /* use password from request */);
                for (final String settingName : keystore.getSettingNames()) {
                    if (settingName.equals(KeyStoreWrapper.SEED_SETTING.getKey())) {
                        continue;
                    }
                    // assume all secure setting values are string typed
                    final MessageDigest valueDigest = MessageDigest.getInstance("SHA-256");
                    try (SecureString settingValue = keystore.getString(settingName)) {
                        for (final char c : settingValue.getChars()) {
                            valueDigest.update((byte)c);
                        }
                    }
                    secretHashes.put(settingName, valueDigest.digest());
                }
            } finally {
                if (keystore != null) {
                    keystore.close();
                }
            }
            channel.sendResponse(new NodeResponse(clusterService.localNode(), secretHashes));
        }

    }

    private static class NodeRequest extends BaseNodeRequest {
        private String secretStorePassword;

        NodeRequest() {
        }

        NodeRequest(String nodeId, String secretStorePassword) {
            super(nodeId);
            this.secretStorePassword = secretStorePassword;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.secretStorePassword = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(this.secretStorePassword);
        }
    }

    private static class NodeResponse extends BaseNodeResponse {

        private Map<String, byte[]> secretSettingHashes;

        NodeResponse() {
        }

        public NodeResponse(DiscoveryNode node, Map<String, byte[]> secretSettingHashes) {
            super(node);
            this.secretSettingHashes = secretSettingHashes;
        }

        public Map<String, byte[]> secretSettingHashes() {
            return this.secretSettingHashes;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.secretSettingHashes = in.readMap(StreamInput::readString, StreamInput::readByteArray);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(this.secretSettingHashes, StreamOutput::writeString, StreamOutput::writeByteArray);
        }
    }

}
