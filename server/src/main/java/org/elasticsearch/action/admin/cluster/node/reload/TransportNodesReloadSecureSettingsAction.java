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

package org.elasticsearch.action.admin.cluster.node.reload;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportNodesReloadSecureSettingsAction extends TransportNodesAction<NodesReloadSecureSettingsRequest,
                                                                    NodesReloadSecureSettingsResponse,
                                                                    TransportNodesReloadSecureSettingsAction.NodeRequest,
                                                                    NodesReloadSecureSettingsResponse.NodeResponse> {

    private final Environment environment;
    private final PluginsService pluginsService;

    @Inject
    public TransportNodesReloadSecureSettingsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                                    ActionFilters actionFilters, Environment environment, PluginsService pluginService) {
        super(NodesReloadSecureSettingsAction.NAME, threadPool, clusterService, transportService, actionFilters,
              NodesReloadSecureSettingsRequest::new, NodeRequest::new, ThreadPool.Names.GENERIC,
              NodesReloadSecureSettingsResponse.NodeResponse.class);
        this.environment = environment;
        this.pluginsService = pluginService;
    }

    @Override
    protected NodesReloadSecureSettingsResponse newResponse(NodesReloadSecureSettingsRequest request,
                                                            List<NodesReloadSecureSettingsResponse.NodeResponse> responses,
                                                            List<FailedNodeException> failures) {
        return new NodesReloadSecureSettingsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(NodesReloadSecureSettingsRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodesReloadSecureSettingsResponse.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodesReloadSecureSettingsResponse.NodeResponse(in);
    }

    @Override
    protected void doExecute(Task task, NodesReloadSecureSettingsRequest request,
                             ActionListener<NodesReloadSecureSettingsResponse> listener) {
        if (request.hasPassword() && isNodeLocal(request) == false && isNodeTransportTLSEnabled() == false) {
            request.closePassword();
            listener.onFailure(
                new ElasticsearchException("Secure settings cannot be updated cluster wide when TLS for the transport layer" +
                " is not enabled. Enable TLS or use the API with a `_local` filter on each node."));
        } else {
            super.doExecute(task, request, ActionListener.wrap(response -> {
                request.closePassword();
                listener.onResponse(response);
            }, e -> {
                request.closePassword();
                listener.onFailure(e);
            }));
        }
    }

    @Override
    protected NodesReloadSecureSettingsResponse.NodeResponse nodeOperation(NodeRequest nodeReloadRequest, Task task) {
        final NodesReloadSecureSettingsRequest request = nodeReloadRequest.request;
        // We default to using an empty string as the keystore password so that we mimic pre 7.3 API behavior
        final SecureString secureSettingsPassword = request.hasPassword() ? request.getSecureSettingsPassword() :
            new SecureString(new char[0]);
        try (KeyStoreWrapper keystore = KeyStoreWrapper.load(environment.configFile())) {
            // reread keystore from config file
            if (keystore == null) {
                return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(),
                        new IllegalStateException("Keystore is missing"));
            }
            // decrypt the keystore using the password from the request
            keystore.decrypt(secureSettingsPassword.getChars());
            // add the keystore to the original node settings object
            final Settings settingsWithKeystore = Settings.builder()
                    .put(environment.settings(), false)
                    .setSecureSettings(keystore)
                    .build();
            final List<Exception> exceptions = new ArrayList<>();
            // broadcast the new settings object (with the open embedded keystore) to all reloadable plugins
            pluginsService.filterPlugins(ReloadablePlugin.class).stream().forEach(p -> {
                try {
                    p.reload(settingsWithKeystore);
                } catch (final Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("Reload failed for plugin [{}]", p.getClass().getSimpleName()),
                            e);
                    exceptions.add(e);
                }
            });
            ExceptionsHelper.rethrowAndSuppress(exceptions);
            return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(), null);
        } catch (final Exception e) {
            return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(), e);
        } finally {
            secureSettingsPassword.close();
        }
    }

    public static class NodeRequest extends TransportRequest {

        NodesReloadSecureSettingsRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesReloadSecureSettingsRequest(in);
        }

        NodeRequest(NodesReloadSecureSettingsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    /**
     * Returns true if the node is configured for TLS on the transport layer
     */
    private boolean isNodeTransportTLSEnabled() {
        return transportService.isTransportSecure();
    }

    private boolean isNodeLocal(NodesReloadSecureSettingsRequest request) {
        if (null == request.concreteNodes()) {
            resolveRequest(request, clusterService.state());
            assert request.concreteNodes() != null;
        }
        final DiscoveryNode[] nodes = request.concreteNodes();
        return nodes.length == 1 && nodes[0].getId().equals(clusterService.localNode().getId());
    }
}
