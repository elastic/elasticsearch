/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.jmx.action;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StringStreamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.jmx.JmxService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class GetJmxServiceUrlAction extends AbstractComponent {

    private final JmxService jmxService;

    private final TransportService transportService;

    private final ClusterService clusterService;

    @Inject
    public GetJmxServiceUrlAction(Settings settings, JmxService jmxService,
                                  TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.jmxService = jmxService;
        this.transportService = transportService;
        this.clusterService = clusterService;

        transportService.registerHandler(GetJmxServiceUrlTransportHandler.ACTION, new GetJmxServiceUrlTransportHandler());
    }

    public String obtainPublishUrl(final DiscoveryNode node) throws ElasticSearchException {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            return jmxService.publishUrl();
        } else {
            return transportService.submitRequest(node, GetJmxServiceUrlTransportHandler.ACTION, VoidStreamable.INSTANCE, new FutureTransportResponseHandler<StringStreamable>() {
                @Override
                public StringStreamable newInstance() {
                    return new StringStreamable();
                }
            }).txGet().get();
        }
    }

    private class GetJmxServiceUrlTransportHandler extends BaseTransportRequestHandler<VoidStreamable> {

        static final String ACTION = "jmx/publishUrl";

        @Override
        public VoidStreamable newInstance() {
            return VoidStreamable.INSTANCE;
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(VoidStreamable request, TransportChannel channel) throws Exception {
            channel.sendResponse(new StringStreamable(jmxService.publishUrl()));
        }
    }
}