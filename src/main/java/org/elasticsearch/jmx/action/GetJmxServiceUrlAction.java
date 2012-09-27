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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.jmx.JmxService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

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
            return transportService.submitRequest(node, GetJmxServiceUrlTransportHandler.ACTION, TransportRequest.Empty.INSTANCE, new FutureTransportResponseHandler<GetJmxServiceUrlResponse>() {
                @Override
                public GetJmxServiceUrlResponse newInstance() {
                    return new GetJmxServiceUrlResponse();
                }
            }).txGet().url();
        }
    }

    private class GetJmxServiceUrlTransportHandler extends BaseTransportRequestHandler<TransportRequest.Empty> {

        static final String ACTION = "jmx/publishUrl";

        @Override
        public TransportRequest.Empty newInstance() {
            return TransportRequest.Empty.INSTANCE;
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(TransportRequest.Empty request, TransportChannel channel) throws Exception {
            channel.sendResponse(new GetJmxServiceUrlResponse(jmxService.publishUrl()));
        }
    }

    static class GetJmxServiceUrlResponse extends TransportResponse {

        private String url;

        GetJmxServiceUrlResponse() {
        }

        GetJmxServiceUrlResponse(String url) {
            this.url = url;
        }

        public String url() {
            return this.url;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            url = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(url);
        }
    }
}