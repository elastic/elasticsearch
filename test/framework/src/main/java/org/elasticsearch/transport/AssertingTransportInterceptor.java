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
package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.Collections;
import java.util.List;

/**
 * A transport interceptor that applies {@link ElasticsearchAssertions#assertVersionSerializable(Streamable)}
 * to all requests and response objects send across the wire
 */
public final class AssertingTransportInterceptor implements TransportInterceptor {

    public static final class TestPlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors() {
            return Collections.singletonList(new AssertingTransportInterceptor());
        }
    }
    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action,
                                                                                    TransportRequestHandler<T> actualHandler) {
        return new TransportRequestHandler<T>() {

            @Override
            public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
                ElasticsearchAssertions.assertVersionSerializable(request);
                actualHandler.messageReceived(request, channel, task);
            }

            @Override
            public void messageReceived(T request, TransportChannel channel) throws Exception {
                ElasticsearchAssertions.assertVersionSerializable(request);
                actualHandler.messageReceived(request, channel);
            }
        };
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                  TransportRequestOptions options, TransportResponseHandler<T> handler) {
                ElasticsearchAssertions.assertVersionSerializable(request);
                sender.sendRequest(node, action, request, options, new TransportResponseHandler<T>() {
                    @Override
                    public T newInstance() {
                        return handler.newInstance();
                    }

                    @Override
                    public void handleResponse(T response) {
                        ElasticsearchAssertions.assertVersionSerializable(response);
                        handler.handleResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        handler.handleException(exp);
                    }

                    @Override
                    public String executor() {
                        return handler.executor();
                    }
                });
            }
        };
    }


}
