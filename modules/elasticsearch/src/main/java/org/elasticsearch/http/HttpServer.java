/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.http;

import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfo;
import org.elasticsearch.rest.JsonThrowableRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.path.PathTrie;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class HttpServer extends AbstractLifecycleComponent<HttpServer> {

    private final HttpServerTransport transport;

    private final ThreadPool threadPool;

    private final RestController restController;

    private final TransportNodesInfo nodesInfo;

    private final PathTrie<HttpServerHandler> getHandlers;
    private final PathTrie<HttpServerHandler> postHandlers;
    private final PathTrie<HttpServerHandler> putHandlers;
    private final PathTrie<HttpServerHandler> deleteHandlers;

    @Inject public HttpServer(Settings settings, HttpServerTransport transport, ThreadPool threadPool,
                              RestController restController, TransportNodesInfo nodesInfo) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.restController = restController;
        this.nodesInfo = nodesInfo;

        getHandlers = new PathTrie<HttpServerHandler>();
        postHandlers = new PathTrie<HttpServerHandler>();
        putHandlers = new PathTrie<HttpServerHandler>();
        deleteHandlers = new PathTrie<HttpServerHandler>();

        transport.httpServerAdapter(new HttpServerAdapter() {
            @Override public void dispatchRequest(HttpRequest request, HttpChannel channel) {
                internalDispatchRequest(request, channel);
            }
        });
    }

    public void registerHandler(HttpRequest.Method method, String path, HttpServerHandler handler) {
        if (method == HttpRequest.Method.GET) {
            getHandlers.insert(path, handler);
        } else if (method == HttpRequest.Method.POST) {
            postHandlers.insert(path, handler);
        } else if (method == HttpRequest.Method.PUT) {
            putHandlers.insert(path, handler);
        } else if (method == HttpRequest.Method.DELETE) {
            deleteHandlers.insert(path, handler);
        }
    }

    @Override protected void doStart() throws ElasticSearchException {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        nodesInfo.putNodeAttribute("http_address", transport.boundAddress().publishAddress().toString());
    }

    @Override protected void doStop() throws ElasticSearchException {
        nodesInfo.removeNodeAttribute("http_address");
        transport.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        transport.close();
    }

    void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        final HttpServerHandler httpHandler = getHandler(request);
        if (httpHandler == null) {
            restController.dispatchRequest(request, channel);
        } else {
            if (httpHandler.spawn()) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            httpHandler.handleRequest(request, channel);
                        } catch (Exception e) {
                            try {
                                channel.sendResponse(new JsonThrowableRestResponse(request, e));
                            } catch (IOException e1) {
                                logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
                            }
                        }
                    }
                });
            } else {
                try {
                    httpHandler.handleRequest(request, channel);
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new JsonThrowableRestResponse(request, e));
                    } catch (IOException e1) {
                        logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
                    }
                }
            }
        }
    }

    private HttpServerHandler getHandler(HttpRequest request) {
        String path = getPath(request);
        HttpRequest.Method method = request.method();
        if (method == HttpRequest.Method.GET) {
            return getHandlers.retrieve(path, request.params());
        } else if (method == HttpRequest.Method.POST) {
            return postHandlers.retrieve(path, request.params());
        } else if (method == HttpRequest.Method.PUT) {
            return putHandlers.retrieve(path, request.params());
        } else if (method == HttpRequest.Method.DELETE) {
            return deleteHandlers.retrieve(path, request.params());
        } else {
            return null;
        }
    }

    private String getPath(HttpRequest request) {
        return request.path();
    }
}
