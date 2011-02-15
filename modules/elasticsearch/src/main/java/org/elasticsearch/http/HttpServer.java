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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.StringRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.support.RestUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.*;

/**
 * @author kimchy (shay.banon)
 */
public class HttpServer extends AbstractLifecycleComponent<HttpServer> {

    private final HttpServerTransport transport;

    private final ThreadPool threadPool;

    private final RestController restController;

    private final TransportNodesInfoAction nodesInfoAction;

    private final PathTrie<HttpServerHandler> getHandlers = new PathTrie<HttpServerHandler>(RestUtils.REST_DECODER);
    private final PathTrie<HttpServerHandler> postHandlers = new PathTrie<HttpServerHandler>(RestUtils.REST_DECODER);
    private final PathTrie<HttpServerHandler> putHandlers = new PathTrie<HttpServerHandler>(RestUtils.REST_DECODER);
    private final PathTrie<HttpServerHandler> deleteHandlers = new PathTrie<HttpServerHandler>(RestUtils.REST_DECODER);
    private final PathTrie<HttpServerHandler> headHandlers = new PathTrie<HttpServerHandler>(RestUtils.REST_DECODER);
    private final PathTrie<HttpServerHandler> optionsHandlers = new PathTrie<HttpServerHandler>(RestUtils.REST_DECODER);

    @Inject public HttpServer(Settings settings, HttpServerTransport transport, ThreadPool threadPool,
                              RestController restController, TransportNodesInfoAction nodesInfoAction) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.restController = restController;
        this.nodesInfoAction = nodesInfoAction;

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
        } else if (method == RestRequest.Method.HEAD) {
            headHandlers.insert(path, handler);
        } else if (method == RestRequest.Method.OPTIONS) {
            optionsHandlers.insert(path, handler);
        }
    }

    @Override protected void doStart() throws ElasticSearchException {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        nodesInfoAction.putNodeAttribute("http_address", transport.boundAddress().publishAddress().toString());
    }

    @Override protected void doStop() throws ElasticSearchException {
        nodesInfoAction.removeNodeAttribute("http_address");
        transport.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        transport.close();
    }

    void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        final HttpServerHandler httpHandler = getHandler(request);
        if (httpHandler == null) {
            // if nothing was dispatched by the rest request, send either error or default handling per method
            if (!restController.dispatchRequest(request, channel)) {
                if (request.method() == RestRequest.Method.OPTIONS) {
                    // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
                    StringRestResponse response = new StringRestResponse(OK);
                    channel.sendResponse(response);
                } else {
                    channel.sendResponse(new StringRestResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
                }
            }
        } else {
            if (httpHandler.spawn()) {
                threadPool.cached().execute(new Runnable() {
                    @Override public void run() {
                        try {
                            httpHandler.handleRequest(request, channel);
                        } catch (Exception e) {
                            try {
                                channel.sendResponse(new XContentThrowableRestResponse(request, e));
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
                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
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
        } else if (method == RestRequest.Method.HEAD) {
            return headHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.OPTIONS) {
            return optionsHandlers.retrieve(path, request.params());
        } else {
            return null;
        }
    }

    private String getPath(HttpRequest request) {
        // we use rawPath since we don't want to decode it while processing the path resolution
        // so we can handle things like:
        // my_index/my_type/http%3A%2F%2Fwww.google.com
        return request.rawPath();
    }
}
