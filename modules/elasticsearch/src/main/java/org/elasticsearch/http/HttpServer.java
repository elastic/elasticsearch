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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.http.HttpResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpServer extends AbstractComponent implements LifecycleComponent<HttpServer> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final HttpServerTransport transport;

    private final ThreadPool threadPool;

    private final PathTrie<HttpServerHandler> getHandlers;
    private final PathTrie<HttpServerHandler> postHandlers;
    private final PathTrie<HttpServerHandler> putHandlers;
    private final PathTrie<HttpServerHandler> deleteHandlers;

    @Inject public HttpServer(Settings settings, HttpServerTransport transport, ThreadPool threadPool) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;

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

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
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

    public HttpServer start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        return this;
    }

    public HttpServer stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        transport.stop();
        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        transport.close();
    }

    private void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        final HttpServerHandler httpHandler = getHandler(request);
        if (httpHandler != null) {
            if (httpHandler.spawn()) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            httpHandler.handleRequest(request, channel);
                        } catch (Exception e) {
                            try {
                                channel.sendResponse(new JsonThrowableHttpResponse(request, e));
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
                        channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                    } catch (IOException e1) {
                        logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
                    }
                }
            }
        } else {
            channel.sendResponse(new StringHttpResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
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
        String uri = request.uri();
        int questionMarkIndex = uri.indexOf('?');
        if (questionMarkIndex == -1) {
            return uri;
        }
        return uri.substring(0, questionMarkIndex);
    }


}
