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
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.StringRestResponse;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.*;

/**
 * @author kimchy (shay.banon)
 */
public class HttpServer extends AbstractLifecycleComponent<HttpServer> {

    private final Environment environment;

    private final HttpServerTransport transport;

    private final RestController restController;

    private final TransportNodesInfoAction nodesInfoAction;

    @Inject public HttpServer(Settings settings, Environment environment, HttpServerTransport transport,
                              RestController restController, TransportNodesInfoAction nodesInfoAction) {
        super(settings);
        this.environment = environment;
        this.transport = transport;
        this.restController = restController;
        this.nodesInfoAction = nodesInfoAction;

        transport.httpServerAdapter(new Dispatcher(this));
    }

    static class Dispatcher implements HttpServerAdapter {

        private final HttpServer server;

        Dispatcher(HttpServer server) {
            this.server = server;
        }

        @Override public void dispatchRequest(HttpRequest request, HttpChannel channel) {
            server.internalDispatchRequest(request, channel);
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

    public void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        if (request.rawPath().startsWith("/_plugin/")) {
            handlePluginSite(request, channel);
            return;
        }
        if (!restController.dispatchRequest(request, channel)) {
            if (request.method() == RestRequest.Method.OPTIONS) {
                // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
                StringRestResponse response = new StringRestResponse(OK);
                channel.sendResponse(response);
            } else {
                channel.sendResponse(new StringRestResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
            }
        }
    }

    private void handlePluginSite(HttpRequest request, HttpChannel channel) {
        if (request.method() != RestRequest.Method.GET) {
            channel.sendResponse(new StringRestResponse(FORBIDDEN));
            return;
        }
        int i1 = request.rawPath().indexOf('/', 9);
        if (i1 == -1) {
            channel.sendResponse(new StringRestResponse(NOT_FOUND));
            return;
        }
        String pluginName = request.rawPath().substring(9, i1);
        String sitePath = request.rawPath().substring(i1 + 1);

        if (sitePath.length() == 0) {
            sitePath = "/index.html";
        }

        // Convert file separators.
        sitePath = sitePath.replace('/', File.separatorChar);

        // this is a plugin provided site, serve it as static files from the plugin location
        File siteFile = new File(new File(environment.pluginsFile(), pluginName), "_site");
        File file = new File(siteFile, sitePath);
        if (!file.exists() || file.isHidden()) {
            channel.sendResponse(new StringRestResponse(NOT_FOUND));
            return;
        }
        if (!file.isFile()) {
            channel.sendResponse(new StringRestResponse(FORBIDDEN));
            return;
        }
        if (!file.getAbsolutePath().startsWith(siteFile.getAbsolutePath())) {
            channel.sendResponse(new StringRestResponse(FORBIDDEN));
            return;
        }
        try {
            byte[] data = Streams.copyToByteArray(file);
            channel.sendResponse(new BytesRestResponse(data, ""));
        } catch (IOException e) {
            channel.sendResponse(new StringRestResponse(INTERNAL_SERVER_ERROR));
        }
    }
}
