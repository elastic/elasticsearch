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

package org.elasticsearch.http;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.*;

/**
 *
 */
public class HttpServer extends AbstractLifecycleComponent<HttpServer> {

    private final Environment environment;

    private final HttpServerTransport transport;

    private final RestController restController;

    private final NodeService nodeService;

    private final boolean disableSites;

    private final PluginSiteFilter pluginSiteFilter = new PluginSiteFilter();

    @Inject
    public HttpServer(Settings settings, Environment environment, HttpServerTransport transport,
                      RestController restController,
                      NodeService nodeService) {
        super(settings);
        this.environment = environment;
        this.transport = transport;
        this.restController = restController;
        this.nodeService = nodeService;
        nodeService.setHttpServer(this);

        this.disableSites = componentSettings.getAsBoolean("disable_sites", false);

        transport.httpServerAdapter(new Dispatcher(this));
    }

    static class Dispatcher implements HttpServerAdapter {

        private final HttpServer server;

        Dispatcher(HttpServer server) {
            this.server = server;
        }

        @Override
        public void dispatchRequest(HttpRequest request, HttpChannel channel) {
            server.internalDispatchRequest(request, channel);
        }
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        nodeService.putAttribute("http_address", transport.boundAddress().publishAddress().toString());
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        nodeService.removeAttribute("http_address");
        transport.stop();
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        transport.close();
    }

    public HttpInfo info() {
        return new HttpInfo(transport.boundAddress());
    }

    public HttpStats stats() {
        return transport.stats();
    }

    public void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        if (request.rawPath().startsWith("/_plugin/")) {
            RestFilterChain filterChain = restController.filterChain(pluginSiteFilter);
            filterChain.continueProcessing(request, channel);
            return;
        }
        restController.dispatchRequest(request, channel);
    }


    class PluginSiteFilter extends RestFilter {

        @Override
        public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) {
            handlePluginSite((HttpRequest) request, (HttpChannel) channel);
        }
    }

    void handlePluginSite(HttpRequest request, HttpChannel channel) {
        if (disableSites) {
            channel.sendResponse(new StringRestResponse(FORBIDDEN));
            return;
        }
        if (request.method() == RestRequest.Method.OPTIONS) {
            // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
            StringRestResponse response = new StringRestResponse(OK);
            channel.sendResponse(response);
            return;
        }
        if (request.method() != RestRequest.Method.GET) {
            channel.sendResponse(new StringRestResponse(FORBIDDEN));
            return;
        }
        // TODO for a "/_plugin" endpoint, we should have a page that lists all the plugins?

        String path = request.rawPath().substring("/_plugin/".length());
        int i1 = path.indexOf('/');
        String pluginName;
        String sitePath;
        if (i1 == -1) {
            pluginName = path;
            sitePath = null;
            // TODO This is a path in the form of "/_plugin/head", without a trailing "/", which messes up
            // resources fetching if it does not exists, a better solution would be to send a redirect
            channel.sendResponse(new StringRestResponse(NOT_FOUND));
            return;
        } else {
            pluginName = path.substring(0, i1);
            sitePath = path.substring(i1 + 1);
        }

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
            channel.sendResponse(new BytesRestResponse(data, guessMimeType(sitePath)));
        } catch (IOException e) {
            channel.sendResponse(new StringRestResponse(INTERNAL_SERVER_ERROR));
        }
    }


    // TODO: Don't respond with a mime type that violates the request's Accept header
    private String guessMimeType(String path) {
        int lastDot = path.lastIndexOf('.');
        if (lastDot == -1) {
            return "";
        }
        String extension = path.substring(lastDot + 1).toLowerCase();
        String mimeType = DEFAULT_MIME_TYPES.get(extension);
        if (mimeType == null) {
            return "";
        }
        return mimeType;
    }

    static {
        // This is not an exhaustive list, just the most common types. Call registerMimeType() to add more.
        Map<String, String> mimeTypes = new HashMap<String, String>();
        mimeTypes.put("txt", "text/plain");
        mimeTypes.put("css", "text/css");
        mimeTypes.put("csv", "text/csv");
        mimeTypes.put("htm", "text/html");
        mimeTypes.put("html", "text/html");
        mimeTypes.put("xml", "text/xml");
        mimeTypes.put("js", "text/javascript"); // Technically it should be application/javascript (RFC 4329), but IE8 struggles with that
        mimeTypes.put("xhtml", "application/xhtml+xml");
        mimeTypes.put("json", "application/json");
        mimeTypes.put("pdf", "application/pdf");
        mimeTypes.put("zip", "application/zip");
        mimeTypes.put("tar", "application/x-tar");
        mimeTypes.put("gif", "image/gif");
        mimeTypes.put("jpeg", "image/jpeg");
        mimeTypes.put("jpg", "image/jpeg");
        mimeTypes.put("tiff", "image/tiff");
        mimeTypes.put("tif", "image/tiff");
        mimeTypes.put("png", "image/png");
        mimeTypes.put("svg", "image/svg+xml");
        mimeTypes.put("ico", "image/vnd.microsoft.icon");
        mimeTypes.put("mp3", "audio/mpeg");
        DEFAULT_MIME_TYPES = ImmutableMap.copyOf(mimeTypes);
    }

    public static final Map<String, String> DEFAULT_MIME_TYPES;
}
