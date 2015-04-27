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

package org.elasticsearch.http;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Locale;
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

        this.disableSites = this.settings.getAsBoolean("http.disable_sites", false);

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
    protected void doStart() throws ElasticsearchException {
        transport.start();
        if (logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        nodeService.putAttribute("http_address", transport.boundAddress().publishAddress().toString());
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        nodeService.removeAttribute("http_address");
        transport.stop();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        transport.close();
    }

    public HttpInfo info() {
        return transport.info();
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
        public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws IOException {
            handlePluginSite((HttpRequest) request, (HttpChannel) channel);
        }
    }

    void handlePluginSite(HttpRequest request, HttpChannel channel) throws IOException {
        if (disableSites) {
            channel.sendResponse(new BytesRestResponse(FORBIDDEN));
            return;
        }
        if (request.method() == RestRequest.Method.OPTIONS) {
            // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
            channel.sendResponse(new BytesRestResponse(OK));
            return;
        }
        if (request.method() != RestRequest.Method.GET) {
            channel.sendResponse(new BytesRestResponse(FORBIDDEN));
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
            // If a trailing / is missing, we redirect to the right page #2654
            String redirectUrl = request.rawPath() + "/";
            BytesRestResponse restResponse = new BytesRestResponse(RestStatus.MOVED_PERMANENTLY, "text/html", "<head><meta http-equiv=\"refresh\" content=\"0; URL=" + redirectUrl + "></head>");
            restResponse.addHeader("Location", redirectUrl);
            channel.sendResponse(restResponse);
            return;
        } else {
            pluginName = path.substring(0, i1);
            sitePath = path.substring(i1 + 1);
        }

        if (sitePath.length() == 0) {
            sitePath = "/index.html";
        }
        final Path siteFile = environment.pluginsFile().resolve(pluginName).resolve("_site");

        final String separator = siteFile.getFileSystem().getSeparator();
        // Convert file separators.
        sitePath = sitePath.replace("/", separator);
        // this is a plugin provided site, serve it as static files from the plugin location
        Path file = FileSystemUtils.append(siteFile, PathUtils.get(sitePath), 0);

        // return not found instead of forbidden to prevent malicious requests to find out if files exist or dont exist
        if (!Files.exists(file) || Files.isHidden(file) || !file.toAbsolutePath().normalize().startsWith(siteFile.toAbsolutePath())) {
            channel.sendResponse(new BytesRestResponse(NOT_FOUND));
            return;
        }

        BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            // If it's not a dir, we send a 403
            if (!attributes.isDirectory()) {
                channel.sendResponse(new BytesRestResponse(FORBIDDEN));
                return;
            }
            // We don't serve dir but if index.html exists in dir we should serve it
            file = file.resolve("index.html");
            if (!Files.exists(file) || Files.isHidden(file) || !Files.isRegularFile(file)) {
                channel.sendResponse(new BytesRestResponse(FORBIDDEN));
                return;
            }
        }

        try {
            byte[] data = Files.readAllBytes(file);
            channel.sendResponse(new BytesRestResponse(OK, guessMimeType(sitePath), data));
        } catch (IOException e) {
            channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR));
        }
    }


    // TODO: Don't respond with a mime type that violates the request's Accept header
    private String guessMimeType(String path) {
        int lastDot = path.lastIndexOf('.');
        if (lastDot == -1) {
            return "";
        }
        String extension = path.substring(lastDot + 1).toLowerCase(Locale.ROOT);
        String mimeType = DEFAULT_MIME_TYPES.get(extension);
        if (mimeType == null) {
            return "";
        }
        return mimeType;
    }

    static {
        // This is not an exhaustive list, just the most common types. Call registerMimeType() to add more.
        Map<String, String> mimeTypes = new HashMap<>();
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
