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

package org.elasticsearch.wares;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.support.RestUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

/**
 * A servlet that can be used to dispatch requests to elasticsearch. A {@link Node} will be started, reading
 * config from either <tt>/WEB-INF/elasticsearch.json</tt> or <tt>/WEB-INF/elasticsearch.yml</tt> but, by defualt,
 * with its internal HTTP interface disabled.
 *
 * <p>The node is registered as a servlet context attribute under <tt>elasticsearchNode</tt> so its easily
 * accessible from other web resources if needed.
 */
public class NodeServlet extends HttpServlet {

    public static String NODE_KEY = "elasticsearchNode";

    private Node node;

    private RestController restController;

    @Override public void init() throws ServletException {
        getServletContext().log("Initializing elasticsearch Node '" + getServletName() + "'");
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();

        InputStream resourceAsStream = getServletContext().getResourceAsStream("/WEB-INF/elasticsearch.json");
        if (resourceAsStream != null) {
            settings.loadFromStream("/WEB-INF/elasticsearch.json", resourceAsStream);
            try {
                resourceAsStream.close();
            } catch (IOException e) {
                // ignore
            }
        }

        resourceAsStream = getServletContext().getResourceAsStream("/WEB-INF/elasticsearch.yml");
        if (resourceAsStream != null) {
            settings.loadFromStream("/WEB-INF/elasticsearch.yml", resourceAsStream);
            try {
                resourceAsStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (settings.get("http.enabled") == null) {
            settings.put("http.enabled", false);
        }

        node = NodeBuilder.nodeBuilder().settings(settings).node();
        restController = ((InternalNode) node).injector().getInstance(RestController.class);
        getServletContext().setAttribute(NODE_KEY, node);
    }

    @Override public void destroy() {
        if (node != null) {
            getServletContext().removeAttribute(NODE_KEY);
            node.close();
        }
    }

    @Override protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ServletRestRequest request = new ServletRestRequest(req);
        ServletRestChannel channel = new ServletRestChannel(request, resp);
        try {
            if (!restController.dispatchRequest(request, channel)) {
                throw new ServletException("No mapping found for [" + request.uri() + "]");
            }
            channel.latch.await();
        } catch (ServletException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("failed to dispatch request", e);
        }
        if (channel.sendFailure != null) {
            throw channel.sendFailure;
        }
    }

    static class ServletRestChannel implements RestChannel {

        final RestRequest restRequest;

        final HttpServletResponse resp;

        final CountDownLatch latch;

        IOException sendFailure;

        ServletRestChannel(RestRequest restRequest, HttpServletResponse resp) {
            this.restRequest = restRequest;
            this.resp = resp;
            this.latch = new CountDownLatch(1);
        }

        @Override public void sendResponse(RestResponse response) {
            resp.setContentType(response.contentType());
            if (RestUtils.isBrowser(restRequest.header("User-Agent"))) {
                resp.addHeader("Access-Control-Allow-Origin", "*");
                if (restRequest.method() == RestRequest.Method.OPTIONS) {
                    // also add more access control parameters
                    resp.addHeader("Access-Control-Max-Age", "1728000");
                    resp.addHeader("Access-Control-Allow-Methods", "PUT, DELETE");
                    resp.addHeader("Access-Control-Allow-Headers", "X-Requested-With");
                }
            }
            String opaque = restRequest.header("X-Opaque-Id");
            if (opaque != null) {
                resp.addHeader("X-Opaque-Id", opaque);
            }
            try {
                int contentLength = response.contentLength() + response.prefixContentLength() + response.suffixContentLength();
                resp.setContentLength(contentLength);

                ServletOutputStream out = resp.getOutputStream();
                if (response.prefixContent() != null) {
                    out.write(response.prefixContent(), 0, response.prefixContentLength());
                }
                out.write(response.content(), 0, response.contentLength());
                if (response.suffixContent() != null) {
                    out.write(response.suffixContent(), 0, response.suffixContentLength());
                }
                out.close();
            } catch (IOException e) {
                sendFailure = e;
            } finally {
                latch.countDown();
            }
        }
    }
}