/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import io.netty.handler.codec.http.HttpHeaderNames;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public abstract class ProtoHandler implements HttpHandler, AutoCloseable {

    private static PlanExecutor planExecutor(EmbeddedModeFilterClient client) {
        return new PlanExecutor(client, new IndexResolver(client));
    }

    protected static final Logger log = ESLoggerFactory.getLogger(ProtoHandler.class.getName());

    private final TimeValue TV = TimeValue.timeValueSeconds(5);
    protected final EmbeddedModeFilterClient client;
    protected final NodeInfo info;
    protected final String clusterName;

    protected ProtoHandler(Client client) {
        NodesInfoResponse niResponse = client.admin().cluster().prepareNodesInfo("_local").clear().get(TV);
        this.client = client instanceof EmbeddedModeFilterClient ? (EmbeddedModeFilterClient) client : new EmbeddedModeFilterClient(client);
        this.client.setPlanExecutor(planExecutor(this.client));
        info = niResponse.getNodes().get(0);
        clusterName = niResponse.getClusterName().value();
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        log.debug("Received query call...");

        if ("HEAD".equals(http.getRequestMethod())) {
            http.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
            http.close();
            return;
        }

        FakeRestChannel channel = new FakeRestChannel(
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(singletonMap("error_trace", "")).build(), true, 1);
        try (DataInputStream in = new DataInputStream(http.getRequestBody())) {
            handle(channel, in);
            while (false == channel.await()) {
            }
            sendHttpResponse(http, channel.capturedResponse());
        } catch (Exception e) {
            sendHttpResponse(http, new BytesRestResponse(channel, e));
        }
    }

    protected abstract void handle(RestChannel channel, DataInput in) throws IOException;

    protected void sendHttpResponse(HttpExchange http, RestResponse response) throws IOException {
        try {
            // first do the conversion in case an exception is triggered
            if (http.getResponseHeaders().isEmpty()) {
                http.sendResponseHeaders(response.status().getStatus(), response.content().length());

                Headers headers = http.getResponseHeaders();
                headers.putIfAbsent(HttpHeaderNames.CONTENT_TYPE.toString(), singletonList(response.contentType()));
                if (response.getHeaders() != null) {
                    headers.putAll(response.getHeaders());
                }
            }
            response.content().writeTo(http.getResponseBody());
        } catch (IOException ex) {
            log.error("Caught error while trying to catch error", ex);
        } finally {
            http.close();
        }
    }

    @Override
    public void close() {
        // no-op
    }
}