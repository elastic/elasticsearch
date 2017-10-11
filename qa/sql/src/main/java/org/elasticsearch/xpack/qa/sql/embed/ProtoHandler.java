/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

public abstract class ProtoHandler implements HttpHandler, AutoCloseable {
    private static PlanExecutor planExecutor(EmbeddedModeFilterClient client) {
        Supplier<ClusterState> clusterStateSupplier =
                () -> client.admin().cluster().prepareState().get(timeValueMinutes(1)).getState();
        return new PlanExecutor(client, clusterStateSupplier, EsCatalog::new);
    }

    protected static final Logger log = ESLoggerFactory.getLogger(ProtoHandler.class.getName());

    private final TimeValue TV = TimeValue.timeValueSeconds(5);
    protected final EmbeddedModeFilterClient client;
    protected final NodeInfo info;
    protected final String clusterName;

    protected ProtoHandler(Client client) {
        NodesInfoResponse niResponse = client.admin().cluster().prepareNodesInfo("_local").clear().get(TV);
        this.client = !(client instanceof EmbeddedModeFilterClient) ? new EmbeddedModeFilterClient(client) : (EmbeddedModeFilterClient) client;
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

        try (DataInputStream in = new DataInputStream(http.getRequestBody())) {
            handle(http, in);
        } catch (Exception ex) {
            fail(http, ex);
        }
    }

    protected abstract void handle(HttpExchange http, DataInput in) throws IOException;

    protected void sendHttpResponse(HttpExchange http, BytesReference response) throws IOException {
        // first do the conversion in case an exception is triggered
        if (http.getResponseHeaders().isEmpty()) {
            http.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
        }
        response.writeTo(http.getResponseBody());
        http.close();
    }

    protected void fail(HttpExchange http, Exception ex) {
        log.error("Caught error while transmitting response", ex);
        try {
            // the error conversion has failed, halt
            if (http.getResponseHeaders().isEmpty()) {
                http.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
            }
        } catch (IOException ioEx) {
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