/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.test.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public abstract class ProtoHandler<R> implements HttpHandler, AutoCloseable {

    protected final static Logger log = ESLoggerFactory.getLogger(ProtoHandler.class.getName());
    private final TimeValue TV = TimeValue.timeValueSeconds(5);
    protected final NodeInfo info;
    protected final String clusterName;
    private final IOFunction<DataInput, String> headerReader;
    private final IOFunction<R, BytesReference> toProto;
    
    protected ProtoHandler(Client client, IOFunction<DataInput, String> headerReader, IOFunction<R, BytesReference> toProto) {
        NodesInfoResponse niResponse = client.admin().cluster().prepareNodesInfo("_local").clear().get(TV);
        info = niResponse.getNodes().get(0);
        clusterName = niResponse.getClusterName().value();

        this.headerReader = headerReader;
        this.toProto = toProto;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        log.debug("Received query call...");

        try (DataInputStream in = new DataInputStream(http.getRequestBody())) {
            String msg = headerReader.apply(in);
            if (msg != null) {
                http.sendResponseHeaders(RestStatus.BAD_REQUEST.getStatus(), -1);
                http.close();
                return;
            }
            handle(http, in);
        } catch (Exception ex) {
            fail(http, ex);
        }
    }

    protected abstract void handle(HttpExchange http, DataInput in) throws IOException;

    protected void sendHttpResponse(HttpExchange http, R response) throws IOException {
        // first do the conversion in case an exception is triggered
        BytesReference data = toProto.apply(response);
        if (http.getResponseHeaders().isEmpty()) {
            http.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
        }
        data.writeTo(http.getResponseBody());
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
    public void close() throws Exception {
        // no-op
    }
}