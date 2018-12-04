/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.sql.action.AbstractSqlRequest;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.xpack.sql.proto.RequestInfo.CANVAS;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLI;

public abstract class AbstractSqlAction extends BaseRestHandler {

    protected AbstractSqlAction(Settings settings) {
        super(settings);
    }

    @Override
    public abstract String getName();
    
    /*
     * Parse the rest request and create a sql request out of it.
     */
    protected abstract AbstractSqlRequest initializeSqlRequest(RestRequest request) throws IOException;
    
    /*
     * Perform additional steps after the default initialization.
     */
    protected abstract RestChannelConsumer doPrepareRequest(AbstractSqlRequest sqlRequest, RestRequest request,
            NodeClient client) throws IOException;

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        AbstractSqlRequest sqlRequest = initializeSqlRequest(request);
        
        // no mode specified, default to "PLAIN"
        if (sqlRequest.requestInfo() == null) {
            sqlRequest.requestInfo(new RequestInfo(Mode.PLAIN));
        }
        
        // default to no client id, unless it's CLI or CANVAS
        if (sqlRequest.requestInfo().clientId() != null) {
            String clientId = sqlRequest.requestInfo().clientId().toLowerCase(Locale.ROOT);
            if (!clientId.equals(CLI) && !clientId.equals(CANVAS)) {
                clientId = null;
            }
            sqlRequest.requestInfo().clientId(clientId);
        }
        
        return doPrepareRequest(sqlRequest, request, client);
    }

}
