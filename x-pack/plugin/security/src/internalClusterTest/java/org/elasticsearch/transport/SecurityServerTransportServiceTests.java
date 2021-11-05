/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.transport;

import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;

// this class sits in org.elasticsearch.transport so that TransportService.requestHandlers is visible
public class SecurityServerTransportServiceTests extends SecurityIntegTestCase {

    public void testSecurityServerTransportServiceWrapsAllHandlers() {
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            @SuppressWarnings("rawtypes")
            RequestHandlerRegistry handler = transportService.transport.getRequestHandlers()
                .getHandler(TransportService.HANDSHAKE_ACTION_NAME);
            assertEquals(
                "handler not wrapped by "
                    + SecurityServerTransportInterceptor.ProfileSecuredRequestHandler.class
                    + "; do all the handler registration methods have overrides?",
                handler.toString(),
                "ProfileSecuredRequestHandler{action='"
                    + handler.getAction()
                    + "', executorName='"
                    + handler.getExecutor()
                    + "', forceExecution="
                    + handler.isForceExecution()
                    + "}"
            );
        }
    }
}
