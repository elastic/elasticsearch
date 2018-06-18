/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.http.netty4;

import org.elasticsearch.transport.netty4.Netty4OpenChannelsHandler;

import static org.mockito.Mockito.mock;

/** Allows setting a mock into Netty3HttpServerTransport */
public class Netty4HttpMockUtil {

    /**
     * We don't really need to start Netty for these tests, but we can't create a pipeline
     * with a null handler. So we set it to a mock for tests.
     */
    public static void setOpenChannelsHandlerToMock(Netty4HttpServerTransport transport) throws Exception {
        transport.serverOpenChannels = mock(Netty4OpenChannelsHandler.class);
    }

}