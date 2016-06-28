/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport.netty;

import org.elasticsearch.common.netty.OpenChannelsHandler;

import static org.mockito.Mockito.mock;

/** Allows setting a mock into NettyTransport */
public class NettyMockUtil {
    /**
     * We don't really need to start Netty for these tests, but we can't create a pipeline
     * with a null handler. So we set it to a mock for tests.
     */
    public static void setOpenChannelsHandlerToMock(NettyTransport transport) throws Exception {
        transport.serverOpenChannels = mock(OpenChannelsHandler.class);
    }
}
