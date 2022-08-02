/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.test.ESTestCase;

public final class SharedGroupFactoryTests extends ESTestCase {

    public void testSharedEventLoops() throws Exception {
        SharedGroupFactory sharedGroupFactory = new SharedGroupFactory(Settings.EMPTY);

        SharedGroupFactory.SharedGroup httpGroup = sharedGroupFactory.getHttpGroup();
        SharedGroupFactory.SharedGroup transportGroup = sharedGroupFactory.getTransportGroup();

        try {
            assertSame(httpGroup.getLowLevelGroup(), transportGroup.getLowLevelGroup());
        } finally {
            httpGroup.shutdown();
            assertFalse(httpGroup.getLowLevelGroup().isShuttingDown());
            assertFalse(transportGroup.getLowLevelGroup().isShuttingDown());
            assertFalse(transportGroup.getLowLevelGroup().isTerminated());
            assertFalse(transportGroup.getLowLevelGroup().terminationFuture().isDone());
            transportGroup.shutdown();
            assertTrue(httpGroup.getLowLevelGroup().isShuttingDown());
            assertTrue(transportGroup.getLowLevelGroup().isShuttingDown());
            assertTrue(transportGroup.getLowLevelGroup().isTerminated());
            assertTrue(transportGroup.getLowLevelGroup().terminationFuture().isDone());
        }
    }

    public void testNonSharedEventLoops() throws Exception {
        Settings settings = Settings.builder()
            .put(Netty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT.getKey(), randomIntBetween(1, 10))
            .build();
        SharedGroupFactory sharedGroupFactory = new SharedGroupFactory(settings);
        SharedGroupFactory.SharedGroup httpGroup = sharedGroupFactory.getHttpGroup();
        SharedGroupFactory.SharedGroup transportGroup = sharedGroupFactory.getTransportGroup();

        try {
            assertNotSame(httpGroup.getLowLevelGroup(), transportGroup.getLowLevelGroup());
        } finally {
            httpGroup.shutdown();
            assertTrue(httpGroup.getLowLevelGroup().isShuttingDown());
            assertTrue(httpGroup.getLowLevelGroup().isTerminated());
            assertTrue(httpGroup.getLowLevelGroup().terminationFuture().isDone());
            assertFalse(transportGroup.getLowLevelGroup().isShuttingDown());
            assertFalse(transportGroup.getLowLevelGroup().isTerminated());
            assertFalse(transportGroup.getLowLevelGroup().terminationFuture().isDone());
            transportGroup.shutdown();
            assertTrue(transportGroup.getLowLevelGroup().isShuttingDown());
            assertTrue(transportGroup.getLowLevelGroup().isTerminated());
            assertTrue(transportGroup.getLowLevelGroup().terminationFuture().isDone());
        }
    }
}
