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

package org.elasticsearch.transport;

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
