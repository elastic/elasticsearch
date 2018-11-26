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

package org.elasticsearch.discovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnicastConfiguredHostsResolverTests extends ESTestCase {

    private List<TransportAddress> transportAddresses;
    private UnicastConfiguredHostsResolver unicastConfiguredHostsResolver;
    private ThreadPool threadPool;

    @Before
    public void startResolver() {
        threadPool = new TestThreadPool("node");
        transportAddresses = new ArrayList<>();

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        unicastConfiguredHostsResolver
            = new UnicastConfiguredHostsResolver("test_node", Settings.EMPTY, transportService, hostsResolver -> transportAddresses);
        unicastConfiguredHostsResolver.start();
    }

    @After
    public void stopResolver() {
        unicastConfiguredHostsResolver.stop();
        threadPool.shutdown();
    }

    public void testResolvesAddressesInBackgroundAndIgnoresConcurrentCalls() throws Exception {
        final AtomicReference<List<TransportAddress>> resolvedAddressesRef = new AtomicReference<>();
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(1);

        final int addressCount = randomIntBetween(0, 5);
        for (int i = 0; i < addressCount; i++) {
            transportAddresses.add(buildNewFakeTransportAddress());
        }

        unicastConfiguredHostsResolver.resolveConfiguredHosts(resolvedAddresses -> {
            try {
                assertTrue(startLatch.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            resolvedAddressesRef.set(resolvedAddresses);
            endLatch.countDown();
        });

        unicastConfiguredHostsResolver.resolveConfiguredHosts(resolvedAddresses -> {
            throw new AssertionError("unexpected concurrent resolution");
        });

        assertThat(resolvedAddressesRef.get(), nullValue());
        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
        assertThat(resolvedAddressesRef.get(), equalTo(transportAddresses));
    }
}
