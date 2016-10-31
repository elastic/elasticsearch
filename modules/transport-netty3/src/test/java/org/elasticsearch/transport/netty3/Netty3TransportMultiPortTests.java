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
package org.elasticsearch.transport.netty3;

import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class Netty3TransportMultiPortTests extends ESTestCase {

    private String host;

    @Before
    public void setup() {
        if (NetworkUtils.SUPPORTS_V6 && randomBoolean()) {
            host = "::1";
        } else {
            host = "127.0.0.1";
        }
    }

    public void testThatNettyCanBindToMultiplePorts() throws Exception {
        Settings settings = Settings.builder()
                .put("network.host", host)
                .put(TransportSettings.PORT.getKey(), 22) // will not actually bind to this
                .put("transport.profiles.default.port", 0)
                .put("transport.profiles.client1.port", 0)
                .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport<?> transport = startTransport(settings, threadPool)) {
            assertEquals(1, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatDefaultProfileInheritsFromStandardSettings() throws Exception {
        Settings settings = Settings.builder()
                .put("network.host", host)
                .put(TransportSettings.PORT.getKey(), 0)
                .put("transport.profiles.client1.port", 0)
                .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport<?> transport = startTransport(settings, threadPool)) {
            assertEquals(1, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatProfileWithoutPortSettingsFails() throws Exception {

        Settings settings = Settings.builder()
                .put("network.host", host)
                .put(TransportSettings.PORT.getKey(), 0)
                .put("transport.profiles.client1.whatever", "foo")
                .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport<?> transport = startTransport(settings, threadPool)) {
            assertEquals(0, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatDefaultProfilePortOverridesGeneralConfiguration() throws Exception {
        Settings settings = Settings.builder()
                .put("network.host", host)
                .put(TransportSettings.PORT.getKey(), 22) // will not actually bind to this
                .put("transport.profiles.default.port", 0)
                .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport<?> transport = startTransport(settings, threadPool)) {
            assertEquals(0, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    public void testThatProfileWithoutValidNameIsIgnored() throws Exception {
        Settings settings = Settings.builder()
                .put("network.host", host)
                .put(TransportSettings.PORT.getKey(), 0)
                // mimics someone trying to define a profile for .local which is the profile for a node request to itself
                .put("transport.profiles." + TransportService.DIRECT_RESPONSE_PROFILE + ".port", 22) // will not actually bind to this
                .put("transport.profiles..port", 23) // will not actually bind to this
                .build();

        ThreadPool threadPool = new TestThreadPool("tst");
        try (TcpTransport<?> transport = startTransport(settings, threadPool)) {
            assertEquals(0, transport.profileBoundAddresses().size());
            assertEquals(1, transport.boundAddress().boundAddresses().length);
        } finally {
            terminate(threadPool);
        }
    }

    private TcpTransport<?> startTransport(Settings settings, ThreadPool threadPool) {
        BigArrays bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
        TcpTransport<?> transport = new Netty3Transport(settings, threadPool, new NetworkService(settings, Collections.emptyList()),
            bigArrays, new NamedWriteableRegistry(Collections.emptyList()), new NoneCircuitBreakerService());
        transport.start();

        assertThat(transport.lifecycleState(), is(Lifecycle.State.STARTED));
        return transport;
    }
}
