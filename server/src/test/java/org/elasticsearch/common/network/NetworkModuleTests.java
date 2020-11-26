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

package org.elasticsearch.common.network;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class NetworkModuleTests extends ESTestCase {
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(NetworkModuleTests.class.getName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    static class FakeHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {
        @Override
        protected void doStart() {}
        @Override
        protected void doStop() {}
        @Override
        protected void doClose() {}
        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }
        @Override
        public HttpInfo info() {
            return null;
        }
        @Override
        public HttpStats stats() {
            return null;
        }
    }

    public void testRegisterTransport() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "custom").build();
        Supplier<Transport> custom = () -> null; // content doesn't matter we check reference equality
        NetworkPlugin plugin = new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool,
                                                                  PageCacheRecycler pageCacheRecycler,
                                                                  CircuitBreakerService circuitBreakerService,
                                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                                  NetworkService networkService) {
                return Collections.singletonMap("custom", custom);
            }
        };
        NetworkModule module = newNetworkModule(settings, plugin);
        assertSame(custom, module.getTransportSupplier());
    }

    public void testRegisterHttpTransport() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;

        NetworkModule module = newNetworkModule(settings, new NetworkPlugin() {
            @Override
            public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool,
                                                                                BigArrays bigArrays,
                                                                                PageCacheRecycler pageCacheRecycler,
                                                                                CircuitBreakerService circuitBreakerService,
                                                                                NamedXContentRegistry xContentRegistry,
                                                                                NetworkService networkService,
                                                                                HttpServerTransport.Dispatcher requestDispatcher,
                                                                                ClusterSettings clusterSettings) {
                return Collections.singletonMap("custom", custom);
            }
        });
        assertSame(custom, module.getHttpServerTransportSupplier());

        settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        NetworkModule newModule = newNetworkModule(settings);
        expectThrows(IllegalStateException.class, () -> newModule.getHttpServerTransportSupplier());
    }

    public void testOverrideDefault() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom")
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), "local")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "default_custom").build();
        Supplier<Transport> customTransport = () -> null;  // content doesn't matter we check reference equality
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;
        Supplier<HttpServerTransport> def = FakeHttpTransport::new;
        NetworkModule module = newNetworkModule(settings, new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool,
                                                                  PageCacheRecycler pageCacheRecycler,
                                                                  CircuitBreakerService circuitBreakerService,
                                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                                  NetworkService networkService) {
                return Collections.singletonMap("default_custom", customTransport);
            }

            @Override
            public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool,
                                                                                BigArrays bigArrays,
                                                                                PageCacheRecycler pageCacheRecycler,
                                                                                CircuitBreakerService circuitBreakerService,
                                                                                NamedXContentRegistry xContentRegistry,
                                                                                NetworkService networkService,
                                                                                HttpServerTransport.Dispatcher requestDispatcher,
                                                                                ClusterSettings clusterSettings) {
                Map<String, Supplier<HttpServerTransport>> supplierMap = new HashMap<>();
                supplierMap.put("custom", custom);
                supplierMap.put("default_custom", def);
                return supplierMap;
            }
        });
        assertSame(custom, module.getHttpServerTransportSupplier());
        assertSame(customTransport, module.getTransportSupplier());
    }

    public void testDefaultKeys() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), "default_custom").build();
        Supplier<HttpServerTransport> custom = FakeHttpTransport::new;
        Supplier<HttpServerTransport> def = FakeHttpTransport::new;
        Supplier<Transport> customTransport = () -> null;
        NetworkModule module = newNetworkModule(settings, new NetworkPlugin() {
            @Override
            public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool,
                                                                  PageCacheRecycler pageCacheRecycler,
                                                                  CircuitBreakerService circuitBreakerService,
                                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                                  NetworkService networkService) {
                return Collections.singletonMap("default_custom", customTransport);
            }

            @Override
            public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool,
                                                                                BigArrays bigArrays,
                                                                                PageCacheRecycler pageCacheRecycler,
                                                                                CircuitBreakerService circuitBreakerService,
                                                                                NamedXContentRegistry xContentRegistry,
                                                                                NetworkService networkService,
                                                                                HttpServerTransport.Dispatcher requestDispatcher,
                                                                                ClusterSettings clusterSettings) {
                Map<String, Supplier<HttpServerTransport>> supplierMap = new HashMap<>();
                supplierMap.put("custom", custom);
                supplierMap.put("default_custom", def);
                return supplierMap;
            }
        });

        assertSame(def, module.getHttpServerTransportSupplier());
        assertSame(customTransport, module.getTransportSupplier());
    }

    public void testRegisterInterceptor() {
        Settings settings = Settings.builder()
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        AtomicInteger called = new AtomicInteger(0);

        TransportInterceptor interceptor = new TransportInterceptor() {
            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                            boolean forceExecution,
                                                                                            TransportRequestHandler<T> actualHandler) {
                called.incrementAndGet();
                if ("foo/bar/boom".equals(action)) {
                    assertTrue(forceExecution);
                } else {
                    assertFalse(forceExecution);
                }
                return actualHandler;
            }
        };
        NetworkModule module = newNetworkModule(settings, new NetworkPlugin() {
                @Override
                public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
                                                                           ThreadContext threadContext) {
                    assertNotNull(threadContext);
                    return Collections.singletonList(interceptor);
                }
            });

        TransportInterceptor transportInterceptor = module.getTransportInterceptor();
        assertEquals(0, called.get());
        transportInterceptor.interceptHandler("foo/bar/boom", null, true, null);
        assertEquals(1, called.get());
        transportInterceptor.interceptHandler("foo/baz/boom", null, false, null);
        assertEquals(2, called.get());
        assertTrue(transportInterceptor instanceof  NetworkModule.CompositeTransportInterceptor);
        assertEquals(((NetworkModule.CompositeTransportInterceptor)transportInterceptor).transportInterceptors.size(), 1);
        assertSame(((NetworkModule.CompositeTransportInterceptor)transportInterceptor).transportInterceptors.get(0), interceptor);

        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> {
            newNetworkModule(settings, new NetworkPlugin() {
                @Override
                public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
                                                                           ThreadContext threadContext) {
                    assertNotNull(threadContext);
                    return Collections.singletonList(null);
                }
            });
        });
        assertEquals("interceptor must not be null", nullPointerException.getMessage());
    }

    private NetworkModule newNetworkModule(Settings settings, NetworkPlugin... plugins) {
        return new NetworkModule(settings, Arrays.asList(plugins), threadPool, null, null, null, null,
            xContentRegistry(), null, new NullDispatcher(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }
}
