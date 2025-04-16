/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;

public class Netty4Plugin extends Plugin implements NetworkPlugin {

    public static final String NETTY_TRANSPORT_NAME = "netty4";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty4";
    public static final Setting<Integer> SETTING_HTTP_WORKER_COUNT = Setting.intSetting(
        "http.netty.worker_count",
        0,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE = byteSizeSetting(
        "http.netty.receive_predictor_size",
        ByteSizeValue.of(64, ByteSizeUnit.KB),
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> WORKER_COUNT = new Setting<>(
        "transport.netty.worker_count",
        (s) -> Integer.toString(EsExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"),
        Setting.Property.NodeScope
    );
    private static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = byteSizeSetting(
        "transport.netty.receive_predictor_size",
        ByteSizeValue.of(64, ByteSizeUnit.KB),
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX = byteSizeSetting(
        "transport.netty.receive_predictor_max",
        NETTY_RECEIVE_PREDICTOR_SIZE,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN = byteSizeSetting(
        "transport.netty.receive_predictor_min",
        NETTY_RECEIVE_PREDICTOR_SIZE,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> NETTY_BOSS_COUNT = intSetting("transport.netty.boss_count", 1, 1, Setting.Property.NodeScope);
    /*
     * Size in bytes of an individual message received by io.netty.handler.codec.MessageAggregator which accumulates the content for an
     * HTTP request. This number is used for estimating the maximum number of allowed buffers before the MessageAggregator's internal
     * collection of buffers is resized.
     *
     * By default we assume the Ethernet MTU (1500 bytes) but users can override it with a system property.
     */
    private static final ByteSizeValue MTU = ByteSizeValue.ofBytes(Long.parseLong(System.getProperty("es.net.mtu", "1500")));
    private static final String SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS = "http.netty.max_composite_buffer_components";
    public static final Setting<Integer> SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS = new Setting<>(
        SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
        (s) -> {
            ByteSizeValue maxContentLength = SETTING_HTTP_MAX_CONTENT_LENGTH.get(s);
            /*
             * Netty accumulates buffers containing data from all incoming network packets that make up one HTTP request in an instance of
             * io.netty.buffer.CompositeByteBuf (think of it as a buffer of buffers). Once its capacity is reached, the buffer will iterate
             * over its individual entries and put them into larger buffers (see io.netty.buffer.CompositeByteBuf#consolidateIfNeeded()
             * for implementation details). We want to to resize that buffer because this leads to additional garbage on the heap and also
             * increases the application's native memory footprint (as direct byte buffers hold their contents off-heap).
             *
             * With this setting we control the CompositeByteBuf's capacity (which is by default 1024, see
             * io.netty.handler.codec.MessageAggregator#DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS). To determine a proper default capacity for
             * that buffer, we need to consider that the upper bound for the size of HTTP requests is determined by `maxContentLength`. The
             * number of buffers that are needed depend on how often Netty reads network packets which depends on the network type (MTU).
             * We assume here that Elasticsearch receives HTTP requests via an Ethernet connection which has a MTU of 1500 bytes.
             *
             * Note that we are *not* pre-allocating any memory based on this setting but rather determine the CompositeByteBuf's capacity.
             * The tradeoff is between less (but larger) buffers that are contained in the CompositeByteBuf and more (but smaller) buffers.
             * With the default max content length of 100MB and a MTU of 1500 bytes we would allow 69905 entries.
             */
            long maxBufferComponentsEstimate = Math.round((double) (maxContentLength.getBytes() / MTU.getBytes()));
            // clamp value to the allowed range
            long maxBufferComponents = Math.max(2, Math.min(maxBufferComponentsEstimate, Integer.MAX_VALUE));
            return String.valueOf(maxBufferComponents);
            // Netty's CompositeByteBuf implementation does not allow less than two components.
        },
        s -> Setting.parseInt(s, 2, Integer.MAX_VALUE, SETTING_KEY_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS),
        Setting.Property.NodeScope
    );

    private final SetOnce<SharedGroupFactory> groupFactory = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            SETTING_HTTP_WORKER_COUNT,
            SETTING_HTTP_NETTY_RECEIVE_PREDICTOR_SIZE,
            WORKER_COUNT,
            NETTY_RECEIVE_PREDICTOR_SIZE,
            NETTY_RECEIVE_PREDICTOR_MIN,
            NETTY_RECEIVE_PREDICTOR_MAX,
            NETTY_BOSS_COUNT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            // here we set the netty4 transport and http transport as the default. This is a set once setting
            // ie. if another plugin does that as well the server will fail - only one default network can exist!
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), NETTY_HTTP_TRANSPORT_NAME)
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), NETTY_TRANSPORT_NAME)
            .build();
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService
    ) {
        return Collections.singletonMap(
            NETTY_TRANSPORT_NAME,
            () -> new Netty4Transport(
                settings,
                TransportVersion.current(),
                threadPool,
                networkService,
                pageCacheRecycler,
                namedWriteableRegistry,
                circuitBreakerService,
                getSharedGroupFactory(settings)
            )
        );
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4HttpServerTransport(
                settings,
                networkService,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings),
                tracer,
                TLSConfig.noTLS(),
                null,
                null
            ) {
                @Override
                protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
                    perRequestThreadContext.accept(restRequest.getHttpRequest(), threadContext);
                }
            }
        );
    }

    private SharedGroupFactory getSharedGroupFactory(Settings settings) {
        SharedGroupFactory factory = this.groupFactory.get();
        if (factory != null) {
            assert factory.getSettings().equals(settings) : "Different settings than originally provided";
            return factory;
        } else {
            this.groupFactory.set(new SharedGroupFactory(settings));
            return this.groupFactory.get();
        }
    }
}
