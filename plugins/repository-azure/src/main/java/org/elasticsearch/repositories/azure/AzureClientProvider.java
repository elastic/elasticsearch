/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.logging.ClientLogger;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.azure.executors.PrivilegedExecutor;
import org.elasticsearch.repositories.azure.executors.ReactorScheduledExecutorService;
import org.elasticsearch.threadpool.ThreadPool;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import static org.elasticsearch.repositories.azure.AzureRepositoryPlugin.NETTY_EVENT_LOOP_THREAD_POOL_NAME;
import static org.elasticsearch.repositories.azure.AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME;

class AzureClientProvider extends AbstractLifecycleComponent {
    private static final TimeValue DEFAULT_CONNECTION_TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final TimeValue DEFAULT_MAX_CONNECTION_IDLE_TIME = TimeValue.timeValueSeconds(60);
    private static final int DEFAULT_MAX_CONNECTIONS = 50;
    private static final int DEFAULT_EVENT_LOOP_THREAD_COUNT = Math.min(Runtime.getRuntime().availableProcessors(), 8) * 2;

    static final Setting<String> EVENT_LOOP_EXECUTOR = Setting.simpleString(
        "repository.azure.http_client.event_loop_executor_name",
        NETTY_EVENT_LOOP_THREAD_POOL_NAME,
        Setting.Property.NodeScope);

    static final Setting<Integer> EVENT_LOOP_THREAD_COUNT = Setting.intSetting(
        "repository.azure.http_client.event_loop_executor_thread_count",
        DEFAULT_EVENT_LOOP_THREAD_COUNT,
        Setting.Property.NodeScope);

    static final Setting<Integer> MAX_OPEN_CONNECTIONS = Setting.intSetting(
        "repository.azure.http_client.max_open_connections",
        DEFAULT_MAX_CONNECTIONS,
        Setting.Property.NodeScope);

    static final Setting<TimeValue> OPEN_CONNECTION_TIMEOUT = Setting.timeSetting(
        "repository.azure.http_client.connection_timeout",
        DEFAULT_CONNECTION_TIMEOUT,
        Setting.Property.NodeScope);

    static final Setting<TimeValue> MAX_IDLE_TIME = Setting.timeSetting(
        "repository.azure.http_client.connection_max_idle_time",
        DEFAULT_MAX_CONNECTION_IDLE_TIME,
        Setting.Property.NodeScope);

    static final Setting<String> REACTOR_SCHEDULER_EXECUTOR_NAME = Setting.simpleString(
        "repository.azure.http_client.reactor_executor_name",
        REPOSITORY_THREAD_POOL_NAME,
        Setting.Property.NodeScope);

    private final ThreadPool threadPool;
    private final String reactorExecutorName;
    private final EventLoopGroup eventLoopGroup;
    private final ConnectionProvider connectionProvider;
    private final ByteBufAllocator byteBufAllocator;
    private final ClientLogger clientLogger = new ClientLogger(AzureClientProvider.class);
    private volatile boolean closed = false;

    AzureClientProvider(ThreadPool threadPool,
                        String reactorExecutorName,
                        EventLoopGroup eventLoopGroup,
                        ConnectionProvider connectionProvider,
                        ByteBufAllocator byteBufAllocator) {
        this.threadPool = threadPool;
        this.reactorExecutorName = reactorExecutorName;
        this.eventLoopGroup = eventLoopGroup;
        this.connectionProvider = connectionProvider;
        this.byteBufAllocator = byteBufAllocator;
    }

    static int eventLoopThreadsFromSettings(Settings settings) {
        return EVENT_LOOP_THREAD_COUNT.get(settings);
    }

    static AzureClientProvider create(ThreadPool threadPool, Settings settings) {
        final ExecutorService executorService;
        try {
            executorService = threadPool.executor(EVENT_LOOP_EXECUTOR.get(settings));
        } catch (IllegalArgumentException e) {
            throw new SettingsException("Unable to find executor [" + EVENT_LOOP_EXECUTOR.get(settings) + "]");
        }
        // Most of the code that needs special permissions (i.e. jackson serializers generation) is executed
        // in the event loop executor. That's the reason why we should provide an executor that allows the
        // execution of privileged code
        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(eventLoopThreadsFromSettings(settings),
            new PrivilegedExecutor(executorService));

        final TimeValue openConnectionTimeout = OPEN_CONNECTION_TIMEOUT.get(settings);
        final TimeValue maxIdleTime = MAX_IDLE_TIME.get(settings);

        ConnectionProvider provider =
            ConnectionProvider.builder("azure-sdk-connection-pool")
                .maxConnections(MAX_OPEN_CONNECTIONS.get(settings))
                .pendingAcquireTimeout(Duration.ofMillis(openConnectionTimeout.millis()))
                .maxIdleTime(Duration.ofMillis(maxIdleTime.millis()))
                .build();

        ByteBufAllocator pooledByteBufAllocator = createByteBufAllocator();

        String reactorExecutorName = REACTOR_SCHEDULER_EXECUTOR_NAME.get(settings);

        return new AzureClientProvider(threadPool, reactorExecutorName, eventLoopGroup, provider, pooledByteBufAllocator);
    }

    private static ByteBufAllocator createByteBufAllocator() {
        int nHeapArena = PooledByteBufAllocator.defaultNumHeapArena();
        int pageSize = PooledByteBufAllocator.defaultPageSize();
        int maxOrder = PooledByteBufAllocator.defaultMaxOrder();
        int tinyCacheSize = PooledByteBufAllocator.defaultTinyCacheSize();
        int smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
        int normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
        boolean useCacheForAllThreads = PooledByteBufAllocator.defaultUseCacheForAllThreads();

        return new PooledByteBufAllocator(false,
            nHeapArena,
            0,
            pageSize,
            maxOrder,
            tinyCacheSize,
            smallCacheSize,
            normalCacheSize,
            useCacheForAllThreads);
    }

    AzureBlobServiceClient createClient(AzureStorageSettings settings,
                                        LocationMode locationMode,
                                        RequestRetryOptions retryOptions,
                                        ProxyOptions proxyOptions,
                                        BiConsumer<String, URL> successfulRequestConsumer) {
        if (closed) {
            throw new IllegalArgumentException("AzureClientProvider is already closed");
        }

        reactor.netty.http.client.HttpClient nettyHttpClient = reactor.netty.http.client.HttpClient.create(connectionProvider);
        nettyHttpClient = nettyHttpClient
            .port(80)
            .wiretap(false);

        nettyHttpClient = nettyHttpClient.tcpConfiguration(tcpClient -> {
            tcpClient = tcpClient.runOn(eventLoopGroup);
            tcpClient = tcpClient.option(ChannelOption.ALLOCATOR, byteBufAllocator);
            return tcpClient;
        });

        final HttpClient httpClient = new NettyAsyncHttpClientBuilder(nettyHttpClient)
            .disableBufferCopy(false)
            .proxy(proxyOptions)
            .build();

        final String connectionString = settings.getConnectString();

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .httpClient(httpClient)
            .retryOptions(retryOptions);

        if (successfulRequestConsumer != null) {
            builder.addPolicy(new SuccessfulRequestTracker(successfulRequestConsumer));
        }

        if (locationMode.isSecondary()) {
            // TODO: maybe extract this logic so we don't need to have a client logger around?
            StorageConnectionString storageConnectionString = StorageConnectionString.create(connectionString, clientLogger);
            String secondaryUri = storageConnectionString.getBlobEndpoint().getSecondaryUri();
            if (secondaryUri == null) {
                throw new IllegalArgumentException("Unable to configure an AzureClient using a secondary location without a secondary " +
                    "endpoint");
            }

            builder.endpoint(secondaryUri);
        }

        BlobServiceClient blobServiceClient = SocketAccess.doPrivilegedException(builder::buildClient);
        BlobServiceAsyncClient asyncClient = SocketAccess.doPrivilegedException(builder::buildAsyncClient);
        return new AzureBlobServiceClient(blobServiceClient, asyncClient);
    }

    @Override
    protected void doStart() {
        ReactorScheduledExecutorService executorService = new ReactorScheduledExecutorService(threadPool, reactorExecutorName);

        // The only way to configure the schedulers used by the SDK is to inject a new global factory. This is a bit ugly...
        // See https://github.com/Azure/azure-sdk-for-java/issues/17272 for a feature request to avoid this need.
        Schedulers.setFactory(new Schedulers.Factory() {
            @Override
            public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
                return Schedulers.fromExecutor(executorService);
            }

            @Override
            public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
                return Schedulers.fromExecutor(executorService);
            }

            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                return Schedulers.fromExecutor(executorService);
            }

            @Override
            public Scheduler newSingle(ThreadFactory threadFactory) {
                return Schedulers.fromExecutor(executorService);
            }
        });
    }

    @Override
    protected void doStop() {
        closed = true;
        connectionProvider.dispose();
        eventLoopGroup.shutdownGracefully();
        Schedulers.resetFactory();
    }

    @Override
    protected void doClose() throws IOException {}

    private static final class SuccessfulRequestTracker implements HttpPipelinePolicy {
        private final BiConsumer<String, URL> onSuccessfulRequest;
        private final Logger logger = LogManager.getLogger(SuccessfulRequestTracker.class);

        private SuccessfulRequestTracker(BiConsumer<String, URL> onSuccessfulRequest) {
            this.onSuccessfulRequest = onSuccessfulRequest;
        }

        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
            return next.process()
                .doOnSuccess(httpResponse -> trackSuccessfulRequest(context.getHttpRequest(), httpResponse));
        }

        private void trackSuccessfulRequest(HttpRequest httpRequest, HttpResponse httpResponse) {
            HttpMethod method = httpRequest.getHttpMethod();
            if (httpResponse != null && method != null && httpResponse.getStatusCode() > 199 && httpResponse.getStatusCode() <= 299) {
                try {
                    onSuccessfulRequest.accept(method.name(), httpRequest.getUrl());
                } catch (Exception e) {
                    logger.warn("Unable to notify a successful request", e);
                }
            }
        }
    }
}
