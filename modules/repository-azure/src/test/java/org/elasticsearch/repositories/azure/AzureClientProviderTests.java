/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolverGroup;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.transport.TransportConfig;

import com.azure.storage.common.policy.RequestRetryOptions;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class AzureClientProviderTests extends ESTestCase {
    private static final AzureClientProvider.RequestMetricsHandler NOOP_HANDLER = (purpose, request, metrics) -> {};

    private ThreadPool threadPool;
    private AzureClientProvider azureClientProvider;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        azureClientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
    }

    @After
    public void tearDownThreadPool() {
        azureClientProvider.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCanCreateAClientWithSecondaryLocation() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey1"));

        final String endpoint;
        if (randomBoolean()) {
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;"
                + "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
        } else {
            endpoint = "core.windows.net";
        }

        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get("azure1");
        assertNotNull(storageSettings);

        LocationMode locationMode = LocationMode.SECONDARY_ONLY;
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions();
        azureClientProvider.createClient(
            null,
            "",
            storageSettings,
            locationMode,
            requestRetryOptions,
            null,
            NOOP_HANDLER,
            randomFrom(OperationPurpose.values())
        );
    }

    public void testCanNotCreateAClientWithSecondaryLocationWithoutAProperEndpoint() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey1"));

        final String endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net";

        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get("azure1");
        assertNotNull(storageSettings);

        LocationMode locationMode = LocationMode.SECONDARY_ONLY;
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions();
        expectThrows(
            IllegalArgumentException.class,
            () -> azureClientProvider.createClient(
                null,
                "",
                storageSettings,
                locationMode,
                requestRetryOptions,
                null,
                NOOP_HANDLER,
                randomFrom(OperationPurpose.values())
            )
        );
    }

    public void testFallsBackToDefaultMaxConnections() {
        final var defaultMaxConnections = randomIntBetween(1, 200);
        final var clientName = "someClientName";
        final var account = "someAccount";
        final var storageSettings = createStorageSettings(
            clientName,
            account,
            Settings.builder().put("repository.azure.http_client.max_open_connections", defaultMaxConnections)
        );

        createClient(clientName, storageSettings);

        final var key = new AzureClientProvider.ConnectionProviderKey(null, clientName, account);
        assertEquals(
            defaultMaxConnections,
            azureClientProvider.getConnectionProvidersCache().get(key).connectionProvider().maxConnections()
        );
    }

    public void testPerClientMaxConnectionsTakePrecedenceOverDefault() {
        final var defaultMaxConnections = randomIntBetween(1, 200);
        final int perClientMaxConnections = randomValueOtherThan(defaultMaxConnections, () -> randomIntBetween(1, 200));
        final var clientName = "someClientName";
        final var account = "someAccount";
        final var storageSettings = createStorageSettings(
            clientName,
            account,
            Settings.builder()
                .put("repository.azure.http_client.max_open_connections", defaultMaxConnections)
                .put(Strings.format("azure.client.%s.max_connections", clientName), perClientMaxConnections)
        );

        createClient(clientName, storageSettings);

        final var key = new AzureClientProvider.ConnectionProviderKey(null, clientName, account);
        assertEquals(
            perClientMaxConnections,
            azureClientProvider.getConnectionProvidersCache().get(key).connectionProvider().maxConnections()
        );
    }

    public void testSameClientSameConnectionProvider() {
        final var clientName = "someClientName";
        final var account = "someAccount";
        final var maxConnections = randomIntBetween(1, 200);
        final var storageSettings = createStorageSettings(clientName, account, maxConnections);
        final var key = new AzureClientProvider.ConnectionProviderKey(null, clientName, account);

        var ref = azureClientProvider.getConnectionProvidersCache().get(key);
        assertNull(ref);

        try (var client1 = createClient(clientName, storageSettings)) {
            ref = azureClientProvider.getConnectionProvidersCache().get(key);
            assertNotNull(ref);
            var connectionProvider = ref.connectionProvider();
            assertEquals(2, ref.refCount()); // 2 = (1 from being in the cache + 1 from the client)

            final var sameClientName = new String(clientName);
            final var sameAccountName = new String(account);
            final var sameStorageSettings = createStorageSettings(sameClientName, sameAccountName, maxConnections);
            final var sameKey = new AzureClientProvider.ConnectionProviderKey(null, sameClientName, sameAccountName);
            try (var client2 = createClient(sameClientName, sameStorageSettings)) {
                ref = azureClientProvider.getConnectionProvidersCache().get(sameKey);
                var sameConnectionProvider = ref.connectionProvider();
                assertSame(connectionProvider, sameConnectionProvider);
                assertEquals(3, ref.refCount()); // 3 = (2 (from before) + 1 for the client pointing at it)
            }
            assertEquals(2, ref.refCount()); // 2 because `client2` closed
        }
        assertEquals(1, ref.refCount()); // 1 from being in the cache
    }

    public void testDifferentClientsDifferentConnectionProviders() {
        final var clientName = "someClientName";
        final var account = "someAccount";
        final var maxConnections = randomIntBetween(1, 200);
        final var storageSettings = createStorageSettings(clientName, account, maxConnections);
        final var key = new AzureClientProvider.ConnectionProviderKey(null, clientName, account);

        var ref = azureClientProvider.getConnectionProvidersCache().get(key);
        assertNull(ref);

        try (var client = createClient(clientName, storageSettings)) {
            ref = azureClientProvider.getConnectionProvidersCache().get(key);
            assertNotNull(ref);
            assertEquals(2, ref.refCount()); // 2 = (1 from being in the cache + 1 from the client)

            final var otherClientName = "otherClientName";
            final var otherKey = new AzureClientProvider.ConnectionProviderKey(null, otherClientName, account);

            try (var otherClient = createClient(otherClientName, storageSettings)) {
                final var otherRef = azureClientProvider.getConnectionProvidersCache().get(otherKey);
                assertNotNull(otherRef);
                // this is a different reference and hence 2 = (1 from being in the cache + 1 from the client)
                assertEquals(2, otherRef.refCount());
                assertNotSame(ref.connectionProvider(), otherRef.connectionProvider());
            }

            // get a different client by changing the account name
            final var otherAccountName = "otherAccount";
            final var otherStorageSettings = createStorageSettings(clientName, otherAccountName, maxConnections);
            final var otherKey2 = new AzureClientProvider.ConnectionProviderKey(null, clientName, otherAccountName);
            try (var otherClient2 = createClient(clientName, otherStorageSettings)) {
                final var otherRef = azureClientProvider.getConnectionProvidersCache().get(otherKey2);
                assertNotNull(otherRef);
                // this is a different reference and hence 2 = (1 from being in the cache + 1 from the client)
                assertEquals(2, otherRef.refCount());
                assertNotSame(ref.connectionProvider(), otherRef.connectionProvider());
            }
        }
        assertEquals(1, ref.refCount()); // 1 from being in the cache
    }

    public void testDisposeLaterIsTriggeredWhenProviderIsNotReferenced() {
        final var clientName = "someClientName";
        final var account = "someAccount";
        final var maxConnections = randomIntBetween(1, 200);
        final var storageSettings = createStorageSettings(clientName, account, maxConnections);

        class RecordingAzureClientProvider extends AzureClientProvider {
            final AtomicBoolean calledDisposeLater = new AtomicBoolean();

            RecordingAzureClientProvider(
                ThreadPool threadPool,
                String reactorExecutorName,
                EventLoopGroup eventLoopGroup,
                TimeValue openConnectionTimeout,
                TimeValue maxIdleTime,
                ByteBufAllocator byteBufAllocator,
                int multipartUploadMaxConcurrency
            ) {
                super(
                    threadPool,
                    reactorExecutorName,
                    eventLoopGroup,
                    openConnectionTimeout,
                    maxIdleTime,
                    byteBufAllocator,
                    multipartUploadMaxConcurrency
                );
            }

            @Override
            ConnectionProvider buildConnectionProvider(int maxConnections) {
                return new ConnectionProvider() {
                    @Override
                    public Mono<? extends Connection> acquire(
                        TransportConfig config,
                        ConnectionObserver connectionObserver,
                        Supplier<? extends SocketAddress> remoteAddress,
                        AddressResolverGroup<?> resolverGroup
                    ) {
                        return null;
                    }

                    @Override
                    public Mono<Void> disposeLater() {
                        calledDisposeLater.set(true);
                        return Mono.empty();
                    }
                };
            }

            public boolean getCalledDisposeLater() {
                return calledDisposeLater.get();
            }
        }

        final var recordingAzureClientProvider = new RecordingAzureClientProvider(threadPool, null, null, null, null, null, 0);

        assertFalse(recordingAzureClientProvider.getCalledDisposeLater());

        final var key = new AzureClientProvider.ConnectionProviderKey(null, clientName, account);

        try (var client = createClient(recordingAzureClientProvider, clientName, storageSettings)) {
            assertFalse(recordingAzureClientProvider.getCalledDisposeLater());
        }
        assertFalse(recordingAzureClientProvider.getCalledDisposeLater());

        recordingAzureClientProvider.dropConnectionProviders(Set.of(key));
        assertTrue(recordingAzureClientProvider.getCalledDisposeLater());
    }

    private static String encodeKey(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public void testConnectionProvidersToEvict() {
        // clientName1: account unchanged (only `max_connections` changed) -> not evicted
        // clientName2: account changed -> the key with the old account is evicted
        // clientName3: removed from the current settings -> evicted
        // clientName4: exists only in the current settings -> not evicted
        final var previousSettings = Map.of(
            "clientName1",
            createStorageSettings("clientName1", "account1", randomIntBetween(1, 100)),
            "clientName2",
            createStorageSettings("clientName2", "account2", randomIntBetween(1, 200)),
            "clientName3",
            createStorageSettings("clientName3", "account3", randomIntBetween(1, 200))
        );
        final var currentSettings = Map.of(
            "clientName1",
            createStorageSettings("clientName1", "account1", randomIntBetween(101, 200)),
            "clientName2",
            createStorageSettings("clientName2", "newAccount2", randomIntBetween(1, 200)),
            "clientName4",
            createStorageSettings("clientName4", "account4", randomIntBetween(1, 200))
        );

        final var projectId = randomProjectIdOrDefault();
        final var toEvict = AzureClientProvider.ConnectionProviderKey.connectionProvidersToEvict(
            projectId,
            previousSettings,
            currentSettings
        );
        assertEquals(
            Set.of(
                new AzureClientProvider.ConnectionProviderKey(projectId, "clientName2", "account2"),
                new AzureClientProvider.ConnectionProviderKey(projectId, "clientName3", "account3")
            ),
            toEvict
        );

        // when the current settings are null, all previous keys are evicted
        final var toEvictAll = AzureClientProvider.ConnectionProviderKey.connectionProvidersToEvict(null, previousSettings, null);
        assertEquals(
            Set.of(
                new AzureClientProvider.ConnectionProviderKey(null, "clientName1", "account1"),
                new AzureClientProvider.ConnectionProviderKey(null, "clientName2", "account2"),
                new AzureClientProvider.ConnectionProviderKey(null, "clientName3", "account3")
            ),
            toEvictAll
        );
    }

    private AzureStorageSettings createStorageSettings(String clientName, String account, Settings.Builder builder) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(Strings.format("azure.client.%s.account", clientName), account);
        secureSettings.setString(Strings.format("azure.client.%s.key", clientName), encodeKey("key"));

        final Settings settings = builder.setSecureSettings(secureSettings).build();

        Map<String, AzureStorageSettings> clientSettings = AzureStorageSettings.load(settings);
        AzureStorageSettings storageSettings = clientSettings.get(clientName);
        assertNotNull(storageSettings);
        return storageSettings;
    }

    private AzureStorageSettings createStorageSettings(String clientName, String account, int perClientMaxConnections) {
        return createStorageSettings(
            clientName,
            account,
            Settings.builder().put(Strings.format("azure.client.%s.max_connections", clientName), perClientMaxConnections)
        );
    }

    private AzureBlobServiceClient createClient(
        AzureClientProvider clientProvider,
        String clientName,
        AzureStorageSettings storageSettings
    ) {
        return clientProvider.createClient(
            null,
            clientName,
            storageSettings,
            LocationMode.PRIMARY_ONLY,
            new RequestRetryOptions(),
            null,
            NOOP_HANDLER,
            randomFrom(OperationPurpose.values())
        );
    }

    private AzureBlobServiceClient createClient(String clientName, AzureStorageSettings storageSettings) {
        return createClient(azureClientProvider, clientName, storageSettings);
    }
}
