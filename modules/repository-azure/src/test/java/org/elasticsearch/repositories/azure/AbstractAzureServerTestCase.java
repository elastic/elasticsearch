/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.LOCATION_MODE_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@SuppressForbidden(reason = "use a http server")
public abstract class AbstractAzureServerTestCase extends ESTestCase {
    protected static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1L;
    protected static final String ACCOUNT = "account";
    protected static final String CONTAINER = "container";

    protected HttpServer httpServer;
    protected HttpServer secondaryHttpServer;
    protected boolean serverlessMode;
    private ThreadPool threadPool;
    private AzureClientProvider clientProvider;

    @Before
    public void setUp() throws Exception {
        serverlessMode = false;
        threadPool = new TestThreadPool(
            getTestClass().getName(),
            AzureRepositoryPlugin.executorBuilder(),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        secondaryHttpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        secondaryHttpServer.start();
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        clientProvider.close();
        httpServer.stop(0);
        secondaryHttpServer.stop(0);
        super.tearDown();
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    protected BlobContainer createBlobContainer(final int maxRetries) {
        return createBlobContainer(maxRetries, null, LocationMode.PRIMARY_ONLY);
    }

    protected BlobContainer createBlobContainer(final int maxRetries, String secondaryHost, final LocationMode locationMode) {
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), ACCOUNT);
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);

        return createBlobContainer(maxRetries, secondaryHost, locationMode, clientName, secureSettings);
    }

    protected BlobContainer createBlobContainer(
        final int maxRetries,
        String secondaryHost,
        final LocationMode locationMode,
        String clientName,
        SecureSettings secureSettings
    ) {
        final Settings.Builder clientSettings = Settings.builder();

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + getEndpointForServer(httpServer, ACCOUNT);
        if (secondaryHost != null) {
            endpoint += ";BlobSecondaryEndpoint=" + getEndpointForServer(secondaryHttpServer, ACCOUNT);
        }
        clientSettings.put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueMillis(500));

        clientSettings.setSecureSettings(secureSettings);
        clientSettings.put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, serverlessMode);

        final AzureStorageService service = new AzureStorageService(clientSettings.build(), clientProvider) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    maxRetries + 1,
                    60,
                    50L,
                    100L,
                    // The SDK doesn't work well with ip endponts. Secondary host endpoints that contain
                    // a path causes the sdk to rewrite the endpoint with an invalid path, that's the reason why we provide just the host +
                    // port.
                    secondaryHost != null ? secondaryHost.replaceFirst("/" + ACCOUNT, "") : null
                );
            }

            @Override
            long getUploadBlockSize() {
                return ByteSizeUnit.MB.toBytes(1);
            }

            @Override
            int getMaxReadRetries(String clientName) {
                return maxRetries;
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), CONTAINER)
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .put(LOCATION_MODE_SETTING.getKey(), locationMode)
                .put(MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.MB))
                .build()
        );

        return new AzureBlobContainer(
            BlobPath.EMPTY,
            new AzureBlobStore(repositoryMetadata, service, BigArrays.NON_RECYCLING_INSTANCE, RepositoriesMetrics.NOOP)
        );
    }

    protected static byte[] randomBlobContent() {
        return randomByteArrayOfLength(randomIntBetween(1, 1 << 20)); // rarely up to 1mb
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    protected static Tuple<Long, Long> getRanges(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("X-ms-range");
        if (rangeHeader == null) {
            return Tuple.tuple(0L, MAX_RANGE_VAL);
        }

        final Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        final long rangeStart = Long.parseLong(matcher.group(1));
        final long rangeEnd = Long.parseLong(matcher.group(2));
        assertThat(rangeStart, lessThanOrEqualTo(rangeEnd));
        return Tuple.tuple(rangeStart, rangeEnd);
    }

    protected static int getRangeStart(HttpExchange exchange) {
        return Math.toIntExact(getRanges(exchange).v1());
    }

    protected static Optional<Integer> getRangeEnd(HttpExchange exchange) {
        final long rangeEnd = getRanges(exchange).v2();
        if (rangeEnd == MAX_RANGE_VAL) {
            return Optional.empty();
        }
        return Optional.of(Math.toIntExact(rangeEnd));
    }

    protected String getEndpointForServer(HttpServer server, String accountName) {
        InetSocketAddress address = server.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + "/" + accountName;
    }

    protected void readFromInputStream(InputStream inputStream, long bytesToRead) {
        try {
            long totalBytesRead = 0;
            while (inputStream.read() != -1 && totalBytesRead < bytesToRead) {
                totalBytesRead += 1;
            }
            assertThat(totalBytesRead, equalTo(bytesToRead));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
