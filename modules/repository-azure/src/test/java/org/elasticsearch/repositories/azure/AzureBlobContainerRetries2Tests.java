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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.LOCATION_MODE_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;

@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerRetries2Tests extends AbstractBlobContainerRetriesTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    protected boolean serverlessMode;
    private AzureClientProvider clientProvider;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        serverlessMode = false;
        threadPool = new TestThreadPool(
            getTestClass().getName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        clientProvider.close();
        super.tearDown();
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    @Override
    protected String downloadStorageEndpoint(BlobContainer container, String blob) {
        return "/account/container/" + container.path().buildAsString() + blob;
    }

    @Override
    protected String bytesContentType() {
        return "application/octet-stream";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        return Exception.class;
    }

    @Override
    protected BlobContainer createBlobContainer(
        @Nullable Integer maxRetries,
        @Nullable TimeValue readTimeout,
        @Nullable Boolean disableChunkedEncoding,
        @Nullable Integer maxConnections,
        @Nullable ByteSizeValue bufferSize,
        @Nullable Integer maxBulkDeletes,
        @Nullable BlobPath blobContainerPath
    ) {
        warnIfUnsupportedSettingSet("disableChunkedEncoding", disableChunkedEncoding);
        warnIfUnsupportedSettingSet("maxConnections", maxConnections);
        warnIfUnsupportedSettingSet("bufferSize", bufferSize);
        warnIfUnsupportedSettingSet("maxBulkDeletes", maxBulkDeletes);

        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + getEndpointForServer(httpServer, ACCOUNT);
        clientSettings.put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        if (maxRetries != null) {
            clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        }
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueSeconds(1));
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), ACCOUNT);
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);
        clientSettings.setSecureSettings(secureSettings);
        clientSettings.put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, serverlessMode);

        final AzureStorageService service = new AzureStorageService(
            clientSettings.build(),
            clientProvider,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                RequestRetryOptions retryOptions = super.getRetryOptions(locationMode, azureStorageSettings);
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    retryOptions.getMaxTries(),
                    retryOptions.getTryTimeoutDuration(),
                    Duration.ofMillis(50),
                    Duration.ofMinutes(5),
                    // The SDK doesn't work well with ip endpoints. Secondary host endpoints that contain
                    // a path causes the sdk to rewrite the endpoint with an invalid path, that's the reason why we provide just the host +
                    // port.
                    null // secondaryHost != null ? secondaryHost.replaceFirst("/" + ACCOUNT, "") : null
                );
            }

            @Override
            long getUploadBlockSize() {
                return ByteSizeUnit.MB.toBytes(1);
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), CONTAINER)
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .put(LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
                .put(MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                .build()
        );

        return new AzureBlobContainer(
            Objects.requireNonNullElse(blobContainerPath, randomBoolean() ? BlobPath.EMPTY : BlobPath.EMPTY.add(randomIdentifier())),
            new AzureBlobStore(ProjectId.DEFAULT, repositoryMetadata, service, BigArrays.NON_RECYCLING_INSTANCE, RepositoriesMetrics.NOOP)
        );
    }

    private void warnIfUnsupportedSettingSet(String settingName, Object value) {
        if (value != null) {
            logger.warn("Setting [{}] is not supported for Azure repository. Ignoring value [{}]", settingName, value);
        }
    }

    protected String getEndpointForServer(HttpServer server, String accountName) {
        InetSocketAddress address = server.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + "/" + accountName;
    }

    @Override
    protected void addSuccessfulDownloadHeaders(HttpExchange exchange, byte[] blobContents) {
        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("Content-Length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
        exchange.getResponseHeaders().add("ETag", eTagForContents(blobContents));
    }

    @Override
    protected HttpHeaderParser.Range getRange(HttpExchange exchange) {
        return AbstractAzureServerTestCase.getRanges(exchange);
    }

    @Override
    protected void handleHeadRequest(HttpExchange exchange, byte[] blobContents) throws IOException {
        if (exchange.getRequestHeaders().containsKey("X-ms-range")) {
            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("Shouldn't send a HEAD request for a range"));
        }
        addSuccessfulDownloadHeaders(exchange, blobContents);
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
    }

    private static String eTagForContents(byte[] blobContents) {
        return Base64.getEncoder().encodeToString(MessageDigests.digest(new BytesArray(blobContents), MessageDigests.md5()));
    }
}
