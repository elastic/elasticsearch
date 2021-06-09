/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.http.ProxyOptions;
import com.azure.core.util.logging.ClientLogger;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyMap;

public class AzureStorageService {
    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);

    /**
     * The maximum size of a BlockBlob block.
     * See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
     */
    public static ByteSizeValue MAX_BLOCK_SIZE = new ByteSizeValue(100, ByteSizeUnit.MB);

    /**
     * The maximum number of blocks.
     * See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
     */
    public static final long MAX_BLOCK_NUMBER = 50000;

    /**
     * Default block size for multi-block uploads. The Azure repository will use the Put block and Put block list APIs to split the
     * stream into several part, each of block_size length, and will upload each part in its own request.
     */
    private static final ByteSizeValue DEFAULT_BLOCK_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(
                MAX_BLOCK_SIZE.getBytes(),
                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)),
        ByteSizeUnit.BYTES);

    /**
     * The maximum size of a Block Blob.
     * See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
     */
    public static final long MAX_BLOB_SIZE = MAX_BLOCK_NUMBER * DEFAULT_BLOCK_SIZE.getBytes();

    /**
     * Maximum allowed blob size in Azure blob store.
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(MAX_BLOB_SIZE , ByteSizeUnit.BYTES);

    private static final long DEFAULT_UPLOAD_BLOCK_SIZE = DEFAULT_BLOCK_SIZE.getBytes();

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();
    private final AzureClientProvider azureClientProvider;
    private final ClientLogger clientLogger = new ClientLogger(AzureStorageService.class);

    public AzureStorageService(Settings settings, AzureClientProvider azureClientProvider) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshSettings(clientsSettings);
        this.azureClientProvider = azureClientProvider;
    }

    public AzureBlobServiceClient client(String clientName, LocationMode locationMode) {
        return client(clientName, locationMode, null);
    }

    public AzureBlobServiceClient client(String clientName, LocationMode locationMode, BiConsumer<String, URL> successfulRequestConsumer) {
        final AzureStorageSettings azureStorageSettings = getClientSettings(clientName);

        RequestRetryOptions retryOptions = getRetryOptions(locationMode, azureStorageSettings);
        ProxyOptions proxyOptions = getProxyOptions(azureStorageSettings);
        return azureClientProvider.createClient(azureStorageSettings, locationMode, retryOptions, proxyOptions, successfulRequestConsumer);
    }

    private AzureStorageSettings getClientSettings(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        return azureStorageSettings;
    }

    private static ProxyOptions getProxyOptions(AzureStorageSettings settings) {
        Proxy proxy = settings.getProxy();
        if (proxy == null) {
            return null;
        }

        switch (proxy.type()) {
            case HTTP:
                return new ProxyOptions(ProxyOptions.Type.HTTP, (InetSocketAddress) proxy.address());
            case SOCKS:
                return new ProxyOptions(ProxyOptions.Type.SOCKS5, (InetSocketAddress) proxy.address());
            default:
                return null;
        }
    }

    // non-static, package private for testing
    long getUploadBlockSize() {
        return DEFAULT_UPLOAD_BLOCK_SIZE;
    }

    int getMaxReadRetries(String clientName) {
        AzureStorageSettings azureStorageSettings = getClientSettings(clientName);
        return azureStorageSettings.getMaxRetries();
    }

    // non-static, package private for testing
    RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
        String connectString = azureStorageSettings.getConnectString();
        StorageConnectionString storageConnectionString = StorageConnectionString.create(connectString, clientLogger);
        String primaryUri = storageConnectionString.getBlobEndpoint().getPrimaryUri();
        String secondaryUri = storageConnectionString.getBlobEndpoint().getSecondaryUri();

        if (locationMode == LocationMode.PRIMARY_THEN_SECONDARY && secondaryUri == null) {
            throw new IllegalArgumentException("Unable to use " + locationMode + " location mode without a secondary location URI");
        }

        final String secondaryHost;
        switch (locationMode) {
            case PRIMARY_ONLY:
            case SECONDARY_ONLY:
                secondaryHost = null;
                break;
            case PRIMARY_THEN_SECONDARY:
                secondaryHost = secondaryUri;
                break;
            case SECONDARY_THEN_PRIMARY:
                secondaryHost = primaryUri;
                break;
            default:
                assert false;
                throw new AssertionError("Impossible to get here");
        }

        // The request retry policy uses seconds as the default time unit, since
        // it's possible to configure a timeout < 1s we should ceil that value
        // as RequestRetryOptions expects a value >= 1.
        // See https://github.com/Azure/azure-sdk-for-java/issues/17590 for a proposal
        // to fix this issue.
        TimeValue configuredTimeout = azureStorageSettings.getTimeout();
        int timeout = configuredTimeout.duration() == -1 ? Integer.MAX_VALUE : Math.max(1, Math.toIntExact(configuredTimeout.getSeconds()));
        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL,
            azureStorageSettings.getMaxRetries(), timeout,
            null, null, secondaryHost);
    }

    /**
     * Updates settings for building clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     */
    public void refreshSettings(Map<String, AzureStorageSettings> clientsSettings) {
        this.storageSettings = Map.copyOf(clientsSettings);
        // clients are built lazily by {@link client(String, LocationMode)}
    }
}
