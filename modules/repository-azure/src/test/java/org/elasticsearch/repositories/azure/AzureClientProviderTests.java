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

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AzureClientProviderTests extends ESTestCase {
    private static final AzureClientProvider.RequestMetricsHandler NOOP_HANDLER = (purpose, method, url, metrics) -> {};

    private ThreadPool threadPool;
    private AzureClientProvider azureClientProvider;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            AzureRepositoryPlugin.executorBuilder(),
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
                storageSettings,
                locationMode,
                requestRetryOptions,
                null,
                NOOP_HANDLER,
                randomFrom(OperationPurpose.values())
            )
        );
    }

    private static String encodeKey(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
