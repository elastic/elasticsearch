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

import com.azure.storage.common.policy.RequestRetryOptions;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class AzureClientProviderTests extends ESTestCase {
    private static final BiConsumer<String, URL> EMPTY_CONSUMER = (method, url) -> { };

    private ThreadPool threadPool;
    private AzureClientProvider azureClientProvider;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName(),
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
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;" +
                "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
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
        azureClientProvider.createClient(storageSettings, locationMode, requestRetryOptions, null, EMPTY_CONSUMER);
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
        expectThrows(IllegalArgumentException.class, () -> {
            azureClientProvider.createClient(storageSettings, locationMode, requestRetryOptions, null, EMPTY_CONSUMER);
        });
    }

    private static String encodeKey(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
