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

package org.elasticsearch.cloud.azure.storage;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.core.Base64;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl.blobNameFromUri;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AzureStorageServiceTests extends ESTestCase {

    static final Settings settings = Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", encodeKey("mykey1"))
            .put("cloud.azure.storage.azure1.default", true)
            .put("cloud.azure.storage.azure2.account", "myaccount2")
            .put("cloud.azure.storage.azure2.key",  encodeKey("mykey2"))
            .put("cloud.azure.storage.azure3.account", "myaccount3")
            .put("cloud.azure.storage.azure3.key", encodeKey("mykey3"))
            .put("cloud.azure.storage.azure3.timeout", "30s")
            .build();

    public void testGetSelectedClientWithNoPrimaryAndSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(Settings.EMPTY);
        try {
            azureStorageService.getSelectedClient("whatever", LocationMode.PRIMARY_ONLY);
            fail("we should have raised an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("No primary azure storage can be found. Check your elasticsearch.yml."));
        }
    }

    public void testGetSelectedClientWithNoSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", encodeKey("mykey1"))
            .build());
        CloudBlobClient client = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount1.blob.core.windows.net")));
    }

    public void testGetDefaultClientWithNoSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", encodeKey("mykey1"))
            .build());
        CloudBlobClient client = azureStorageService.getSelectedClient(null, LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount1.blob.core.windows.net")));
    }

    public void testGetSelectedClientPrimary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        CloudBlobClient client = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount1.blob.core.windows.net")));
    }

    public void testGetSelectedClientSecondary1() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        CloudBlobClient client = azureStorageService.getSelectedClient("azure2", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount2.blob.core.windows.net")));
    }

    public void testGetSelectedClientSecondary2() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        CloudBlobClient client = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount3.blob.core.windows.net")));
    }

    public void testGetDefaultClientWithPrimaryAndSecondaries() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        CloudBlobClient client = azureStorageService.getSelectedClient(null, LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount1.blob.core.windows.net")));
    }

    public void testGetSelectedClientNonExisting() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        try {
            azureStorageService.getSelectedClient("azure4", LocationMode.PRIMARY_ONLY);
            fail("we should have raised an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Can not find azure account. Check your elasticsearch.yml."));
        }
    }

    public void testGetSelectedClientDefault() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        CloudBlobClient client = azureStorageService.getSelectedClient(null, LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://myaccount1.blob.core.windows.net")));
    }

    public void testGetSelectedClientGlobalTimeout() {
        Settings timeoutSettings = Settings.builder()
                .put(settings)
                .put(AzureStorageService.Storage.TIMEOUT_SETTING.getKey(), "10s")
                .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(10 * 1000));
        CloudBlobClient client3 = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));
    }

    public void testGetSelectedClientDefaultTimeout() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), nullValue());
        CloudBlobClient client3 = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));
    }

    public void testGetSelectedClientNoTimeout() {
        Settings timeoutSettings = Settings.builder()
            .put("cloud.azure.storage.azure.account", "myaccount")
            .put("cloud.azure.storage.azure.key", encodeKey("mykey"))
            .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(nullValue()));
    }

    public void testGetSelectedClientBackoffPolicy() {
        Settings timeoutSettings = Settings.builder()
            .put("cloud.azure.storage.azure.account", "myaccount")
            .put("cloud.azure.storage.azure.key", encodeKey("mykey"))
            .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testGetSelectedClientBackoffPolicyNbRetries() {
        Settings timeoutSettings = Settings.builder()
            .put("cloud.azure.storage.azure.account", "myaccount")
            .put("cloud.azure.storage.azure.key", encodeKey("mykey"))
            .put("cloud.azure.storage.azure.max_retries", 7)
            .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    private static String encodeKey(final String value) {
        return Base64.encode(value.getBytes(StandardCharsets.UTF_8));
    }

    public void testBlobNameFromUri() throws URISyntaxException {
        String name = blobNameFromUri(new URI("https://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("https://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
    }
}
