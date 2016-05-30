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
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl.blobNameFromUri;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AzureStorageServiceTests extends ESTestCase {
    final static Settings settings = Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .put("cloud.azure.storage.azure1.default", true)
            .put("cloud.azure.storage.azure2.account", "myaccount2")
            .put("cloud.azure.storage.azure2.key", "mykey2")
            .put("cloud.azure.storage.azure3.account", "myaccount3")
            .put("cloud.azure.storage.azure3.key", "mykey3")
            .put("cloud.azure.storage.azure3.timeout", "30s")
            .build();

    public void testGetSelectedClientWithNoPrimaryAndSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(Settings.EMPTY);
        azureStorageService.doStart();
        try {
            azureStorageService.getSelectedClient("whatever", LocationMode.PRIMARY_ONLY);
            fail("we should have raised an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("No primary azure storage can be found. Check your elasticsearch.yml."));
        }
    }

    public void testGetSelectedClientWithNoSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .build());
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
    }

    public void testGetDefaultClientWithNoSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .build());
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient(null, LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
    }

    public void testGetSelectedClientPrimary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
    }

    public void testGetSelectedClientSecondary1() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient("azure2", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure2")));
    }

    public void testGetSelectedClientSecondary2() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure3")));
    }

    public void testGetDefaultClientWithPrimaryAndSecondaries() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient(null, LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
    }

    public void testGetSelectedClientNonExisting() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        try {
            azureStorageService.getSelectedClient("azure4", LocationMode.PRIMARY_ONLY);
            fail("we should have raised an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Can not find azure account [azure4]. Check your elasticsearch.yml."));
        }
    }

    public void testGetSelectedClientDefault() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        CloudBlobClient client = azureStorageService.getSelectedClient(null, LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
    }

    public void testGetSelectedClientGlobalTimeout() {
        Settings timeoutSettings = Settings.builder()
                .put(settings)
                .put(AzureStorageService.Storage.TIMEOUT_SETTING.getKey(), "10s")
                .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(timeoutSettings);
        azureStorageService.doStart();
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(10 * 1000));
        CloudBlobClient client3 = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));
    }

    public void testGetSelectedClientDefaultTimeout() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(settings);
        azureStorageService.doStart();
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), nullValue());
        CloudBlobClient client3 = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));
    }

    public void testGetSelectedClientNoTimeout() {
        Settings timeoutSettings = Settings.builder()
            .put("cloud.azure.storage.azure.account", "myaccount")
            .put("cloud.azure.storage.azure.key", "mykey")
            .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(timeoutSettings);
        azureStorageService.doStart();
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(nullValue()));
    }

    /**
     * This internal class just overload createClient method which is called by AzureStorageServiceImpl.doStart()
     */
    class AzureStorageServiceMock extends AzureStorageServiceImpl {
        public AzureStorageServiceMock(Settings settings) {
            super(settings);
        }

        // We fake the client here
        @Override
        void createClient(AzureStorageSettings azureStorageSettings) {
            this.clients.put(azureStorageSettings.getAccount(),
                    new CloudBlobClient(URI.create("https://" + azureStorageSettings.getName())));
        }
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
