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

package org.elasticsearch.cloud.azure;

import com.microsoft.windowsazure.services.blob.BlobConfiguration;
import com.microsoft.windowsazure.services.blob.BlobContract;
import com.microsoft.windowsazure.services.blob.BlobService;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
import com.microsoft.windowsazure.services.blob.models.BlobProperties;
import com.microsoft.windowsazure.services.blob.models.GetBlobResult;
import com.microsoft.windowsazure.services.blob.models.ListBlobsOptions;
import com.microsoft.windowsazure.services.blob.models.ListBlobsResult;
import com.microsoft.windowsazure.services.core.Configuration;
import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 *
 */
public class AzureStorageServiceImpl extends AbstractLifecycleComponent<AzureStorageServiceImpl>
    implements AzureStorageService {

    private final String account;
    private final String key;
    private final String blob;

    private CloudStorageAccount storage_account;
    private CloudBlobClient client;
    private BlobContract service;


    @Inject
    public AzureStorageServiceImpl(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        settingsFilter.addFilter(new AzureSettingsFilter());

        // We try to load storage API settings from `cloud.azure.`
        account = settings.get("cloud.azure." + Fields.ACCOUNT);
        key = settings.get("cloud.azure." + Fields.KEY);
        blob = "http://" + account + ".blob.core.windows.net/";

        try {
            if (account != null) {
                if (logger.isTraceEnabled()) logger.trace("creating new Azure storage client using account [{}], key [{}], blob [{}]",
                        account, key, blob);

                String storageConnectionString =
                        "DefaultEndpointsProtocol=http;"
                                + "AccountName="+ account +";"
                                + "AccountKey=" + key;

                Configuration configuration = Configuration.getInstance();
                configuration.setProperty(BlobConfiguration.ACCOUNT_NAME, account);
                configuration.setProperty(BlobConfiguration.ACCOUNT_KEY, key);
                configuration.setProperty(BlobConfiguration.URI, blob);
                service = BlobService.create(configuration);

                storage_account = CloudStorageAccount.parse(storageConnectionString);
                client = storage_account.createCloudBlobClient();
            }
        } catch (Exception e) {
            // Can not start Azure Storage Client
            logger.error("can not start azure storage client: {}", e.getMessage());
        }
    }

    @Override
    public boolean doesContainerExist(String container) {
        try {
            CloudBlobContainer blob_container = client.getContainerReference(container);
            return blob_container.exists();
        } catch (Exception e) {
            logger.error("can not access container [{}]", container);
        }
        return false;
    }

    @Override
    public void removeContainer(String container) throws URISyntaxException, StorageException {
        CloudBlobContainer blob_container = client.getContainerReference(container);
        // TODO Should we set some timeout and retry options?
        /*
        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(1000);
        options.setRetryPolicyFactory(new RetryNoRetry());
        blob_container.deleteIfExists(options, null);
        */
        blob_container.deleteIfExists();
    }

    @Override
    public void createContainer(String container) throws URISyntaxException, StorageException {
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (logger.isTraceEnabled()) {
            logger.trace("creating container [{}]", container);
        }
        blob_container.createIfNotExist();
    }

    @Override
    public void deleteFiles(String container, String path) throws URISyntaxException, StorageException, ServiceException {
        if (logger.isTraceEnabled()) {
            logger.trace("delete files container [{}], path [{}]",
                    container, path);
        }

        // Container name must be lower case.
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            ListBlobsOptions options = new ListBlobsOptions();
            options.setPrefix(path);

            List<ListBlobsResult.BlobEntry> blobs = service.listBlobs(container, options).getBlobs();
            for (ListBlobsResult.BlobEntry blob : blobs) {
                if (logger.isTraceEnabled()) {
                    logger.trace("removing in container [{}], path [{}], blob [{}]",
                            container, path, blob.getName());
                }
                service.deleteBlob(container, blob.getName());
            }
        }
    }

    @Override
    public boolean blobExists(String container, String blob) throws URISyntaxException, StorageException {
        // Container name must be lower case.
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            CloudBlockBlob azureBlob = blob_container.getBlockBlobReference(blob);
            return azureBlob.exists();
        }

        return false;
    }

    @Override
    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException {
        if (logger.isTraceEnabled()) {
            logger.trace("delete blob for container [{}], blob [{}]",
                    container, blob);
        }

        // Container name must be lower case.
        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            if (logger.isTraceEnabled()) {
                logger.trace("blob found. removing.",
                        container, blob);
            }
            // TODO A REVOIR
            CloudBlockBlob azureBlob = blob_container.getBlockBlobReference(blob);
            azureBlob.delete();
        }
    }

    @Override
    public InputStream getInputStream(String container, String blob) throws ServiceException {
        GetBlobResult blobResult = service.getBlob(container, blob);
        return blobResult.getContentStream();
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) throws URISyntaxException, StorageException, ServiceException {
        logger.debug("listBlobsByPrefix container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix);
        ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();

        CloudBlobContainer blob_container = client.getContainerReference(container);
        if (blob_container.exists()) {
            Iterable<ListBlobItem> blobs = blob_container.listBlobs(keyPath + prefix);
            for (ListBlobItem blob : blobs) {
                URI uri = blob.getUri();
                if (logger.isTraceEnabled()) {
                    logger.trace("blob url [{}]", uri);
                }
                String blobpath = uri.getPath().substring(container.length() + 1);
                BlobProperties properties = service.getBlobProperties(container, blobpath).getProperties();
                String name = blobpath.substring(keyPath.length() + 1);
                if (logger.isTraceEnabled()) {
                    logger.trace("blob url [{}], name [{}], size [{}]", uri, name, properties.getContentLength());
                }
                blobsBuilder.put(name, new PlainBlobMetaData(name, properties.getContentLength()));
            }
        }

        return blobsBuilder.build();
    }

    @Override
    public void putObject(String container, String blobname, InputStream is, long length) throws URISyntaxException, StorageException, IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("creating blob in container [{}], blob [{}], length [{}]",
                    container, blobname, length);
        }
        CloudBlockBlob blob = client.getContainerReference(container).getBlockBlobReference(blobname);
        blob.upload(is, length);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.debug("starting azure storage client instance");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.debug("stopping azure storage client instance");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
