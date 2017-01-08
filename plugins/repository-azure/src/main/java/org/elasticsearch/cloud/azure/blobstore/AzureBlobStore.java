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

package org.elasticsearch.cloud.azure.blobstore;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.cloud.azure.storage.AzureStorageService.Storage;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.getValue;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository;

public class AzureBlobStore extends AbstractComponent implements BlobStore {

    private final AzureStorageService client;

    private final String accountName;
    private final LocationMode locMode;
    private final String container;
    private final String repositoryName;

    private String[] getAccounts() {
        if (Strings.isNullOrEmpty(this.accountName ))
            return new String[0];

        return this.accountName.split(",");
    }

    private String getAccount(String blob) {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            return null;
        }
        int hash = this.getAccountHash(blob, accounts.length);
        return accounts[hash];
    }

    private int getAccountHash(String blob, int numberOfAccounts) {
        int hash = this.hashCode(blob);
        return Math.abs(hash % numberOfAccounts);
    }

    /**
     * Returns a hash code for this blob name.
     *
     * @return  a hash code value for this blob name.
     */
    private int hashCode(String blob) {
        if (Strings.isNullOrEmpty(blob))
            return 0;

        int hash = 0;
        final char chars[] = blob.toCharArray();
        for (char ch : chars) {
            hash += ch;
        }
        return hash;
    }

    public AzureBlobStore(RepositoryMetaData metadata, Settings settings,
                          AzureStorageService client) throws URISyntaxException, StorageException {
        super(settings);
        this.client = client;
        this.container = getValue(metadata.settings(), settings, Repository.CONTAINER_SETTING, Storage.CONTAINER_SETTING);
        this.repositoryName = metadata.name();
        this.accountName = getValue(metadata.settings(), settings, Repository.ACCOUNT_SETTING, Storage.ACCOUNT_SETTING);

        String modeStr = getValue(metadata.settings(), settings, Repository.LOCATION_MODE_SETTING, Storage.LOCATION_MODE_SETTING);
        if (Strings.hasLength(modeStr)) {
            this.locMode = LocationMode.valueOf(modeStr.toUpperCase(Locale.ROOT));
        } else {
            this.locMode = LocationMode.PRIMARY_ONLY;
        }
    }

    @Override
    public String toString() {
        return container;
    }

    public String container() {
        return container;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(repositoryName, path, this);
    }

    @Override
    public void delete(BlobPath path) {
        String keyPath = path.buildAsString();
        final String[] accounts = this.getAccounts();
        try {
            if (accounts.length == 0) {
                this.client.deleteFiles(null, this.locMode, container, keyPath);
            }else{
                for (String account : accounts)
                {
                    this.client.deleteFiles(account, this.locMode, container, keyPath);
                }
            }
        } catch (URISyntaxException | StorageException e) {
            logger.warn("can not remove [{}] in container {{}}: {}", keyPath, container, e.getMessage());
        }
    }

    @Override
    public void close() {
    }

    public boolean doesContainerExist(String container)
    {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0)
        {
            return this.client.doesContainerExist(null, this.locMode, container);
        }else{
            for (String account : accounts)
            {
                if (!this.client.doesContainerExist(account, this.locMode, container)) {
                    return false;
                }
            }
        }

        return true;
    }

    public void removeContainer(String container) throws URISyntaxException, StorageException
    {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0){
            this.client.removeContainer(null, this.locMode, container);
        }else{
            for (String account : accounts) {
                this.client.removeContainer(account, this.locMode, container);
            }
        }
    }

    public void createContainer(String container) throws URISyntaxException, StorageException
    {
        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            this.client.createContainer(null, this.locMode, container);
        }else{
            for (String account : accounts) {
                this.client.createContainer(account, this.locMode, container);
            }
        }
    }

    public void deleteFiles(String container, String path) throws URISyntaxException, StorageException
    {
        this.client.deleteFiles(this.accountName, this.locMode, container, path);
    }

    public boolean blobExists(String container, String blob) throws URISyntaxException, StorageException
    {
        String account = this.getAccount(blob);
        return this.client.blobExists(account, this.locMode, container, blob);
    }

    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException
    {
        String account = this.getAccount(blob);
        this.client.deleteBlob(account, this.locMode, container, blob);
    }

    public InputStream getInputStream(String container, String blob) throws URISyntaxException, StorageException, IOException
    {
        String account = this.getAccount(blob);
        return this.client.getInputStream(account, this.locMode, container, blob);
    }

    public OutputStream getOutputStream(String container, String blob) throws URISyntaxException, StorageException
    {
        String account = this.getAccount(blob);
        return this.client.getOutputStream(account, this.locMode, container, blob);
    }

    public Map<String,BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix) throws URISyntaxException, StorageException
    {
        MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();

        final String[] accounts = this.getAccounts();
        if (accounts.length == 0) {
            return this.client.listBlobsByPrefix(null, this.locMode, container, keyPath, prefix);
        } else {
            for (String account : accounts) {
                Map<String, BlobMetaData> blobs = this.client.listBlobsByPrefix(account, this.locMode, container, keyPath, prefix);
                blobsBuilder.putAll(blobs);
            }
        }
        return blobsBuilder.immutableMap();
    }

    public void moveBlob(String container, String sourceBlob, String targetBlob) throws URISyntaxException, StorageException
    {
        String sourceAccount = this.getAccount(sourceBlob);
        String targetAccount = this.getAccount(targetBlob);
        this.client.moveBlob(sourceAccount, targetAccount,this.locMode, container, sourceBlob, targetBlob);
    }
}
