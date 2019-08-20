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

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

public class EncryptedRepository extends BlobStoreRepository {

    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity(),
            Setting.Property.NodeScope);
    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private BlobStoreRepository delegatedRepository;

    protected EncryptedRepository(BlobStoreRepository delegatedRepository) {
        super(delegatedRepository);
        this.delegatedRepository = delegatedRepository;
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new EncryptedBlobStoreDecorator(this.delegatedRepository.blobStore());
    }

    @Override
    protected void doStart() {
        this.delegatedRepository.start();
        super.doStart();
    }

    @Override
    protected void doStop() {
        super.doStop();
        this.delegatedRepository.stop();
    }

    @Override
    protected void doClose() {
        super.doClose();
        this.delegatedRepository.close();
    }

    /**
     * Returns a new encrypted repository factory
     */
    public static Repository.Factory newRepositoryFactory() {
        return new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                Repository delegatedRepository = factory.create(new RepositoryMetaData(metaData.name(),
                        delegateType, metaData.settings()));
                if (false == (delegatedRepository instanceof BlobStoreRepository)) {
                    throw new IllegalArgumentException("Unsupported type " + DELEGATE_TYPE.getKey());
                }
                return new EncryptedRepository((BlobStoreRepository)delegatedRepository);
            }
        };
    }

    private static class EncryptedBlobStoreDecorator implements BlobStore {

        private final BlobStore delegatedBlobStore;

        EncryptedBlobStoreDecorator(BlobStore blobStore) {
            this.delegatedBlobStore = blobStore;
        }

        @Override
        public void close() throws IOException {
            this.delegatedBlobStore.close();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            BlobPath encryptionMetadataBlobPath = BlobPath.cleanPath();
            encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(ENCRYPTION_METADATA_PREFIX + "<master-key-id>");
            for (String pathComponent : path) {
                encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(pathComponent);
            }
            return new EncryptedBlobContainerDecorator(this.delegatedBlobStore.blobContainer(path),
                    this.delegatedBlobStore.blobContainer(encryptionMetadataBlobPath));
        }
    }

    private static class EncryptedBlobContainerDecorator implements BlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer metadataBlobContainer;

        EncryptedBlobContainerDecorator(BlobContainer delegatedBlobContainer, BlobContainer metadataBlobContainer) {
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.metadataBlobContainer = metadataBlobContainer;
        }

        @Override
        public BlobPath path() {
            return this.delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            // TODO
            return this.delegatedBlobContainer.readBlob(blobName);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            // TODO
            this.delegatedBlobContainer.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
            // TODO
            this.delegatedBlobContainer.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void deleteBlob(String blobName) throws IOException {
            // TODO
            this.delegatedBlobContainer.deleteBlob(blobName);
        }

        @Override
        public void delete() throws IOException {
            // TODO
            this.delegatedBlobContainer.delete();
        }

        @Override
        public Map<String, BlobMetaData> listBlobs() throws IOException {
            return this.delegatedBlobContainer.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return this.delegatedBlobContainer.children();
        }

        @Override
        public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return this.delegatedBlobContainer.listBlobsByPrefix(blobNamePrefix);
        }
    }
}
