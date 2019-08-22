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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.function.Function;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class EncryptedRepository extends BlobStoreRepository {

    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity(),
            Setting.Property.NodeScope);
    private static final Setting<String> PASSWORD = new Setting<>("password", "", Function.identity(),
            Setting.Property.NodeScope);
    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private final BlobStoreRepository delegatedRepository;
    private final SecretKey masterSecretKey;

    protected EncryptedRepository(BlobStoreRepository delegatedRepository, SecretKey masterSecretKey) {
        super(delegatedRepository);
        this.delegatedRepository = delegatedRepository;
        this.masterSecretKey = masterSecretKey;
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new EncryptedBlobStoreDecorator(this.delegatedRepository.blobStore(), this.masterSecretKey);
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

    private static SecretKey generateSecretKeyFromPassword(String password) throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecureRandom sr = SecureRandom.getInstanceStrong();
        byte[] salt = new byte[16];
        sr.nextBytes(salt);
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 1000, 128 * 8);
        return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1").generateSecret(spec);
    }

    private static SecretKey generateRandomSecretKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        return keyGen.generateKey();
    }

    private static byte[] wrapKey(SecretKey toWrap, SecretKey keyWrappingKey)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException {
        Cipher cipher = Cipher.getInstance("AESWrap");
        cipher.init(Cipher.WRAP_MODE, keyWrappingKey);
        return cipher.wrap(toWrap);
    }

    private static SecretKey unwrapKey(byte[] toUnwrap, SecretKey keyEncryptionKey)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        Cipher cipher = Cipher.getInstance("AESWrap");
        cipher.init(Cipher.UNWRAP_MODE, keyEncryptionKey);
        return (SecretKey) cipher.unwrap(toUnwrap, "AES", Cipher.SECRET_KEY);
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
                String password = PASSWORD.get(metaData.settings());
                if (Strings.hasLength(password) == false) {
                    throw new IllegalArgumentException(PASSWORD.getKey() + " must be set");
                }
                SecretKey secretKey = generateSecretKeyFromPassword(password);
                Repository.Factory factory = typeLookup.apply(delegateType);
                Repository delegatedRepository = factory.create(new RepositoryMetaData(metaData.name(),
                        delegateType, metaData.settings()));
                if (false == (delegatedRepository instanceof BlobStoreRepository)) {
                    throw new IllegalArgumentException("Unsupported type " + DELEGATE_TYPE.getKey());
                }
                return new EncryptedRepository((BlobStoreRepository)delegatedRepository, secretKey);
            }
        };
    }

    private static class EncryptedBlobStoreDecorator implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final SecretKey masterSecretKey;

        EncryptedBlobStoreDecorator(BlobStore blobStore, SecretKey masterSecretKey) {
            this.delegatedBlobStore = blobStore;
            this.masterSecretKey = masterSecretKey;
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
                    this.delegatedBlobStore.blobContainer(encryptionMetadataBlobPath), this.masterSecretKey);
        }
    }

    private static class EncryptedBlobContainerDecorator implements BlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;
        private final SecretKey masterSecretKey;

        EncryptedBlobContainerDecorator(BlobContainer delegatedBlobContainer, BlobContainer encryptionMetadataBlobContainer,
                SecretKey masterSecretKey) {
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.encryptionMetadataBlobContainer = encryptionMetadataBlobContainer;
            this.masterSecretKey = masterSecretKey;
        }

        @Override
        public BlobPath path() {
            return this.delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            final BytesReference dataDecryptionKeyBytes = Streams.readFully(this.encryptionMetadataBlobContainer.readBlob(blobName));
            try {
                SecretKey dataDecryptionKey = unwrapKey(BytesReference.toBytes(dataDecryptionKeyBytes), this.masterSecretKey);
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, dataDecryptionKey);
                return new CipherInputStream(this.delegatedBlobContainer.readBlob(blobName), cipher);
            } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            try {
                SecretKey dataEncryptionKey = generateRandomSecretKey();
                byte[] wrappedDataEncryptionKey = wrapKey(dataEncryptionKey, this.masterSecretKey);
                try (InputStream stream = new ByteArrayInputStream(wrappedDataEncryptionKey)) {
                    this.encryptionMetadataBlobContainer.writeBlob(blobName, stream, wrappedDataEncryptionKey.length, failIfAlreadyExists);
                }
                Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.ENCRYPT_MODE, dataEncryptionKey);
                this.delegatedBlobContainer.writeBlob(blobName, new CipherInputStream(inputStream, cipher), blobSize, failIfAlreadyExists);
            } catch (NoSuchAlgorithmException | InvalidKeyException | NoSuchPaddingException | IllegalBlockSizeException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
            // does not support atomic write
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void deleteBlob(String blobName) throws IOException {
            this.delegatedBlobContainer.deleteBlob(blobName);
            this.encryptionMetadataBlobContainer.deleteBlob(blobName);
        }

        @Override
        public void delete() throws IOException {
            this.delegatedBlobContainer.delete();
            this.encryptionMetadataBlobContainer.delete();
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
