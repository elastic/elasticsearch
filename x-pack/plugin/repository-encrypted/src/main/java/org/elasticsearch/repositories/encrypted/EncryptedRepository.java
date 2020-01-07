/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EncryptedRepository extends BlobStoreRepository {

    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_SIZE_IN_BYTES = 128;
    static final String DEK_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final String KEK_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int DEK_KEY_SIZE_IN_BITS = 256;
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 1 << 20; // 1MB
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // when something about the encryption scheme changes (eg. metadata format) we increment this version number
    static final int ENCRYPTION_PROTOCOL_VERSION_NUMBER = 1;

    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private final BlobStoreRepository delegatedRepository;
    private final KeyGenerator dataEncryptionKeyGenerator;
    private final PasswordBasedEncryptor metadataEncryptor;
    private final SecureRandom secureRandom;

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService
            , BlobStoreRepository delegatedRepository, PasswordBasedEncryptor metadataEncryptor) throws NoSuchAlgorithmException {
        super(metadata, namedXContentRegistry, clusterService, delegatedRepository.basePath());
        this.delegatedRepository = delegatedRepository;
        this.dataEncryptionKeyGenerator = KeyGenerator.getInstance(EncryptedRepositoryPlugin.CIPHER_ALGO);
        this.dataEncryptionKeyGenerator.init(DEK_KEY_SIZE_IN_BITS, SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO));
        this.metadataEncryptor = metadataEncryptor;
        this.secureRandom = SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO);
    }

    @Override
    protected BlobStore createBlobStore() {
        return new EncryptedBlobStoreDecorator(this.delegatedRepository.blobStore(), dataEncryptionKeyGenerator, metadataEncryptor,
                secureRandom);
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

    private static class EncryptedBlobStoreDecorator implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final PasswordBasedEncryptor metadataEncryptor;
        private final SecureRandom secureRandom;

        EncryptedBlobStoreDecorator(BlobStore delegatedBlobStore, KeyGenerator dataEncryptionKeyGenerator,
                                    PasswordBasedEncryptor metadataEncryptor, SecureRandom secureRandom) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.metadataEncryptor = metadataEncryptor;
            this.secureRandom = secureRandom;
        }

        @Override
        public void close() throws IOException {
            delegatedBlobStore.close();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            BlobPath encryptionMetadataBlobPath = BlobPath.cleanPath();
            encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(ENCRYPTION_METADATA_PREFIX + ENCRYPTION_PROTOCOL_VERSION_NUMBER);
            for (String pathComponent : path) {
                encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(pathComponent);
            }
            return new EncryptedBlobContainerDecorator(delegatedBlobStore.blobContainer(path),
                    delegatedBlobStore.blobContainer(encryptionMetadataBlobPath), dataEncryptionKeyGenerator, metadataEncryptor,
                    secureRandom);
        }
    }

    private static class EncryptedBlobContainerDecorator implements BlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final PasswordBasedEncryptor metadataEncryptor;
        private final SecureRandom secureRandom;

        EncryptedBlobContainerDecorator(BlobContainer delegatedBlobContainer, BlobContainer encryptionMetadataBlobContainer,
                                        KeyGenerator dataEncryptionKeyGenerator, PasswordBasedEncryptor metadataEncryptor,
                                        SecureRandom secureRandom) {
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.encryptionMetadataBlobContainer = encryptionMetadataBlobContainer;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.metadataEncryptor = metadataEncryptor;
            this.secureRandom = secureRandom;
        }

        @Override
        public BlobPath path() {
            return this.delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            BytesReference encryptedMetadataBytes = Streams.readFully(this.encryptionMetadataBlobContainer.readBlob(blobName));
            final byte[] decryptedMetadata;
            try {
                decryptedMetadata = metadataEncryptor.decrypt(BytesReference.toBytes(encryptedMetadataBytes));
            } catch (ExecutionException | GeneralSecurityException e) {
                throw new IOException("Exception while decrypting metadata");
            }
            final BlobEncryptionMetadata metadata = BlobEncryptionMetadata.deserializeMetadataFromByteArray(decryptedMetadata);
            SecretKey dataDecryptionKey = new SecretKeySpec(metadata.getDataEncryptionKeyMaterial(), 0,
                    metadata.getDataEncryptionKeyMaterial().length, "AES");
            return new DecryptionPacketsInputStream(this.delegatedBlobContainer.readBlob(blobName), dataDecryptionKey,
                    metadata.getNonce(), metadata.getPacketLengthInBytes());
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            SecretKey dataEncryptionKey = dataEncryptionKeyGenerator.generateKey();
            int nonce = secureRandom.nextInt();
            // first write the encrypted blob
            long encryptedBlobSize = EncryptionPacketsInputStream.getEncryptionLength(blobSize, PACKET_LENGTH_IN_BYTES);
            try (EncryptionPacketsInputStream encryptedInputStream = new EncryptionPacketsInputStream(inputStream,
                    dataEncryptionKey, nonce, PACKET_LENGTH_IN_BYTES)) {
                this.delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedBlobSize, failIfAlreadyExists);
            }
            // metadata required to decrypt back the encrypted blob
            BlobEncryptionMetadata metadata = new BlobEncryptionMetadata(dataEncryptionKey.getEncoded(), nonce, PACKET_LENGTH_IN_BYTES);
            final byte[] encryptedMetadata;
            try {
                encryptedMetadata = metadataEncryptor.encrypt(BlobEncryptionMetadata.serializeMetadataToByteArray(metadata));
            } catch (ExecutionException | GeneralSecurityException e) {
                throw new IOException("Exception while encrypting metadata");
            }
            // write the encrypted metadata
            try (ByteArrayInputStream encryptedMetadataInputStream = new ByteArrayInputStream(encryptedMetadata)) {
                this.encryptionMetadataBlobContainer.writeBlob(blobName, encryptedMetadataInputStream, encryptedMetadata.length,
                        false /* overwrite any blob with the same name because it cannot correspond to any other encrypted blob */);
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
            // does not support atomic write
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            this.encryptionMetadataBlobContainer.delete();
            return this.delegatedBlobContainer.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            this.encryptionMetadataBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
            this.delegatedBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
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
