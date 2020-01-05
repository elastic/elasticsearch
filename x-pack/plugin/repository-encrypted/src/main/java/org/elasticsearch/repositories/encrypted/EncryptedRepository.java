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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;

public class EncryptedRepository extends BlobStoreRepository {

    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_SIZE_IN_BYTES = 128;
    static final String DEK_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final String KEK_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int DEK_KEY_SIZE_IN_BITS = 256;
    static final String RAND_ALGO = "SHA1PRNG";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 1 << 20; // 1MB
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // when something about the encryption scheme changes (eg. metadata format) we increment this version number
    static final int ENCRYPTION_PROTOCOL_VERSION_NUMBER = 1;

    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private final BlobStoreRepository delegatedRepository;
    private final KeyGenerator dataEncryptionKeyGenerator;
    private final SecretKey keyEncryptionKey;
    private final SecureRandom secureRandom;

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService
            , BlobStoreRepository delegatedRepository, SecretKey keyEncryptionKey) throws NoSuchAlgorithmException {
        super(metadata, namedXContentRegistry, clusterService, delegatedRepository.basePath());
        this.delegatedRepository = delegatedRepository;
        this.dataEncryptionKeyGenerator = KeyGenerator.getInstance("AES");
        this.dataEncryptionKeyGenerator.init(DEK_KEY_SIZE_IN_BITS, SecureRandom.getInstance(RAND_ALGO));
        this.keyEncryptionKey = keyEncryptionKey;
        this.secureRandom = SecureRandom.getInstance(RAND_ALGO);
    }

    @Override
    protected BlobStore createBlobStore() {
        return new EncryptedBlobStoreDecorator(this.delegatedRepository.blobStore(), dataEncryptionKeyGenerator, keyEncryptionKey,
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
        private final SecretKey keyEncryptionKey;
        private final SecureRandom secureRandom;

        EncryptedBlobStoreDecorator(BlobStore delegatedBlobStore, KeyGenerator dataEncryptionKeyGenerator,
                                    SecretKey keyEncryptionKey, SecureRandom secureRandom) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.keyEncryptionKey = keyEncryptionKey;
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
                    delegatedBlobStore.blobContainer(encryptionMetadataBlobPath), dataEncryptionKeyGenerator, keyEncryptionKey,
                    secureRandom);
        }
    }

    private static class EncryptedBlobContainerDecorator implements BlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final SecretKey keyEncryptionKey;
        private final SecureRandom secureRandom;

        EncryptedBlobContainerDecorator(BlobContainer delegatedBlobContainer, BlobContainer encryptionMetadataBlobContainer,
                                        KeyGenerator dataEncryptionKeyGenerator, SecretKey keyEncryptionKey, SecureRandom secureRandom) {
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.encryptionMetadataBlobContainer = encryptionMetadataBlobContainer;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.keyEncryptionKey = keyEncryptionKey;
            this.secureRandom = secureRandom;
        }

        @Override
        public BlobPath path() {
            return this.delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            BytesReference encryptedMetadataBytes = Streams.readFully(this.encryptionMetadataBlobContainer.readBlob(blobName));
            final BlobEncryptionMetadata metadata = decryptMetadata(BytesReference.toBytes(encryptedMetadataBytes), keyEncryptionKey);
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
            byte[] encryptedMetadata = encryptMetadata(metadata, keyEncryptionKey, secureRandom);
            // write the encrypted metadata
            try (ByteArrayInputStream encryptedMetadataInputStream = new ByteArrayInputStream(encryptedMetadata)) {
                this.encryptionMetadataBlobContainer.writeBlob(blobName, encryptedMetadataInputStream, encryptedMetadata.length,
                        false /* overwrite any blob with the same name because it cannot correspond to any other encrypted blob */);
            }
        }

        private byte[] encryptMetadata(BlobEncryptionMetadata metadata, SecretKey keyEncryptionKey,
                                       SecureRandom secureRandom) throws IOException {
            // serialize metadata to byte[]
            final byte[] plaintextMetadata;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                try (StreamOutput out = new OutputStreamStreamOutput(baos)) {
                    metadata.writeTo(out);
                }
                plaintextMetadata = baos.toByteArray();
            }
            // create cipher for metadata encryption
            byte[] iv = new byte[GCM_IV_LENGTH_IN_BYTES];
            secureRandom.nextBytes(iv);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_IN_BYTES * Byte.SIZE, iv);
            final Cipher cipher;
            try {
                cipher = Cipher.getInstance(KEK_ENCRYPTION_SCHEME);
                cipher.init(Cipher.ENCRYPT_MODE, keyEncryptionKey, gcmParameterSpec);
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e) {
                throw new IOException("Exception while initializing KEK encryption cipher", e);
            }
            // encrypt metadata
            final byte[] encryptedMetadata;
            try {
                encryptedMetadata = cipher.doFinal(plaintextMetadata);
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new IOException("Exception while encrypting metadata and DEK", e);
            }
            // concatenate iv and metadata cipher text
            byte[] resultCiphertext = new byte[iv.length + encryptedMetadata.length];
            // prepend IV
            System.arraycopy(iv, 0, resultCiphertext, 0, iv.length);
            System.arraycopy(encryptedMetadata, 0, resultCiphertext, iv.length, encryptedMetadata.length);
            return resultCiphertext;
        }

        private BlobEncryptionMetadata decryptMetadata(byte[] encryptedMetadata, SecretKey keyEncryptionKey) throws IOException {
            // first bytes are IV
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_IN_BYTES * Byte.SIZE, encryptedMetadata, 0,
                    GCM_IV_LENGTH_IN_BYTES);
            // initialize cipher
            final Cipher cipher;
            try {
                cipher = Cipher.getInstance(KEK_ENCRYPTION_SCHEME);
                cipher.init(Cipher.DECRYPT_MODE, keyEncryptionKey, gcmParameterSpec);
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e) {
                throw new IOException("Exception while initializing KEK decryption cipher", e);
            }
            // decrypt metadata (use cipher)
            final byte[] decryptedMetadata;
            try {
                decryptedMetadata = cipher.doFinal(encryptedMetadata, GCM_IV_LENGTH_IN_BYTES,
                        encryptedMetadata.length - GCM_IV_LENGTH_IN_BYTES);
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new IOException("Exception while decrypting metadata and DEK", e);
            }
            try (ByteArrayInputStream decryptedMetadataInputStream = new ByteArrayInputStream(decryptedMetadata)) {
                return new BlobEncryptionMetadata(decryptedMetadataInputStream);
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
