/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.cms.CMSAlgorithm;
import org.bouncycastle.cms.CMSEnvelopedData;
import org.bouncycastle.cms.CMSEnvelopedDataGenerator;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.CMSTypedData;
import org.bouncycastle.cms.PasswordRecipientId;
import org.bouncycastle.cms.PasswordRecipientInfoGenerator;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.RecipientInformationStore;
import org.bouncycastle.cms.jcajce.JceCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JcePasswordEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JcePasswordRecipientInfoGenerator;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;

public class EncryptedRepository extends BlobStoreRepository {
    static final int GCM_TAG_SIZE_IN_BYTES = 16;
    static final int GCM_IV_SIZE_IN_BYTES = 12;
    static final int AES_BLOCK_SIZE_IN_BYTES = 128;
    static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 1 << 20; // 1MB
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // when something about the encryption scheme changes (eg. metadata format) we increment this version number
    static final int ENCRYPTION_PROTOCOL_VERSION_NUMBER = 1;

    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private final BlobStoreRepository delegatedRepository;
    private final char[] masterPassword;
    private final KeyGenerator keyGenerator;
    private final SecureRandom secureRandom;

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService
            , BlobStoreRepository delegatedRepository, char[] materPassword) throws NoSuchAlgorithmException {
        super(metadata, namedXContentRegistry, clusterService, delegatedRepository.basePath());
        this.delegatedRepository = delegatedRepository;
        this.masterPassword = materPassword;
        this.keyGenerator = KeyGenerator.getInstance("AES");
        this.keyGenerator.init(256, SecureRandom.getInstance("SHA1PRNG"));
        this.secureRandom = SecureRandom.getInstance("SHA1PRNG");
        // TODO run self-test to make sure encryption/decryption works correctly on this JVM
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new EncryptedBlobStoreDecorator(this.delegatedRepository.blobStore(), keyGenerator, secureRandom, masterPassword);
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
        private final KeyGenerator keyGenerator;
        private final SecureRandom secureRandom;
        private final char[] masterPassword;

        EncryptedBlobStoreDecorator(BlobStore delegatedBlobStore, KeyGenerator keyGenerator, SecureRandom secureRandom,
                                    char[] masterPassword) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.keyGenerator = keyGenerator;
            this.secureRandom = secureRandom;
            this.masterPassword = masterPassword;
        }

        @Override
        public void close() throws IOException {
            this.delegatedBlobStore.close();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            BlobPath encryptionMetadataBlobPath = BlobPath.cleanPath();
            encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(ENCRYPTION_METADATA_PREFIX + ENCRYPTION_PROTOCOL_VERSION_NUMBER);
            for (String pathComponent : path) {
                encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(pathComponent);
            }
            return new EncryptedBlobContainerDecorator(this.delegatedBlobStore.blobContainer(path),
                    this.delegatedBlobStore.blobContainer(encryptionMetadataBlobPath), this.keyGenerator, this.secureRandom,
                    this.masterPassword);
        }
    }

    private static class EncryptedBlobContainerDecorator implements BlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;
        private final KeyGenerator keyGenerator;
        private final SecureRandom secureRandom;
        private final char[] masterPassword;

        EncryptedBlobContainerDecorator(BlobContainer delegatedBlobContainer, BlobContainer encryptionMetadataBlobContainer,
                                        KeyGenerator keyGenerator, SecureRandom secureRandom, char[] masterPassword) {
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.encryptionMetadataBlobContainer = encryptionMetadataBlobContainer;
            this.masterPassword = masterPassword;
            this.secureRandom = secureRandom;
            this.keyGenerator = keyGenerator;
        }

        @Override
        public BlobPath path() {
            return this.delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            BytesReference encryptedMetadataBytes = Streams.readFully(this.encryptionMetadataBlobContainer.readBlob(blobName));
            final BlobEncryptionMetadata metadata;
            try {
                metadata = decryptMetadata(BytesReference.toBytes(encryptedMetadataBytes));
            } catch (CMSException e) {
                throw new IOException(e);
            }
            SecretKey dataDecryptionKey = new SecretKeySpec(metadata.getDataEncryptionKeyMaterial(), 0,
                    metadata.getDataEncryptionKeyMaterial().length, "AES");
            return new DecryptionPacketsInputStream(this.delegatedBlobContainer.readBlob(blobName), dataDecryptionKey,
                    metadata.getNonce(), metadata.getPacketLengthInBytes());
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            SecretKey dataEncryptionKey = keyGenerator.generateKey();
            int nonce = secureRandom.nextInt();
            long encryptedBlobSize = EncryptionPacketsInputStream.getEncryptionSize(blobSize, PACKET_LENGTH_IN_BYTES);
            try (EncryptionPacketsInputStream encryptedInputStream = new EncryptionPacketsInputStream(inputStream,
                    dataEncryptionKey, nonce, PACKET_LENGTH_IN_BYTES)) {
                this.delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedBlobSize, failIfAlreadyExists);
            }
            BlobEncryptionMetadata metadata = new BlobEncryptionMetadata(dataEncryptionKey.getEncoded(), nonce, PACKET_LENGTH_IN_BYTES);
            final byte[] encryptedMetadata;
            try {
                encryptedMetadata = encryptMetadata(metadata);
            } catch (CMSException e) {
                throw new IOException(e);
            }
            try (InputStream encryptedMetadataInputStream = new ByteArrayInputStream(encryptedMetadata)) {
                this.encryptionMetadataBlobContainer.writeBlob(blobName, encryptedMetadataInputStream, encryptedMetadata.length, false);
            }
        }

        private byte[] encryptMetadata(BlobEncryptionMetadata metadata) throws IOException, CMSException {
            CMSEnvelopedDataGenerator envelopedDataGenerator = new CMSEnvelopedDataGenerator();
            PasswordRecipientInfoGenerator passwordRecipientInfoGenerator = new JcePasswordRecipientInfoGenerator(CMSAlgorithm.AES256_GCM
                    , masterPassword);
            envelopedDataGenerator.addRecipientInfoGenerator(passwordRecipientInfoGenerator);
            final CMSEnvelopedData envelopedData = envelopedDataGenerator.generate(new CMSTypedData() {
                @Override
                public ASN1ObjectIdentifier getContentType() {
                    return CMSObjectIdentifiers.data;
                }

                @Override
                public void write(OutputStream out) throws IOException, CMSException {
                    metadata.write(out);
                }

                @Override
                public Object getContent() {
                    return metadata;
                }
            }, new JceCMSContentEncryptorBuilder(CMSAlgorithm.AES256_GCM).build());
            return envelopedData.getEncoded();
        }

        private BlobEncryptionMetadata decryptMetadata(byte[] metadata) throws CMSException, IOException {
            final CMSEnvelopedData envelopedData = new CMSEnvelopedData(metadata);
            RecipientInformationStore recipients = envelopedData.getRecipientInfos();
            RecipientInformation recipient = recipients.get(new PasswordRecipientId());
            if (recipient == null) {
                throw new IllegalArgumentException();
            }
            final byte[] decryptedMetadata = recipient.getContent(new JcePasswordEnvelopedRecipient(masterPassword));
            return new BlobEncryptionMetadata(new ByteArrayInputStream(decryptedMetadata));
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
