/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.cms.CMSObjectIdentifiers;
import org.bouncycastle.cms.CMSAlgorithm;
import org.bouncycastle.cms.CMSEnvelopedData;
import org.bouncycastle.cms.CMSEnvelopedDataGenerator;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.CMSTypedData;
import org.bouncycastle.cms.PasswordRecipientInfoGenerator;
import org.bouncycastle.cms.PasswordRecipientInformation;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.RecipientInformationStore;
import org.bouncycastle.cms.jcajce.JceCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JcePasswordEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JcePasswordRecipientInfoGenerator;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.Repository;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class EncryptedRepository extends BlobStoreRepository {

    static final int ENCRYPTION_PROTOCOL_VERSION_NUMBER = 1;
    // this is chosen somewhat arbitrarily. Assuming no mark calls during encryption/upload/snapshot, a larger value increases
    // the metadata/data size ratio, thereby reducing the encryption overhead, but it also requires that decryption/download/restore
    // allocate and use a buffer of this size.
    static final int MAX_PACKET_SIZE = 64 * 1024; // 64KB packet sizes
    static final int GCM_TAG_SIZE_IN_BYTES = 16;
    static final int GCM_IV_SIZE_IN_BYTES = 12;
    static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int AES_BLOCK_SIZE_IN_BYTES = 128;

    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "password", key -> SecureSetting.secureString(key, null));

    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity());
    private static final String ENCRYPTION_METADATA_PREFIX = "encryption-metadata-";

    private final BlobStoreRepository delegatedRepository;
    private final char[] masterPassword;

    protected EncryptedRepository(BlobStoreRepository delegatedRepository, char[] masterPassword) {
        super(delegatedRepository);
        this.delegatedRepository = delegatedRepository;
        this.masterPassword = masterPassword;
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new EncryptedBlobStoreDecorator(this.delegatedRepository.blobStore(), masterPassword);
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
    public static Repository.Factory newRepositoryFactory(final Settings settings) {
        final Map<String, char[]> cachedRepositoryPasswords = new HashMap<>();
        for (String repositoryName : ENCRYPTION_PASSWORD_SETTING.getNamespaces(settings)) {
            Setting<SecureString> encryptionPasswordSetting = ENCRYPTION_PASSWORD_SETTING
                    .getConcreteSettingForNamespace(repositoryName);
            SecureString encryptionPassword = encryptionPasswordSetting.get(settings);
            cachedRepositoryPasswords.put(repositoryName, encryptionPassword.getChars());
        }
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

                if (false == cachedRepositoryPasswords.containsKey(metaData.name())) {
                    throw new IllegalArgumentException(
                            ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(metaData.name()).getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                Repository delegatedRepository = factory.create(new RepositoryMetaData(metaData.name(),
                        delegateType, metaData.settings()));
                if (false == (delegatedRepository instanceof BlobStoreRepository)) {
                    throw new IllegalArgumentException("Unsupported type " + DELEGATE_TYPE.getKey());
                }
                char[] masterPassword = cachedRepositoryPasswords.get(metaData.name());
                return new EncryptedRepository((BlobStoreRepository) delegatedRepository, masterPassword);
            }
        };
    }

    private static class EncryptedBlobStoreDecorator implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final char[] masterPassword;

        EncryptedBlobStoreDecorator(BlobStore blobStore, char[] masterPassword) {
            this.delegatedBlobStore = blobStore;
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
                    this.delegatedBlobStore.blobContainer(encryptionMetadataBlobPath), this.masterPassword);
        }
    }

    private static class EncryptedBlobContainerDecorator implements BlobContainer {

        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;
        private final char[] masterPassword;

        EncryptedBlobContainerDecorator(BlobContainer delegatedBlobContainer, BlobContainer encryptionMetadataBlobContainer,
                                        char[] masterPassword) {
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.encryptionMetadataBlobContainer = encryptionMetadataBlobContainer;
            this.masterPassword = masterPassword;
        }

        @Override
        public BlobPath path() {
            return this.delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            BytesReference encryptedMetadataBytes = Streams.readFully(this.encryptionMetadataBlobContainer.readBlob(blobName));
            BlobEncryptionMetadata metadata = decryptMetadata(BytesReference.toBytes(encryptedMetadataBytes));
            SecretKey dataEncryptionKey = new SecretKeySpec(metadata.getDataEncryptionKeyMaterial(), 0,
                    metadata.getDataEncryptionKeyMaterial().length, "AES");
            return new GCMPacketsDecryptorInputStream(this.delegatedBlobContainer.readBlob(blobName), dataEncryptionKey,
                    metadata.getMaxPacketSizeInBytes(), metadata.getAuthTagSizeInBytes(), metadata.getPacketsInfoList().iterator());
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            final SecretKey dataEncryptionKey;
            try {
                dataEncryptionKey = generateRandomSecretKey();
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
            GCMPacketsEncryptorInputStream encryptedInputStream = new GCMPacketsEncryptorInputStream(inputStream, dataEncryptionKey,
                    MAX_PACKET_SIZE);
            try {
                this.delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, blobSize, failIfAlreadyExists);
            } finally {
                encryptedInputStream.close();
            }
            List<BlobEncryptionMetadata.PacketInfo> packetsMetadataList = encryptedInputStream.getEncryptionPacketMetadata();
            BlobEncryptionMetadata metadata = new BlobEncryptionMetadata(MAX_PACKET_SIZE, GCM_IV_SIZE_IN_BYTES, GCM_TAG_SIZE_IN_BYTES,
                    dataEncryptionKey.getEncoded(), packetsMetadataList);
            byte[] encryptedMetadata = encryptMetadata(metadata);
            try (InputStream stream = new ByteArrayInputStream(encryptedMetadata)) {
                this.encryptionMetadataBlobContainer.writeBlob(blobName, stream, encryptedMetadata.length, false);
            }
        }

        private byte[] encryptMetadata(BlobEncryptionMetadata metadata) throws IOException {
            CMSEnvelopedDataGenerator envelopedDataGenerator = new CMSEnvelopedDataGenerator();
            PasswordRecipientInfoGenerator passwordRecipientInfoGenerator = new JcePasswordRecipientInfoGenerator(CMSAlgorithm.AES256_GCM
                    , masterPassword);
            envelopedDataGenerator.addRecipientInfoGenerator(passwordRecipientInfoGenerator);
            final CMSEnvelopedData envelopedData;
            try {
                envelopedData = envelopedDataGenerator.generate(new CMSTypedData() {
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
            } catch (CMSException e) {
                throw new IOException(e);
            }
            return envelopedData.getEncoded();
        }

        private BlobEncryptionMetadata decryptMetadata(byte[] metadata) throws IOException {
            final CMSEnvelopedData envelopedData;
            try {
                envelopedData = new CMSEnvelopedData(metadata);
            } catch (CMSException e) {
                throw new IOException(e);
            }
            RecipientInformationStore recipients = envelopedData.getRecipientInfos();
            if (recipients.getRecipients().size() != 1) {
                throw new IllegalStateException();
            }
            RecipientInformation recipient = recipients.iterator().next();
            if (false == (recipient instanceof PasswordRecipientInformation)) {
                throw new IllegalStateException();
            }
            final byte[] decryptedMetadata;
            try {
                decryptedMetadata = recipient.getContent(new JcePasswordEnvelopedRecipient(masterPassword));
            } catch (CMSException e) {
                throw new IOException(e);
            }
            return new BlobEncryptionMetadata(new ByteArrayInputStream(decryptedMetadata));
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
            // does not support atomic write
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void deleteBlob(String blobName) throws IOException {
            this.encryptionMetadataBlobContainer.deleteBlob(blobName);
            this.delegatedBlobContainer.deleteBlob(blobName);
        }

        @Override
        public DeleteResult delete() throws IOException {
            this.encryptionMetadataBlobContainer.delete();
            return this.delegatedBlobContainer.delete();
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

    private static SecretKey generateRandomSecretKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, SecureRandom.getInstance("DEFAULT"));
        return keyGen.generateKey();
    }

}
