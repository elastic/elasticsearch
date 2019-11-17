/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.crypto.fips.FipsUnapprovedOperationError;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class EncryptedRepository extends BlobStoreRepository {

    private static BouncyCastleFipsProvider BC_FIPS_PROVIDER;
     static {
         AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
             EncryptedRepository.BC_FIPS_PROVIDER = new BouncyCastleFipsProvider();
             return null;
         });
     }
    // TODO change this and make it (secure) randomly generated for each blob
    private static final int NONCE = 5318008;
    private static final int ENCRYPTION_PROTOCOL_VERSION_NUMBER = 1;

    static final Setting.AffixSetting<SecureString> ENCRYPTION_PASSWORD_SETTING = Setting.affixKeySetting("repository.encrypted.",
            "password", key -> SecureSetting.secureString(key, null));

    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity());
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
                SecretKey secretKey = generateSecretKeyFromPassword(cachedRepositoryPasswords.get(metaData.name()));
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
            encryptionMetadataBlobPath = encryptionMetadataBlobPath.add(ENCRYPTION_METADATA_PREFIX + keyId(this.masterSecretKey));
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
            final SecretKey dataDecryptionKey;
            try {
                dataDecryptionKey = unwrapKey(BytesReference.toBytes(dataDecryptionKeyBytes), this.masterSecretKey);
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                throw new IOException(e);
            }
            return GCMPacketsCipherInputStream.getDecryptor(this.delegatedBlobContainer.readBlob(blobName), dataDecryptionKey, NONCE,
                    BC_FIPS_PROVIDER);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            final byte[] wrappedDataEncryptionKey;
            final SecretKey dataEncryptionKey;
            try {
                dataEncryptionKey = generateRandomSecretKey();
                wrappedDataEncryptionKey = wrapKey(dataEncryptionKey, this.masterSecretKey);
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException e) {
                throw new IOException(e);
            }
            try (InputStream stream = new ByteArrayInputStream(wrappedDataEncryptionKey)) {
                this.encryptionMetadataBlobContainer.writeBlob(blobName, stream, wrappedDataEncryptionKey.length, failIfAlreadyExists);
            }
            final long encryptedSize = GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(blobSize);
            try (InputStream encryptedInputStream = GCMPacketsCipherInputStream.getEncryptor(inputStream, dataEncryptionKey, NONCE,
                    BC_FIPS_PROVIDER)) {
                this.delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedSize, failIfAlreadyExists);
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
        public DeleteResult delete() throws IOException {
            DeleteResult result = this.delegatedBlobContainer.delete();
            this.encryptionMetadataBlobContainer.delete();
            return result;
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
            Map<String, BlobMetaData> delegatedBlobs = this.delegatedBlobContainer.listBlobsByPrefix(blobNamePrefix);
            Map<String, BlobMetaData> delegatedBlobsWithPlainSize = new HashMap<>(delegatedBlobs.size());
            for (Map.Entry<String, BlobMetaData> entry : delegatedBlobs.entrySet()) {
                delegatedBlobsWithPlainSize.put(entry.getKey(), new BlobMetaData() {

                    @Override
                    public String name() {
                        return entry.getValue().name();
                    }

                    @Override
                    public long length() {
                        return GCMPacketsCipherInputStream.getDecryptionSizeFromCipherSize(entry.getValue().length());
                    }
                });
            }
            return delegatedBlobsWithPlainSize;
        }
    }

    private static SecretKey generateSecretKeyFromPassword(char[] password) throws NoSuchAlgorithmException, InvalidKeySpecException {
        byte[] salt = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}; // same salt for 1:1 password to key
        PBEKeySpec spec = new PBEKeySpec(password, salt, 65536, 256);
        try {
            SecretKey tmp = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256", BC_FIPS_PROVIDER).generateSecret(spec);
            return new SecretKeySpec(tmp.getEncoded(), "AES");
        } catch (FipsUnapprovedOperationError e) {
            // password must be at least 112 bits
            // wrap the error into an exception because the exception is better handled up the stack
            // TODO don't do this wrapping
            throw new RuntimeException(e);
        }
    }

    private static String keyId(SecretKey secretKey) {
        return MessageDigests.toHexString(MessageDigests.sha256().digest(secretKey.getEncoded()));
    }

    private static SecretKey generateRandomSecretKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES", BC_FIPS_PROVIDER);
        keyGen.init(256, SecureRandom.getInstance("DEFAULT", BC_FIPS_PROVIDER));
        return keyGen.generateKey();
    }

    private static byte[] wrapKey(SecretKey toWrap, SecretKey keyWrappingKey)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException {
        Cipher cipher = Cipher.getInstance("AESWrap", BC_FIPS_PROVIDER);
        cipher.init(Cipher.WRAP_MODE, keyWrappingKey);
        return cipher.wrap(toWrap);
    }

    private static SecretKey unwrapKey(byte[] toUnwrap, SecretKey keyEncryptionKey)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        Cipher cipher = Cipher.getInstance("AESWrap", BC_FIPS_PROVIDER);
        cipher.init(Cipher.UNWRAP_MODE, keyEncryptionKey);
        return (SecretKey) cipher.unwrap(toUnwrap, "AES", Cipher.SECRET_KEY);
    }
}
