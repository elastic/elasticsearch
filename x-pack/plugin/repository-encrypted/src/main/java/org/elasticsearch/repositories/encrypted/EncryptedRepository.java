/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
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
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class EncryptedRepository extends BlobStoreRepository {
    static final Logger logger = LogManager.getLogger(EncryptedRepository.class);

    // the following constants are fixed by definition
    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_SIZE_IN_BYTES = 128;
    // changing the following constants implies breaking compatibility with previous versions
    // in this case the {@link #CURRENT_ENCRYPTION_VERSION_NUMBER} MUST be incremented
    static final String DATA_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final int DATA_KEY_SIZE_IN_BITS = 256;
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 8 << 20; // 8MB
    static final int METADATA_NAME_LENGTH_IN_BYTES = 18;
    // this can be changed freely (can be made a repository parameter) without adjusting
    // the {@link #CURRENT_ENCRYPTION_VERSION_NUMBER}, as long as it stays under the value
    // of {@link #MAX_PACKET_LENGTH_IN_BYTES}
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // each snapshot metadata contains the salted password hash of the master node that started the snapshot operation
    // this hash is then verified on each data node before the actual shard snapshot as well as on the
    // master node that finalizes the snapshot (could be a different master node, if a master failover
    // has occurred in the mean time)
    private static final String PASSWORD_HASH_RESERVED_USER_METADATA_KEY = "__passwordHash";

    // The encryption scheme version number to which the current implementation conforms to.
    // The version number MUST be incremented whenever the format of the metadata, or
    // the way the metadata is used for the actual decryption are changed.
    // Incrementing the version number signals that previous implementations cannot make sense
    // of the new scheme.
    private static final int CURRENT_ENCRYPTION_VERSION_NUMBER = 2; // nobody trusts v1 of anything
    // the path of the blob container holding the encryption metadata
    // this is relative to the root path holding the encrypted blobs (i.e. the repository root path)
    private static final String ENCRYPTION_METADATA_ROOT = "encryption-metadata-v" + CURRENT_ENCRYPTION_VERSION_NUMBER;

    private final BlobStoreRepository delegatedRepository;
    private final KeyGenerator dataEncryptionKeyGenerator;
    private final PasswordBasedEncryption metadataEncryption;
    private final Supplier<Integer> encryptionNonceGenerator;
    private final Supplier<byte[]> metadataNameGenerator;
    private final String passwordPublicHash;
    private final HashVerifier passwordHashVerifier;

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService,
                                  BlobStoreRepository delegatedRepository, char[] password) throws NoSuchAlgorithmException {
        super(metadata, namedXContentRegistry, clusterService, delegatedRepository.basePath());
        this.delegatedRepository = delegatedRepository;
        this.dataEncryptionKeyGenerator = KeyGenerator.getInstance(EncryptedRepositoryPlugin.CIPHER_ALGO);
        this.dataEncryptionKeyGenerator.init(DATA_KEY_SIZE_IN_BITS, SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO));
        this.metadataEncryption = new PasswordBasedEncryption(password, SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO));
        final SecureRandom secureRandom = SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO);
        this.encryptionNonceGenerator = () -> secureRandom.nextInt();
        final Random random = new Random();
        this.metadataNameGenerator = () -> {
            byte[] randomMetadataName = new byte[METADATA_NAME_LENGTH_IN_BYTES];
            random.nextBytes(randomMetadataName);
            return randomMetadataName;
        };
        this.passwordPublicHash = computeSaltedPBKDF2Hash(password, SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO));
        this.passwordHashVerifier = new HashVerifier(password);
    }

    @Override
    public Map<String, Object> adaptUserMetadata(Map<String, Object> userMetadata) {
        Map<String, Object> snapshotUserMetadata = new HashMap<>();
        snapshotUserMetadata.putAll(userMetadata);
        // pin down the hash of the password that is checked for all the operations of this snapshot
        snapshotUserMetadata.put(PASSWORD_HASH_RESERVED_USER_METADATA_KEY, this.passwordPublicHash);
        return snapshotUserMetadata;
    }

    private void validatePasswordHash(Map<String, Object> snapshotUserMetadata, ActionListener<?> listener) {
        Object repositoryPasswordHash = snapshotUserMetadata.get(PASSWORD_HASH_RESERVED_USER_METADATA_KEY);
        if (repositoryPasswordHash == null || (false == repositoryPasswordHash instanceof String)) {
            listener.onFailure(new IllegalStateException("Snapshot metadata does not contain the password hash or is invalid"));
            return;
        }
        if (false == passwordHashVerifier.verify((String) repositoryPasswordHash)) {
            listener.onFailure(new RepositoryException(metadata.name(),
                    "Password mismatch for repository. The local node's value of the keystore secure setting [" +
                            EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(metadata.name()).getKey() +
                            "] is different from the master node that started the snapshot"));
            return;
        }
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                 boolean writeShardGens, ActionListener<SnapshotInfo> listener) {
        validatePasswordHash(userMetadata, listener);
        super.finalizeSnapshot(snapshotId, shardGenerations, startTime, failure, totalShards, shardFailures, repositoryStateId,
                includeGlobalState, clusterMetaData, userMetadata, writeShardGens, listener);
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus, boolean writeShardGens,
                              Map<String, Object> userMetadata, ActionListener<String> listener) {
        validatePasswordHash(userMetadata, listener);
        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, writeShardGens, userMetadata,
                listener);
    }

    @Override
    public boolean isReadOnly() {
        return false == EncryptedRepositoryPlugin.getLicenseState().isEncryptedSnapshotAllowed();
    }

    @Override
    public void cleanup(long repositoryStateId, boolean writeShardGens, ActionListener<RepositoryCleanupResult> listener) {
        super.cleanup(repositoryStateId, writeShardGens, ActionListener.wrap(repositoryCleanupResult -> {
            EncryptedBlobContainer encryptedBlobContainer = (EncryptedBlobContainer) blobContainer();
            cleanUpOrphanedMetadataRecursively(encryptedBlobContainer);
            listener.onResponse(repositoryCleanupResult);
        }, listener::onFailure));
    }

    private void cleanUpOrphanedMetadataRecursively(EncryptedBlobContainer encryptedBlobContainer) throws IOException{
        encryptedBlobContainer.cleanUpOrphanedMetadata();
        for (BlobContainer childEncryptedBlobContainer : encryptedBlobContainer.children().values()) {
            try {
                cleanUpOrphanedMetadataRecursively((EncryptedBlobContainer) childEncryptedBlobContainer);
            } catch(IOException e) {
                logger.warn("Exception while cleaning up [" + childEncryptedBlobContainer.path() + "]", e);
            }
        }
    }

    @Override
    protected BlobStore createBlobStore() {
        return new EncryptedBlobStore(this.delegatedRepository.blobStore(), dataEncryptionKeyGenerator, metadataEncryption,
                encryptionNonceGenerator, metadataNameGenerator);
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

    private static class EncryptedBlobStore implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final PasswordBasedEncryption metadataEncryptor;
        private final Supplier<Integer> encryptionNonceGenerator;
        private final Supplier<byte[]> metadataNameGenerator;

        EncryptedBlobStore(BlobStore delegatedBlobStore, KeyGenerator dataEncryptionKeyGenerator,
                           PasswordBasedEncryption metadataEncryptor, Supplier<Integer> encryptionNonceGenerator,
                           Supplier<byte[]> metadataNameGenerator) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.metadataEncryptor = metadataEncryptor;
            this.encryptionNonceGenerator = encryptionNonceGenerator;
            this.metadataNameGenerator = metadataNameGenerator;
        }

        @Override
        public void close() throws IOException {
            delegatedBlobStore.close();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new EncryptedBlobContainer(delegatedBlobStore, path, dataEncryptionKeyGenerator, metadataEncryptor,
                    encryptionNonceGenerator, metadataNameGenerator);
        }
    }

    private static class EncryptedBlobContainer implements BlobContainer {

        private final BlobStore delegatedBlobStore;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final PasswordBasedEncryption metadataEncryption;
        private final Supplier<Integer> encryptionNonceGenerator;
        private final Supplier<byte[]> metadataNameGenerator;
        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;

        EncryptedBlobContainer(BlobStore delegatedBlobStore, BlobPath path, KeyGenerator dataEncryptionKeyGenerator,
                               PasswordBasedEncryption metadataEncryption, Supplier<Integer> encryptionNonceGenerator,
                               Supplier<byte[]> metadataNameGenerator) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.metadataEncryption = metadataEncryption;
            this.encryptionNonceGenerator = encryptionNonceGenerator;
            this.metadataNameGenerator = metadataNameGenerator;
            this.delegatedBlobContainer = delegatedBlobStore.blobContainer(path);
            this.encryptionMetadataBlobContainer = delegatedBlobStore.blobContainer(path.prepend(ENCRYPTION_METADATA_ROOT));
        }

        @Override
        public BlobPath path() {
            return delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            final InputStream encryptedDataInputStream = delegatedBlobContainer.readBlob(blobName);
            // read the metadata name which is prefixed to the encrypted blob
            final byte[] metadataNameBytes = encryptedDataInputStream.readNBytes(METADATA_NAME_LENGTH_IN_BYTES);
            if (metadataNameBytes.length != METADATA_NAME_LENGTH_IN_BYTES) {
                throw new IOException("Failure to read encrypted blob metadata name");
            }
            final String metadataName = new String(Base64.getUrlEncoder().withoutPadding().encode(metadataNameBytes),
                    StandardCharsets.UTF_8);
            // read metadata
            BytesReference encryptedMetadataBytes = Streams.readFully(encryptionMetadataBlobContainer.readBlob(metadataName));
            final BlobEncryptionMetadata metadata;
            try {
                // decrypt and parse metadata
                metadata = BlobEncryptionMetadata.deserializeMetadata(BytesReference.toBytes(encryptedMetadataBytes),
                        metadataEncryption::decrypt);
            } catch (IOException e) {
                // friendlier exception message
                String failureMessage = "Failure to decrypt metadata for blob [" + blobName + "]";
                if (e.getCause() instanceof AEADBadTagException) {
                    failureMessage = failureMessage + ". The repository password is probably wrong.";
                }
                throw new IOException(failureMessage, e);
            }
            // read and decrypt blob
            return new DecryptionPacketsInputStream(encryptedDataInputStream, metadata.getDataEncryptionKey(), metadata.getNonce(),
                    metadata.getPacketLengthInBytes());
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            final SecretKey dataEncryptionKey = dataEncryptionKeyGenerator.generateKey();
            final int nonce = encryptionNonceGenerator.get();
            // this is the metadata required to decrypt back the (to be) encrypted blob
            BlobEncryptionMetadata metadata = new BlobEncryptionMetadata(nonce, PACKET_LENGTH_IN_BYTES, dataEncryptionKey);
            // encrypt metadata
            final byte[] encryptedMetadata;
            try {
                encryptedMetadata = BlobEncryptionMetadata.serializeMetadata(metadata, metadataEncryption::encrypt);
            } catch (IOException e) {
                throw new IOException("Failure to encrypt metadata for blob [" + blobName + "]", e);
            }
            final byte[] metadataNameBytes = metadataNameGenerator.get();
            final String metadataName = new String(Base64.getUrlEncoder().withoutPadding().encode(metadataNameBytes),
                    StandardCharsets.UTF_8);
            // first write the encrypted metadata to a UNIQUE blob name
            try (ByteArrayInputStream encryptedMetadataInputStream = new ByteArrayInputStream(encryptedMetadata)) {
                encryptionMetadataBlobContainer.writeBlob(metadataName, encryptedMetadataInputStream, encryptedMetadata.length, true);
            }
            // afterwards overwrite the encrypted data blob
            // prepended to the encrypted data blob is the unique name of the metadata blob
            final long encryptedBlobSize = metadataNameBytes.length + EncryptionPacketsInputStream.getEncryptionLength(blobSize,
                    PACKET_LENGTH_IN_BYTES);
            try (InputStream encryptedInputStream = ChainingInputStream.chain(new ByteArrayInputStream(metadataNameBytes),
                    new EncryptionPacketsInputStream(inputStream, dataEncryptionKey, nonce, PACKET_LENGTH_IN_BYTES), true)) {
                delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedBlobSize, failIfAlreadyExists);
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
            // the encrypted repository does not offer an alternative implementation for atomic writes
            // fallback to regular write
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            // first delete the encrypted data blob
            DeleteResult deleteResult = delegatedBlobContainer.delete();
            // then delete metadata
            encryptionMetadataBlobContainer.delete();
            return deleteResult;
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            // first delete the encrypted data blob
            delegatedBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
            // then delete metadata
            encryptionMetadataBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetaData> listBlobs() throws IOException {
            // the encrypted data blob container is the source-of-truth for list operations
            // the metadata blob container mirrors its structure, but in some failure cases it might contain
            // additional orphaned metadata blobs
            // can list blobs that cannot be decrypted (because metadata is missing or corrupted)
            return delegatedBlobContainer.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            // the encrypted data blob container is the source-of-truth for child container operations
            // the metadata blob container mirrors its structure, but in some failure cases it might contain
            // additional orphaned metadata blobs
            Map<String, BlobContainer> childEncryptedBlobContainers = delegatedBlobContainer.children();
            Map<String, BlobContainer> result = new HashMap<>(childEncryptedBlobContainers.size());
            for (Map.Entry<String, BlobContainer> encryptedBlobContainer : childEncryptedBlobContainers.entrySet()) {
                // get an encrypted blob container for each
                result.put(encryptedBlobContainer.getKey(), new EncryptedBlobContainer(delegatedBlobStore,
                        encryptedBlobContainer.getValue().path(), dataEncryptionKeyGenerator, metadataEncryption,
                        encryptionNonceGenerator, metadataNameGenerator));
            }
            return result;
        }

        @Override
        public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            // the encrypted data blob container is the source-of-truth for list operations
            // the metadata blob container mirrors its structure, but in some failure cases it might contain
            // additional orphaned metadata blobs
            // can list blobs that cannot be decrypted (because metadata is missing or corrupted)
            return delegatedBlobContainer.listBlobsByPrefix(blobNamePrefix);
        }

        public void cleanUpOrphanedMetadata() throws IOException{
            // delete encryption metadata blobs which don't pair with any data blobs
            Set<String> foundEncryptedBlobs = delegatedBlobContainer.listBlobs().keySet();
            Set<String> foundMetadataBlobs = encryptionMetadataBlobContainer.listBlobs().keySet();
            List<String> orphanedMetadataBlobs = new ArrayList<>(foundMetadataBlobs);
            orphanedMetadataBlobs.removeAll(foundEncryptedBlobs);
            try {
                encryptionMetadataBlobContainer.deleteBlobsIgnoringIfNotExists(orphanedMetadataBlobs);
            } catch (IOException e) {
                logger.warn("Exception while deleting orphaned metadata blobs " + orphanedMetadataBlobs, e);
            }
            // delete Encryption metadata blob containers which don't par with any data blob containers
            Set<String> foundEncryptedBlobContainers = delegatedBlobContainer.children().keySet();
            Map<String, BlobContainer> foundMetadataBlobContainers = encryptionMetadataBlobContainer.children();
            for (Map.Entry<String, BlobContainer> metadataBlobContainer : foundMetadataBlobContainers.entrySet()) {
                if (false == foundEncryptedBlobContainers.contains(metadataBlobContainer.getKey())) {
                    try {
                        metadataBlobContainer.getValue().delete();
                    } catch (IOException e) {
                        logger.warn("Exception while deleting orphaned metadata blob container [" + metadataBlobContainer + "]", e);
                    }
                }
            }
        }
    }

    private static String computeSaltedPBKDF2Hash(char[] password, SecureRandom secureRandom) {
        byte[] salt = new byte[16];
        secureRandom.nextBytes(salt);
        return computeSaltedPBKDF2Hash(salt, password);
    }

    private static String computeSaltedPBKDF2Hash(byte[] salt, char[] password) {
        final int iterations = 10000;
        final int keyLength = 512;
        final PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, keyLength);
        final byte[] hash;
        try {
            SecretKeyFactory pbkdf2KeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            hash = pbkdf2KeyFactory.generateSecret(spec).getEncoded();
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Unexpected exception when computing PBKDF2 hash for the repository password", e);
        }
        return new String(Base64.getEncoder().encode(salt), StandardCharsets.UTF_8) + ":" +
                new String(Base64.getEncoder().encode(hash), StandardCharsets.UTF_8);
    }

    private static class HashVerifier {
        private final char[] password;
        private final AtomicReference<String> lastVerifiedHash;

        HashVerifier(char[] password) {
            this.password = password;
            this.lastVerifiedHash = new AtomicReference<>(null);
        }

        boolean verify(String saltedHash) {
            Objects.requireNonNull(saltedHash);
            if (saltedHash.equals(lastVerifiedHash.get())) {
                return true;
            }
            String[] parts = saltedHash.split(":");
            if (parts == null || parts.length != 2) {
                return false;
            }
            String salt = parts[0];
            String computedHash = computeSaltedPBKDF2Hash(Base64.getDecoder().decode(salt.getBytes(StandardCharsets.UTF_8)), password);
            if (false == computedHash.equals(saltedHash)) {
                return false;
            }
            lastVerifiedHash.set(computedHash);
            return true;
        }
    }

}
