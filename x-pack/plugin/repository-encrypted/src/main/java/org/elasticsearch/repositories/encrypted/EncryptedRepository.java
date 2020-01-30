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
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ConsistentSettingsService;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
    // this can be changed freely (can be made a repository parameter) without adjusting
    // the {@link #CURRENT_ENCRYPTION_VERSION_NUMBER}, as long as it stays under the value
    // of {@link #MAX_PACKET_LENGTH_IN_BYTES}
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB

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
    private final PasswordBasedEncryption metadataEncryptor;
    private final ConsistentSettingsService consistentSettingsService;
    private final SecureRandom secureRandom;
    private final SecureSetting<?> passwordSettingForThisRepo;

    protected EncryptedRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, ClusterService clusterService,
                                  BlobStoreRepository delegatedRepository, PasswordBasedEncryption metadataEncryptor,
                                  ConsistentSettingsService consistentSettingsService) throws NoSuchAlgorithmException {
        super(metadata, namedXContentRegistry, clusterService, delegatedRepository.basePath());
        this.delegatedRepository = delegatedRepository;
        this.dataEncryptionKeyGenerator = KeyGenerator.getInstance(EncryptedRepositoryPlugin.CIPHER_ALGO);
        this.dataEncryptionKeyGenerator.init(DATA_KEY_SIZE_IN_BITS, SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO));
        this.metadataEncryptor = metadataEncryptor;
        this.consistentSettingsService = consistentSettingsService;
        this.secureRandom = SecureRandom.getInstance(EncryptedRepositoryPlugin.RAND_ALGO);
        this.passwordSettingForThisRepo =
                (SecureSetting<?>) EncryptedRepositoryPlugin.ENCRYPTION_PASSWORD_SETTING.getConcreteSettingForNamespace(metadata.name());
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus, boolean writeShardGens,
                              ActionListener<String> listener) {
        if (EncryptedRepositoryPlugin.getLicenseState().isEncryptedSnapshotAllowed()) {
            if (consistentSettingsService.isConsistent(passwordSettingForThisRepo)) {
                super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, writeShardGens, listener);
            } else {
                listener.onFailure(new RepositoryException(metadata.name(),
                        "Password mismatch for repository. The local node's value of the " +
                                "keystore secure setting [" + passwordSettingForThisRepo.getKey() + "] is different from the master's"));
            }
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(
                    EncryptedRepositoryPlugin.REPOSITORY_TYPE_NAME + " snapshot repository"));
        }
    }

    @Override
    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId,
                             RecoveryState recoveryState, ActionListener<Void> listener) {
        if (false == consistentSettingsService.isConsistent(passwordSettingForThisRepo)) {
            // the repository has a different password on the local node compared to the master node
            // even though restoring the shard will surely fail (because we know that, by now, the master's password
            // must be correct, otherwise this method will not get called) we let it pass-through in order to avoid
            // having to manipulate the {@code recoveryState} argument
            logger.error("Password mismatch for repository [" + metadata.name() + "]. The local node's value of the " +
                    "keystore secure setting [" + passwordSettingForThisRepo.getKey() + "] is different from the master's");
        }
        super.restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, ActionListener.delegateResponse(listener,
                (l, e) -> l.onFailure(new RepositoryException(metadata.name(), "Password mismatch for repository. " +
                        "The local node's value of the keystore secure setting [" +
                        passwordSettingForThisRepo.getKey() + "] is different from the master's"))));
    }

    @Override
    public void verify(String seed, DiscoveryNode localNode) {
        if (consistentSettingsService.isConsistent(passwordSettingForThisRepo)) {
            super.verify(seed, localNode);
        } else {
            // the repository has a different password on the local node compared to the master node
            throw new RepositoryVerificationException(metadata.name(), "Repository password mismatch. The local node's [" + localNode +
                    "] value of the keystore secure setting [" + passwordSettingForThisRepo.getKey() + "] is different from the master's");
        }
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
        return new EncryptedBlobStore(this.delegatedRepository.blobStore(), dataEncryptionKeyGenerator, metadataEncryptor,
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

    private static class EncryptedBlobStore implements BlobStore {

        private final BlobStore delegatedBlobStore;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final PasswordBasedEncryption metadataEncryptor;
        private final SecureRandom secureRandom;

        EncryptedBlobStore(BlobStore delegatedBlobStore, KeyGenerator dataEncryptionKeyGenerator,
                           PasswordBasedEncryption metadataEncryptor, SecureRandom secureRandom) {
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
            return new EncryptedBlobContainer(delegatedBlobStore, path, dataEncryptionKeyGenerator, metadataEncryptor, secureRandom);
        }
    }

    private static class EncryptedBlobContainer implements BlobContainer {

        private final BlobStore delegatedBlobStore;
        private final KeyGenerator dataEncryptionKeyGenerator;
        private final PasswordBasedEncryption metadataEncryption;
        private final SecureRandom nonceGenerator;
        private final BlobContainer delegatedBlobContainer;
        private final BlobContainer encryptionMetadataBlobContainer;

        EncryptedBlobContainer(BlobStore delegatedBlobStore, BlobPath path, KeyGenerator dataEncryptionKeyGenerator,
                               PasswordBasedEncryption metadataEncryption, SecureRandom nonceGenerator) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.dataEncryptionKeyGenerator = dataEncryptionKeyGenerator;
            this.metadataEncryption = metadataEncryption;
            this.nonceGenerator = nonceGenerator;
            this.delegatedBlobContainer = delegatedBlobStore.blobContainer(path);
            this.encryptionMetadataBlobContainer = delegatedBlobStore.blobContainer(path.prepend(ENCRYPTION_METADATA_ROOT));
        }

        @Override
        public BlobPath path() {
            return delegatedBlobContainer.path();
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            // read metadata
            BytesReference encryptedMetadataBytes = Streams.readFully(encryptionMetadataBlobContainer.readBlob(blobName));
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
            return new DecryptionPacketsInputStream(delegatedBlobContainer.readBlob(blobName), metadata.getDataEncryptionKey(),
                    metadata.getNonce(), metadata.getPacketLengthInBytes());
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            SecretKey dataEncryptionKey = dataEncryptionKeyGenerator.generateKey();
            int nonce = nonceGenerator.nextInt();
            // this is the metadata required to decrypt back the encrypted blob
            BlobEncryptionMetadata metadata = new BlobEncryptionMetadata(nonce, PACKET_LENGTH_IN_BYTES, dataEncryptionKey);
            // encrypt metadata
            final byte[] encryptedMetadata;
            try {
                encryptedMetadata = BlobEncryptionMetadata.serializeMetadata(metadata, metadataEncryption::encrypt);
            } catch (IOException e) {
                throw new IOException("Failure to encrypt metadata for blob [" + blobName + "]", e);
            }
            // first write the encrypted metadata
            try (ByteArrayInputStream encryptedMetadataInputStream = new ByteArrayInputStream(encryptedMetadata)) {
                encryptionMetadataBlobContainer.writeBlob(blobName, encryptedMetadataInputStream, encryptedMetadata.length,
                        failIfAlreadyExists);
            }
            // afterwards write the encrypted data blob
            long encryptedBlobSize = EncryptionPacketsInputStream.getEncryptionLength(blobSize, PACKET_LENGTH_IN_BYTES);
            try (EncryptionPacketsInputStream encryptedInputStream = new EncryptionPacketsInputStream(inputStream,
                    dataEncryptionKey, nonce, PACKET_LENGTH_IN_BYTES)) {
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
                        encryptedBlobContainer.getValue().path(), dataEncryptionKeyGenerator, metadataEncryption, nonceGenerator));
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

}
