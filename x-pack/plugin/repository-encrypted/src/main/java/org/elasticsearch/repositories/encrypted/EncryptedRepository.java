/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotInfo;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class EncryptedRepository extends BlobStoreRepository {
    static final Logger logger = LogManager.getLogger(EncryptedRepository.class);
    // the following constants are fixed by definition
    static final int GCM_TAG_LENGTH_IN_BYTES = 16;
    static final int GCM_IV_LENGTH_IN_BYTES = 12;
    static final int AES_BLOCK_LENGTH_IN_BYTES = 128;
    // the following constants require careful thought before changing because they will break backwards compatibility
    static final String DATA_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
    static final int MAX_PACKET_LENGTH_IN_BYTES = 8 << 20; // 8MB
    // this should be smaller than {@code #MAX_PACKET_LENGTH_IN_BYTES} and it's what {@code EncryptionPacketsInputStream} uses
    // during encryption and what {@code DecryptionPacketsInputStream} expects during decryption (it is not configurable)
    static final int PACKET_LENGTH_IN_BYTES = 64 * (1 << 10); // 64KB
    // the path of the blob container holding all the DEKs
    // this is relative to the root base path holding the encrypted blobs (i.e. the repository root base path)
    static final String DEK_ROOT_CONTAINER = ".encryption-metadata"; // package private for tests
    static final String DEKS_GEN_CONTAINER = "deks"; // package private for tests
    // these blobs indicate the generational container where the encrypted DEKs are stored
    // the blob names are formatted as "done.{password_generation}.{uuid}"
    static final String DEKS_GEN_MARKER_BLOB = "done."; // package private for tests
    static final int DEK_ID_LENGTH = 22; // {@code org.elasticsearch.common.UUIDS} length

    // this is a very very generous cache size, so that in practice the nodes read and decrypt the same DEK only once
    private static final int DEK_CACHE_WEIGHT = 2048;

    // this is the repository instance to which all blob reads and writes are forwarded to (it stores both the encrypted blobs, as well
    // as the associated encrypted DEKs)
    private final BlobStoreRepository delegatedRepository;
    // every data blob is encrypted with its randomly generated AES key (DEK)
    private final Supplier<Tuple<BytesReference, SecretKey>> dekGenerator;
    // license is checked before every snapshot operations; protected non-final for tests
    protected Supplier<XPackLicenseState> licenseStateSupplier;
    private final SecureString repositoryPassword;
    private final Cache<String, SecretKey> dekCache;

    /**
     * Returns the byte length (i.e. the storage size) of an encrypted blob, given the length of the blob's plaintext contents.
     *
     * @see EncryptionPacketsInputStream#getEncryptionLength(long, int)
     */
    public static long getEncryptedBlobByteLength(long plaintextBlobByteLength) {
        return (long) DEK_ID_LENGTH /* UUID byte length */
            + EncryptionPacketsInputStream.getEncryptionLength(plaintextBlobByteLength, PACKET_LENGTH_IN_BYTES);
    }

    protected EncryptedRepository(
        RepositoryMetadata metadata,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        BlobStoreRepository delegatedRepository,
        Supplier<XPackLicenseState> licenseStateSupplier,
        SecureString repositoryPassword
    ) throws GeneralSecurityException {
        super(
            metadata,
            namedXContentRegistry,
            clusterService,
            bigArrays,
            recoverySettings,
            BlobPath.cleanPath() /* the encrypted repository uses a hardcoded empty
                                 base blob path but the base path setting is honored for the delegated repository */
        );
        this.delegatedRepository = delegatedRepository;
        this.dekGenerator = createDEKGenerator();
        this.licenseStateSupplier = licenseStateSupplier;
        this.repositoryPassword = repositoryPassword;
        // stores decrypted DEKs; DEKs are reused to encrypt/decrypt multiple independent blobs
        this.dekCache = CacheBuilder.<String, SecretKey>builder().setMaximumWeight(DEK_CACHE_WEIGHT).build();
        if (isReadOnly() != delegatedRepository.isReadOnly()) {
            throw new RepositoryException(
                metadata.name(),
                "Unexpected fatal internal error",
                new IllegalStateException("The encrypted repository must be read-only iff the delegate repository is read-only")
            );
        }
    }

    @Override
    public RepositoryStats stats() {
        return this.delegatedRepository.stats();
    }

    @Override
    public void finalizeSnapshot(
        ShardGenerations shardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        ActionListener<RepositoryData> listener
    ) {
        if (false == licenseStateSupplier.get().isAllowed(XPackLicenseState.Feature.ENCRYPTED_SNAPSHOT)) {
            listener.onFailure(LicenseUtils.newComplianceException("encrypted snapshots"));
            return;
        }
        super.finalizeSnapshot(
            shardGenerations,
            repositoryStateId,
            clusterMetadata,
            snapshotInfo,
            repositoryMetaVersion,
            stateTransformer,
            listener
        );
    }

    @Override
    public void snapshotShard(SnapshotShardContext context) {
        if (false == licenseStateSupplier.get().isAllowed(XPackLicenseState.Feature.ENCRYPTED_SNAPSHOT)) {
            context.onFailure(LicenseUtils.newComplianceException("encrypted snapshots"));
            return;
        }
        super.snapshotShard(context);
    }

    @Override
    protected void executeConsistentStateUpdate(
        BiConsumer<RepositoryData, ActionListener<ClusterStateUpdateTask>> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    ) {
        super.executeConsistentStateUpdate((repositoryData, createUpdateTaskListener) -> {
            if (repositoryData.equals(RepositoryData.EMPTY)) {
                threadPool().generic().execute(ActionRunnable.wrap(createUpdateTaskListener, (l) -> {
                    // TODO this is not safe for master fail-over
                    ((EncryptedBlobStore) blobStore()).createPasswordGeneration(repositoryPassword, 0);
                    createUpdateTask.accept(repositoryData, createUpdateTaskListener);
                }));
            } else {
                createUpdateTask.accept(repositoryData, createUpdateTaskListener);
            }
        }, source, onFailure);
    }

    @Override
    protected BlobStore createBlobStore() {
        final Supplier<Tuple<BytesReference, SecretKey>> blobStoreDEKGenerator;
        if (isReadOnly()) {
            // make sure that a read-only repository can't encrypt anything
            blobStoreDEKGenerator = () -> {
                throw new RepositoryException(
                    metadata.name(),
                    "Unexpected fatal internal error",
                    new IllegalStateException("DEKs are required for encryption but this is a read-only repository")
                );
            };
        } else {
            blobStoreDEKGenerator = this.dekGenerator;
        }
        return new EncryptedBlobStore(
            delegatedRepository.blobStore(),
            delegatedRepository.basePath(),
            metadata.name(),
            bigArrays,
            () -> repositoryPassword, // password is constant for now, but the blob store is built to assume that it can change
            blobStoreDEKGenerator,
            dekCache
        );
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

    private Supplier<Tuple<BytesReference, SecretKey>> createDEKGenerator() throws GeneralSecurityException {
        // DEK and DEK Ids MUST be generated randomly (with independent random instances)
        // the rand algo is not pinned so that it goes well with various providers (eg FIPS)
        // TODO maybe we can make this configurable for rigorous users
        final SecureRandom dekSecureRandom = new SecureRandom();
        final SecureRandom dekIdSecureRandom = new SecureRandom();
        final KeyGenerator dekGenerator = KeyGenerator.getInstance(DATA_ENCRYPTION_SCHEME.split("/")[0]);
        dekGenerator.init(AESKeyUtils.KEY_LENGTH_IN_BYTES * Byte.SIZE, dekSecureRandom);
        return () -> {
            final BytesReference dekId = new BytesArray(UUIDs.randomBase64UUID(dekIdSecureRandom));
            final SecretKey dek = dekGenerator.generateKey();
            logger.debug("Repository [{}] generated new DEK [{}]", metadata.name(), dekId);
            return new Tuple<>(dekId, dek);
        };
    }

    @Override
    public boolean hasAtomicOverwrites() {
        // TODO verify this
        return delegatedRepository.hasAtomicOverwrites();
    }

    // pkg-private for tests
    static final class EncryptedBlobStore implements BlobStore {
        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final String repositoryName;
        private final BigArrays bigArrays;
        private final Supplier<SecureString> repositoryPasswordSupplier;
        private final Cache<String, SecretKey> dekCache;
        private final CheckedSupplier<SingleUseKey, IOException> singleUseDEKSupplier;
        private final Cache<Set<String>, String> inferLatestPasswordGenerationCache;
        private final Cache<Tuple<SecureString, String>, Boolean> verifyPasswordForGenerationCache;
        private final Cache<SecureString, String> loadGenerationForPasswordCache;

        EncryptedBlobStore(
            BlobStore delegatedBlobStore,
            BlobPath delegatedBasePath,
            String repositoryName,
            BigArrays bigArrays,
            Supplier<SecureString> repositoryPasswordSupplier,
            Supplier<Tuple<BytesReference, SecretKey>> dekGenerator,
            Cache<String, SecretKey> dekCache
        ) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.repositoryName = repositoryName;
            this.bigArrays = bigArrays;
            this.repositoryPasswordSupplier = repositoryPasswordSupplier;
            this.dekCache = dekCache;
            this.singleUseDEKSupplier = SingleUseKey.createSingleUseKeySupplier(() -> {
                Tuple<BytesReference, SecretKey> newDEK = dekGenerator.get();
                // store the newly generated DEK before making it available
                storeDEK(newDEK.v1().utf8ToString(), newDEK.v2());
                return newDEK;
            });
            this.inferLatestPasswordGenerationCache = CacheBuilder.<Set<String>, String>builder().setMaximumWeight(1).build();
            this.verifyPasswordForGenerationCache = CacheBuilder.<Tuple<SecureString, String>, Boolean>builder()
                .setMaximumWeight(1)
                .build();
            this.loadGenerationForPasswordCache = CacheBuilder.<SecureString, String>builder().setMaximumWeight(1).build();
        }

        // protected for tests
        SecretKey getKEKForDek(SecureString repositoryPassword, String dekId) {
            try {
                // we rely on the DEK Id being generated randomly so it can be used as a salt
                final SecretKey kek = AESKeyUtils.generatePasswordBasedKey(repositoryPassword, dekId);
                logger.debug("Repository [{}] computed KEK for DEK [{}]", repositoryName, dekId);
                return kek;
            } catch (GeneralSecurityException e) {
                throw new RepositoryException(repositoryName, "Failure to generate KEK to wrap the DEK [" + dekId + "]", e);
            }
        }

        private BlobContainer getDeksBlobContainer() {
            final BlobPath deksBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(DEKS_GEN_CONTAINER);
            return delegatedBlobStore.blobContainer(deksBlobPath);
        }

        // protected for tests
        String inferLatestPasswordGeneration() throws IOException {
            final BlobContainer deksBlobContainer = getDeksBlobContainer();
            final Set<String> doneMarkersSet = deksBlobContainer.listBlobsByPrefix(DEKS_GEN_MARKER_BLOB).keySet();
            try {
                return this.inferLatestPasswordGenerationCache.computeIfAbsent(doneMarkersSet, ignored -> {
                    // infer the latest DEKs container by taking the highest generation
                    String latestDoneMarker = null;
                    int latestGeneration = -1; // valid generations start at "0"
                    for (String doneMarker : doneMarkersSet) {
                        int generation = Integer.parseInt(
                            doneMarker.substring(DEKS_GEN_MARKER_BLOB.length(), doneMarker.indexOf('.', DEKS_GEN_MARKER_BLOB.length()))
                        );
                        if (generation > latestGeneration) {
                            latestGeneration = generation;
                            latestDoneMarker = doneMarker;
                        } else if (generation == latestGeneration) {
                            // there should never be two different password generations
                            // if there are, they have been independently produced by different clusters
                            // if we throw early the problem can be remediated with manual intervention
                            throw new IllegalStateException("Concurrent password change detected");
                        }
                    }
                    if (latestDoneMarker == null) {
                        throw new IllegalStateException("Repository is not initialized");
                    }
                    return latestDoneMarker.substring(DEKS_GEN_MARKER_BLOB.length());
                });
            } catch (ExecutionException e) {
                // in order for the encrypted repo to act "transparently" do not wrap IOExceptions, and pass them through
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                } else {
                    throw new RepositoryException(repositoryName, "Cannot determine DEK generation container", e.getCause());
                }
            }
        }

        private void verifyPasswordForGeneration(SecureString repositoryPassword, String passwordGeneration) throws IOException {
            final boolean equalHashes;
            try {
                equalHashes = this.verifyPasswordForGenerationCache.computeIfAbsent(
                    new Tuple<>(repositoryPassword, passwordGeneration),
                    ignored -> {
                        final BlobContainer deksBlobContainer = getDeksBlobContainer();
                        final String passwordHash;
                        try (
                            XContentParser parser = JsonXContent.jsonXContent.createParser(
                                NamedXContentRegistry.EMPTY,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                deksBlobContainer.readBlob(DEKS_GEN_MARKER_BLOB + passwordGeneration)
                            )
                        ) {
                            passwordHash = parser.mapStrings().get("password_hash");
                        }
                        // password verification, which requires pbkdf2 computation, is performed on the calling thread (snapshot)
                        final String uuid = passwordGeneration.substring(passwordGeneration.lastIndexOf('.') + 1);
                        final String computedPasswordHash = AESKeyUtils.computeId(
                            AESKeyUtils.generatePasswordBasedKey(repositoryPassword, uuid)
                        );
                        return passwordHash.equals(computedPasswordHash);
                    }
                );
            } catch (ExecutionException e) {
                // in order for the encrypted repo to act "transparently" do not wrap IOExceptions, and pass them through
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                } else {
                    throw new RepositoryException(
                        repositoryName,
                        "Failed to verify password for generation [" + passwordGeneration + "]",
                        e.getCause()
                    );
                }
            }
            if (false == equalHashes) {
                throw new RepositoryException(
                    repositoryName,
                    "The repository password is incorrect. Previous snapshots have used a different password, or the repository "
                        + "password has been changed."
                );
            }
        }

        // protected for tests
        void createPasswordGeneration(SecureString password, int generation) throws GeneralSecurityException, IOException {
            BlobPath deksBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(DEKS_GEN_CONTAINER);
            BlobContainer deksBlobContainer = delegatedBlobStore.blobContainer(deksBlobPath);
            // the uuid ensures different clusters do not overwrite the "done." marker blobs
            String doneUUID = UUIDs.randomBase64UUID();
            String doneBlobName = String.format(DEKS_GEN_MARKER_BLOB + "%d.%s", generation, doneUUID);
            // do the pbkdf2 on the calling thread
            String passwordHash = AESKeyUtils.computeId(AESKeyUtils.generatePasswordBasedKey(password, doneUUID));
            // TODO add other troubleshooting details, eg: node id, cluster uuid, local timestamp, new password name, previous password gen
            XContentBuilder doneBlobContents = XContentFactory.jsonBuilder().map(Map.of("password_hash", passwordHash));
            deksBlobContainer.writeBlobAtomic(doneBlobName, BytesReference.bytes(doneBlobContents), true);
        }

        // pkg-private for tests
        SecretKey getDEKById(String dekId) throws IOException {
            try {
                return dekCache.computeIfAbsent(dekId, ignored -> loadDEK(dekId));
            } catch (ExecutionException e) {
                // some exception types are to be expected
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                } else if (e.getCause() instanceof ElasticsearchException) {
                    throw (ElasticsearchException) e.getCause();
                } else {
                    throw new RepositoryException(repositoryName, "Unexpected exception retrieving DEK [" + dekId + "]", e);
                }
            }
        }

        private SecretKey loadDEK(String dekId) throws IOException {
            final SecureString repositoryPassword = repositoryPasswordSupplier.get();
            // cache the generation that has been verified for the current password
            // the generation is valid until the password changes and the new generation is not valid for the same password
            final String passwordGeneration;
            try {
                passwordGeneration = this.loadGenerationForPasswordCache.computeIfAbsent(repositoryPassword, ignored -> {
                    final String inferredPasswordGeneration = inferLatestPasswordGeneration();
                    verifyPasswordForGeneration(repositoryPassword, inferredPasswordGeneration);
                    return inferredPasswordGeneration;
                });
            } catch (ExecutionException e) {
                // some exception types are to be expected
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                } else if (e.getCause() instanceof ElasticsearchException) {
                    throw (ElasticsearchException) e.getCause();
                } else {
                    throw new RepositoryException(repositoryName, "Unexpected exception retrieving DEK [" + dekId + "]", e);
                }
            }
            final BlobPath dekBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(DEKS_GEN_CONTAINER).add(passwordGeneration);
            logger.debug("Repository [{}] loading wrapped DEK [{}] from blob path {}", repositoryName, dekId, dekBlobPath);
            final BlobContainer dekBlobContainer = delegatedBlobStore.blobContainer(dekBlobPath);
            final SecretKey kek = getKEKForDek(repositoryPassword, dekId);
            logger.trace("Repository [{}] using KEK to unwrap DEK [{}]", repositoryName, dekId);
            final byte[] encryptedDEKBytes = new byte[AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES];
            try (InputStream encryptedDEKInputStream = dekBlobContainer.readBlob(dekId)) {
                final int bytesRead = Streams.readFully(encryptedDEKInputStream, encryptedDEKBytes);
                if (bytesRead != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                    throw new RepositoryException(
                        repositoryName,
                        "Wrapped DEK [" + dekId + "] has smaller length [" + bytesRead + "] than expected"
                    );
                }
                if (encryptedDEKInputStream.read() != -1) {
                    throw new RepositoryException(repositoryName, "Wrapped DEK [" + dekId + "] is larger than expected");
                }
            } catch (NoSuchFileException e) {
                // do NOT throw IOException when the DEK does not exist, as this is a decryption problem, and IOExceptions
                // can move the repository in the corrupted state, and corrupted state is _probably_ not desirable
                // most likely this node hasn't observed that the repository password changed because it is part of a different
                // cluster than the one that performed the password change
                // the repository should be remounted with the new password
                throw new RepositoryException(
                    repositoryName,
                    "The repository password is probably incorrect. Most likely it has been changed outside of this cluster. "
                        + "Try remounting the repository with the new password.",
                    e
                );
            }
            logger.trace("Repository [{}] successfully read DEK [{}] from path {}", repositoryName, dekId, dekBlobPath);
            try {
                final SecretKey dek = AESKeyUtils.unwrap(kek, encryptedDEKBytes);
                logger.debug("Repository [{}] successfully loaded DEK [{}] from path {}", repositoryName, dekId, dekBlobPath);
                return dek;
            } catch (GeneralSecurityException e) {
                // unwrap should not fail because the password has been verified, and all the DEKs in a generation are
                // encrypted using the same password
                throw new RepositoryException(
                    repositoryName,
                    "Unexpected exception retrieving DEK [" + dekId + "]. " + "Failure to AES unwrap the DEK",
                    e
                );
            }
        }

        // pkg-private for tests
        void storeDEK(String dekId, SecretKey dek) throws IOException {
            final SecureString repositoryPassword = repositoryPasswordSupplier.get();
            // Firstly, do a LIST to determine the latest container that should hold the encrypted DEKs.
            // It is NOT safe to cache the result of the LIST because multiple repositories can be configured with the same container path
            // and be used alternatively (but not concurrently) for snapshots. In this case, if one of the cluster changes the password,
            // the other one has to subsequently detect it, and refuse to use the old password.
            // DEK stores are relatively rare ops, so a LIST on every store is not a big overhead
            final String passwordGeneration = inferLatestPasswordGeneration();
            verifyPasswordForGeneration(repositoryPassword, passwordGeneration);
            final BlobPath dekBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(DEKS_GEN_CONTAINER).add(passwordGeneration);
            logger.debug("Repository [{}] storing wrapped DEK [{}] under blob path {}", repositoryName, dekId, dekBlobPath);
            final BlobContainer dekBlobContainer = delegatedBlobStore.blobContainer(dekBlobPath);
            final SecretKey kek = getKEKForDek(repositoryPassword, dekId);
            logger.trace("Repository [{}] using KEK to wrap DEK [{}]", repositoryName, dekId);
            final byte[] encryptedDEKBytes;
            try {
                encryptedDEKBytes = AESKeyUtils.wrap(kek, dek);
                if (encryptedDEKBytes.length != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                    throw new RepositoryException(
                        repositoryName,
                        "Wrapped DEK [" + dekId + "] has unexpected length [" + encryptedDEKBytes.length + "]"
                    );
                }
            } catch (GeneralSecurityException e) {
                // throw unchecked ElasticsearchException; IOExceptions are interpreted differently and can move the repository in the
                // corrupted state
                throw new RepositoryException(repositoryName, "Failure to AES wrap the DEK [" + dekId + "]", e);
            }
            logger.trace("Repository [{}] successfully wrapped DEK [{}]", repositoryName, dekId);
            dekBlobContainer.writeBlobAtomic(dekId, new BytesArray(encryptedDEKBytes), true);
            logger.debug("Repository [{}] successfully stored DEK [{}] under path {}", repositoryName, dekId, dekBlobPath);
            // TODO (maybe) do a double check that the password generation hasn't changed
            // is it guaranteed that the generation doesn't change while this method is invoked?
            // if the generation changed, there is a password change and a snapshot concurrently in-progress
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final Iterator<String> pathIterator = path.iterator();
            BlobPath delegatedBlobContainerPath = delegatedBasePath;
            while (pathIterator.hasNext()) {
                delegatedBlobContainerPath = delegatedBlobContainerPath.add(pathIterator.next());
            }
            final BlobContainer delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBlobContainerPath);
            return new EncryptedBlobContainer(
                path,
                repositoryName,
                bigArrays,
                delegatedBlobContainer,
                singleUseDEKSupplier,
                this::getDEKById
            );
        }

        @Override
        public void close() {
            // do NOT close delegatedBlobStore; it will be closed when the inner delegatedRepository is closed
        }
    }

    private static final class EncryptedBlobContainer extends AbstractBlobContainer {
        private final String repositoryName;
        private final BigArrays bigArrays;
        private final BlobContainer delegatedBlobContainer;
        // supplier for the DEK used for encryption (snapshot)
        private final CheckedSupplier<SingleUseKey, IOException> singleUseDEKSupplier;
        // retrieves the DEK required for decryption (restore)
        private final CheckedFunction<String, SecretKey, IOException> getDEKById;

        EncryptedBlobContainer(
            BlobPath path, // this path contains the {@code EncryptedRepository#basePath} which, importantly, is empty
            String repositoryName,
            BigArrays bigArrays,
            BlobContainer delegatedBlobContainer,
            CheckedSupplier<SingleUseKey, IOException> singleUseDEKSupplier,
            CheckedFunction<String, SecretKey, IOException> getDEKById
        ) {
            super(path);
            this.repositoryName = repositoryName;
            this.bigArrays = bigArrays;
            final String rootPathElement = path.iterator().hasNext() ? path.iterator().next() : null;
            if (DEK_ROOT_CONTAINER.equals(rootPathElement)) {
                throw new RepositoryException(repositoryName, "Cannot descend into the DEK blob container " + path);
            }
            this.delegatedBlobContainer = delegatedBlobContainer;
            this.singleUseDEKSupplier = singleUseDEKSupplier;
            this.getDEKById = getDEKById;
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return delegatedBlobContainer.blobExists(blobName);
        }

        /**
         * Returns a new {@link InputStream} for the given {@code blobName} that can be used to read the contents of the blob.
         * The returned {@code InputStream} transparently handles the decryption of the blob contents, by first working out
         * the blob name of the associated DEK id, reading and decrypting the DEK (given the repository password, unless the DEK is
         * already cached because it had been used for other blobs before), and lastly reading and decrypting the data blob,
         * in a streaming fashion, by employing the {@link DecryptionPacketsInputStream}.
         * The {@code DecryptionPacketsInputStream} does not return un-authenticated data.
         *
         * @param   blobName The name of the blob to get an {@link InputStream} for.
         */
        @Override
        public InputStream readBlob(String blobName) throws IOException {
            // This MIGHT require two concurrent readBlob connections if the DEK is not already in the cache and if the encrypted blob
            // is large enough so that the underlying network library keeps the connection open after reading the prepended DEK ID.
            // Arguably this is a problem only under lab conditions, when the storage service is saturated only by the first read
            // connection of the pair, so that the second read connection (for the DEK) can not be fulfilled.
            // In this case the second connection will time-out which will trigger the closing of the first one, therefore
            // allowing other pair connections to complete.
            // In this situation the restore process should slowly make headway, albeit under read-timeout exceptions
            final InputStream encryptedDataInputStream = delegatedBlobContainer.readBlob(blobName);
            try {
                // read the DEK Id (fixed length) which is prepended to the encrypted blob
                final byte[] dekIdBytes = new byte[DEK_ID_LENGTH];
                final int bytesRead = Streams.readFully(encryptedDataInputStream, dekIdBytes);
                if (bytesRead != DEK_ID_LENGTH) {
                    throw new RepositoryException(repositoryName, "The encrypted blob [" + blobName + "] is too small [" + bytesRead + "]");
                }
                final String dekId = new String(dekIdBytes, StandardCharsets.UTF_8);
                // might open a connection to read and decrypt the DEK, but most likely it will be served from cache
                final SecretKey dek = getDEKById.apply(dekId);
                // read and decrypt the rest of the blob
                return new DecryptionPacketsInputStream(encryptedDataInputStream, dek, PACKET_LENGTH_IN_BYTES);
            } catch (Exception e) {
                try {
                    encryptedDataInputStream.close();
                } catch (IOException closeEx) {
                    e.addSuppressed(closeEx);
                }
                throw e;
            }
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        /**
         * Reads the blob content from the input stream and writes it to the container in a new blob with the given name.
         * If {@code failIfAlreadyExists} is {@code true} and a blob with the same name already exists, the write operation will fail;
         * otherwise, if {@code failIfAlreadyExists} is {@code false} the blob is overwritten.
         * The contents are encrypted in a streaming fashion. The DEK (encryption key) is randomly generated and reused for encrypting
         * subsequent blobs such that the same IV is not reused together with the same key.
         * The DEK encryption key is separately stored in a different blob, which is encrypted with the repository key.
         *
         * @param   blobName
         *          The name of the blob to write the contents of the input stream to.
         * @param   inputStream
         *          The input stream from which to retrieve the bytes to write to the blob.
         * @param   blobSize
         *          The size of the blob to be written, in bytes. The actual number of bytes written to the storage service is larger
         *          because of encryption and authentication overhead. It is implementation dependent whether this value is used
         *          in writing the blob to the repository.
         * @param   failIfAlreadyExists
         *          whether to throw a FileAlreadyExistsException if the given blob already exists
         */
        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            // reuse, but possibly generate and store a new DEK
            final SingleUseKey singleUseNonceAndDEK = singleUseDEKSupplier.get();
            final BytesReference dekIdBytes = getDEKBytes(singleUseNonceAndDEK);
            final long encryptedBlobSize = getEncryptedBlobByteLength(blobSize);
            try (InputStream encryptedInputStream = encryptedInput(inputStream, singleUseNonceAndDEK, dekIdBytes)) {
                delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedBlobSize, failIfAlreadyExists);
            }
        }

        @Override
        public void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
            // reuse, but possibly generate and store a new DEK
            final SingleUseKey singleUseNonceAndDEK = singleUseDEKSupplier.get();
            final BytesReference dekIdBytes = getDEKBytes(singleUseNonceAndDEK);
            try (
                ReleasableBytesStreamOutput tmp = new ReleasableBytesStreamOutput(
                    Math.toIntExact(getEncryptedBlobByteLength(bytes.length())),
                    bigArrays
                )
            ) {
                try (InputStream encryptedInputStream = encryptedInput(bytes.streamInput(), singleUseNonceAndDEK, dekIdBytes)) {
                    org.elasticsearch.core.internal.io.Streams.copy(encryptedInputStream, tmp, false);
                }
                delegatedBlobContainer.writeBlob(blobName, tmp.bytes(), failIfAlreadyExists);
            }
        }

        private ChainingInputStream encryptedInput(InputStream inputStream, SingleUseKey singleUseNonceAndDEK, BytesReference dekIdBytes)
            throws IOException {
            return ChainingInputStream.chain(
                dekIdBytes.streamInput(),
                new EncryptionPacketsInputStream(
                    inputStream,
                    singleUseNonceAndDEK.getKey(),
                    singleUseNonceAndDEK.getNonce(),
                    PACKET_LENGTH_IN_BYTES
                )
            );
        }

        private BytesReference getDEKBytes(SingleUseKey singleUseNonceAndDEK) {
            final BytesReference dekIdBytes = singleUseNonceAndDEK.getKeyId();
            if (dekIdBytes.length() != DEK_ID_LENGTH) {
                throw new RepositoryException(
                    repositoryName,
                    "Unexpected fatal internal error",
                    new IllegalStateException("Unexpected DEK Id length [" + dekIdBytes.length() + "]")
                );
            }
            return dekIdBytes;
        }

        @Override
        public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
            // the encrypted repository does not offer an alternative implementation for atomic writes
            // fallback to regular write
            writeBlob(blobName, bytes, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            return delegatedBlobContainer.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            delegatedBlobContainer.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return delegatedBlobContainer.listBlobs();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return delegatedBlobContainer.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            final Map<String, BlobContainer> childEncryptedBlobContainers = delegatedBlobContainer.children();
            final Map<String, BlobContainer> resultBuilder = new HashMap<>(childEncryptedBlobContainers.size());
            for (Map.Entry<String, BlobContainer> childBlobContainer : childEncryptedBlobContainers.entrySet()) {
                if (childBlobContainer.getKey().equals(DEK_ROOT_CONTAINER) && false == path().iterator().hasNext()) {
                    // do not descend into the DEK blob container
                    continue;
                }
                // get an encrypted blob container for each child
                // Note that the encryption metadata blob container might be missing
                resultBuilder.put(
                    childBlobContainer.getKey(),
                    new EncryptedBlobContainer(
                        path().add(childBlobContainer.getKey()),
                        repositoryName,
                        bigArrays,
                        childBlobContainer.getValue(),
                        singleUseDEKSupplier,
                        getDEKById
                    )
                );
            }
            return Map.copyOf(resultBuilder);
        }
    }
}
