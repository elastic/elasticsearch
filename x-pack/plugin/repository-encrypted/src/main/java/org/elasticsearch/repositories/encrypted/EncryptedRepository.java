/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.support.AESKeyUtils;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
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
    static final int DEK_ID_LENGTH = 22; // {@code org.elasticsearch.common.UUIDS} length
    private static final int DEK_CACHE_WEIGHT = 2048;

    // this is the repository instance to which all blob reads and writes are forwarded to (it stores both the encrypted blobs, as well
    // as the associated encrypted DEKs)
    private final BlobStoreRepository delegatedRepository;
    // every data blob is encrypted with its randomly generated AES key (DEK)
    private final Supplier<Tuple<BytesReference, SecretKey>> dekGenerator;
    // license is checked before every snapshot operations; protected non-final for tests
    protected Supplier<XPackLicenseState> licenseStateSupplier;
    private final RepositoryPasswords repositoryPasswords;
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
        Map<String, SecureString> repositoryPasswordsMap
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
        this.repositoryPasswords = new RepositoryPasswords(repositoryPasswordsMap, threadPool());
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
    public void permitSnapshot() {
        super.permitSnapshot();
        if (false == licenseStateSupplier.get().isAllowed(XPackLicenseState.Feature.ENCRYPTED_SNAPSHOT)) {
            throw LicenseUtils.newComplianceException("encrypted snapshots");
        }
        // TODO prevent starting snapshot that can't finish
        // if the master can't verify hashes it will fail to finalize the snapshots, even if the data nodes
        // verify the hashes
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        super.getRepositoryData(ActionListener.wrap(repositoryData -> {
            final StepListener<Void> putPasswordHashInRepositoryMetadataStep = new StepListener<>();
            // this repository has just been created and no operations have yet triggered a hash publication
            if (false == repositoryPasswords.containsPasswordsHash(metadata)) {
                // trigger hash publication
                maybePublishCurrentPasswordHash(putPasswordHashInRepositoryMetadataStep);
            } else {
                putPasswordHashInRepositoryMetadataStep.onResponse(null);
            }
            putPasswordHashInRepositoryMetadataStep.whenComplete(aVoid -> listener.onResponse(repositoryData), listener::onFailure);
        }, listener::onFailure));
    }

    public void startOrResumePasswordChange(
        @Nullable String fromPasswordName,
        @Nullable String toPasswordName,
        ActionListener<Void> listener
    ) {
        RepositoryMetadata currentMetadata = metadata;
        String changeFromPasswordName = fromPasswordName != null
            ? fromPasswordName
            : repositoryPasswords.currentLocalPassword(currentMetadata).v1();
        String changeToPasswordName = toPasswordName != null
            ? toPasswordName
            : repositoryPasswords.currentLocalPassword(currentMetadata).v1();
        if (repositoryPasswords.isPasswordChangeInProgress(currentMetadata)) {
            repositoryPasswords.verifyResumePasswordChange(currentMetadata, changeFromPasswordName, changeToPasswordName, listener);
        } else {
            updateMetadata(
                (latestRepositoryMetadata, newRepositoryMetadataListener) -> {
                    // unlikely concurrent password change
                    if (repositoryPasswords.isPasswordChangeInProgress(latestRepositoryMetadata)) {
                        // resume or fail, probably not important
                        repositoryPasswords.verifyResumePasswordChange(
                            latestRepositoryMetadata,
                            fromPasswordName,
                            toPasswordName,
                            listener
                        );
                    } else {
                        repositoryPasswords.updateMetadataForPasswordChange(
                            latestRepositoryMetadata,
                            fromPasswordName,
                            toPasswordName,
                            newRepositoryMetadataListener
                        );
                    }
                },
                "start encrypted repository ["
                    + metadata.name()
                    + "] password change from ["
                    + fromPasswordName
                    + "] to ["
                    + toPasswordName
                    + "]",
                listener
            );
        }
    }

    // protected for tests
    protected void maybePublishCurrentPasswordHash(ActionListener<Void> listener) {
        updateMetadata((latestRepositoryMetadata, newRepositoryMetadataListener) -> {
            // something else published the hashes in the meantime
            if (repositoryPasswords.containsPasswordsHash(latestRepositoryMetadata)) {
                logger.debug("Repository [" + latestRepositoryMetadata.name() + "] already contains some passwords hash");
                listener.onResponse(null);
                return;
            }
            repositoryPasswords.updateMetadataWithHashForCurrentPassword(latestRepositoryMetadata, newRepositoryMetadataListener);
        }, "update encrypted repository [" + metadata.name() + "] current password hash", listener);
    }

    @Override
    public boolean isCompatible(RepositoryMetadata repositoryMetadata) {
        // this type of repository is designed to work with SOME password settings changing on the fly, without re-constructing the repo
        return repositoryPasswords.equalsIgnorePasswordSettings(getMetadata(), repositoryMetadata);
    }

    @Override
    public String startVerification() {
        // verification bypasses the encrypted blobstore because it must work without published hashes
        final String seed = this.delegatedRepository.startVerification();
        final RepositoryMetadata repositoryMetadata = metadata;
        try {
            final Map<String, String> passwordsHashes = repositoryPasswords.computePasswordsHashForBlobWrite(repositoryMetadata);
            return packSeed(seed, passwordsHashes);
        } catch (Exception e) {
            throw new RepositoryException(repositoryMetadata.name(), "Exception computing passwords hash for repository verification", e);
        }
    }

    @Override
    public void endVerification(String packedSeed) {
        // verification bypasses the encrypted blobstore because it must work without published hashes
        final Tuple<String, Map<String, String>> seedAndHashes;
        try {
            seedAndHashes = unpackSeed(packedSeed);
        } catch (Exception e) {
            throw new RepositoryVerificationException(metadata.name(), "Error verifying passwords hash", e);
        }
        this.delegatedRepository.endVerification(seedAndHashes.v1());
    }

    @Override
    public void verify(String packedSeed, DiscoveryNode localNode) {
        // verification bypasses the encrypted blobstore because it must work without published hashes
        final RepositoryMetadata repositoryMetadata = metadata;
        final Tuple<String, Map<String, String>> seedAndHashes;
        try {
            seedAndHashes = unpackSeed(packedSeed);
            if (false == repositoryPasswords.verifyPasswordsHash(seedAndHashes.v2())) {
                throw new IllegalArgumentException("Repository password(s) mismatch");
            }
        } catch (Exception e) {
            throw new RepositoryException(repositoryMetadata.name(), "Error verifying passwords hash", e);
        }
        this.delegatedRepository.verify(seedAndHashes.v1(), localNode);
    }

    @Override
    public void start() {
        // TODO move this to plugin repository factory
        super.start();
        try {
            repositoryPasswords.currentLocalPassword(metadata);
        } catch (Exception e) {
            throw new RepositoryVerificationException(metadata.name(), "Error starting repository", e);
        }
    }

    @Override
    public RepositoryStats stats() {
        return this.delegatedRepository.stats();
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
            this::getDekWrapperForBlobStore,
            this::getDekUnwrapperForBlobStore,
            blobStoreDEKGenerator,
            dekCache,
            threadPool
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

    // protected for tests
    protected void updateMetadata(
        BiConsumer<RepositoryMetadata, ActionListener<RepositoryMetadata>> updateAction,
        String source,
        ActionListener<Void> listener
    ) {
        super.executeConsistentStateUpdate((latestRepositoryMetadata, updateTaskListener) -> {
            updateAction.accept(latestRepositoryMetadata, ActionListener.wrap(newRepositoryMetadata -> {
                if (false == newRepositoryMetadata.name().equals(latestRepositoryMetadata.name())) {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "Repository name cannot be changed ["
                                + latestRepositoryMetadata.name()
                                + "] ["
                                + newRepositoryMetadata.name()
                                + "]"
                        )
                    );
                    return;
                }
                updateTaskListener.onResponse(new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoriesMetadata repositories = currentState.metadata()
                            .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
                        final List<RepositoryMetadata> newRepositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                        boolean found = false;
                        for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                            if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                                if (found) {
                                    throw new IllegalStateException(
                                        "Found multiple repositories with the same name ["
                                            + newRepositoryMetadata.name()
                                            + "] when updating repository metadata"
                                    );
                                }
                                found = true;
                                newRepositoriesMetadata.add(newRepositoryMetadata);
                            } else {
                                newRepositoriesMetadata.add(repositoryMetadata);
                            }
                        }
                        if (found == false) {
                            throw new IllegalStateException(
                                "Missing repository with name [" + latestRepositoryMetadata.name() + "] when updating repository metadata"
                            );
                        }
                        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                        mdBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(newRepositoriesMetadata));
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn("failed to update metadata from source: " + source, e);
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                        logger.info("Repository [" + newRepositoryMetadata.name() + "] metadata updated from source: " + source);
                        listener.onResponse(null);
                    }
                });
            }, listener::onFailure));
        }, source, listener::onFailure);
    }

    private Supplier<Tuple<BytesReference, SecretKey>> createDEKGenerator() throws GeneralSecurityException {
        // DEK and DEK Ids MUST be generated randomly (with independent random instances)
        // the rand algo is not pinned so that it goes well with various providers (eg FIPS)
        // TODO maybe we can make this a setting for rigurous users
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

    // package-private for tests
    Function<SecretKey, Map<String, byte[]>> getDekWrapperForBlobStore(String dekId) {
        return getDekWrapperUsingPasswords(dekId, () -> {
            RepositoryMetadata repositoryMetadata = metadata;
            Map<String, SecureString> passwords = repositoryPasswords.passwordsForBlobStoreDekWrapping(repositoryMetadata);
            // check that the local passwords that are used to wrap the DEKs match the repository's
            boolean verifyResult = repositoryPasswords.verifyPasswordsHash(repositoryMetadata, passwords.keySet());
            if (false == verifyResult) {
                throw new IllegalArgumentException("Local repository password is incorrect");
            }
            return passwords.values();
        });
    }

    private Function<SecretKey, Map<String, byte[]>> getDekWrapperUsingPasswords(
        String dekId,
        Supplier<Collection<SecureString>> passwordsSupplier
    ) {
        return (dek) -> {
            Map<String, byte[]> wrappedDeks = new HashMap<>(2);
            for (SecureString password : passwordsSupplier.get()) {
                // we rely on the DEK Id being generated randomly so it can be used as a salt
                SecretKey kek = AESKeyUtils.generatePasswordBasedKey(password, dekId);
                final String kekId;
                try {
                    kekId = AESKeyUtils.computeId(kek);
                } catch (GeneralSecurityException e) {
                    throw new IllegalStateException("Unsupported cryptographic operation", e);
                }
                logger.debug(
                    () -> new ParameterizedMessage(
                        "Repository [{}] computed KEK [{}] for DEK [{}] for wrapping",
                        metadata.name(),
                        kekId,
                        dekId
                    )
                );
                final byte[] encryptedDEKBytes;
                try {
                    encryptedDEKBytes = AESKeyUtils.wrap(kek, dek);
                } catch (GeneralSecurityException e) {
                    throw new IllegalStateException("Unsupported cryptographic operation", e);
                }
                logger.trace(
                    () -> new ParameterizedMessage(
                        "Repository [{}] successfully wrapped DEK [{}] using KEK [{}]",
                        metadata.name(),
                        dekId,
                        kekId
                    )
                );
                if (encryptedDEKBytes.length != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                    throw new IllegalStateException(
                        "Wrapped DEK [" + dekId + "] has unexpected blob size [" + encryptedDEKBytes.length + "]"
                    );
                }
                wrappedDeks.put(kekId, encryptedDEKBytes);
            }
            return wrappedDeks;
        };
    }

    private Tuple<String, Function<byte[], SecretKey>> getDekUnwrapperForBlobStore(String dekId) {
        return getDekUnwrapperUsingPassword(dekId, () -> repositoryPasswords.currentLocalPassword(metadata).v2());
    }

    private Tuple<String, Function<byte[], SecretKey>> getDekUnwrapperUsingPassword(String dekId, Supplier<SecureString> passwordSupplier) {
        final SecureString currentPassword = passwordSupplier.get();
        // this step takes a relatively long time
        final SecretKey kek = AESKeyUtils.generatePasswordBasedKey(currentPassword, dekId);
        final String kekId;
        try {
            kekId = AESKeyUtils.computeId(kek);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unsupported cryptographic operation", e);
        }
        logger.debug(
            () -> new ParameterizedMessage("Repository [{}] computed KEK [{}] for DEK [{}] for unwrapping", metadata.name(), kekId, dekId)
        );
        return new Tuple<>(kekId, (encryptedDEKBytes) -> {
            if (encryptedDEKBytes.length != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                throw new IllegalStateException(
                    "Wrapped DEK ["
                        + dekId
                        + "] has unexpected blob size ["
                        + encryptedDEKBytes.length
                        + "]. "
                        + "Most likely the encryption metadata in the repository has been corrupted."
                );
            }
            final SecretKey dek;
            try {
                dek = AESKeyUtils.unwrap(kek, encryptedDEKBytes);
            } catch (InvalidKeyException e) {
                throw new IllegalStateException(
                    "Cannot unwrap DEK ["
                        + dekId
                        + "] using KEK ["
                        + kekId
                        + "]. "
                        + "Most likely the encryption metadata in the repository has been corrupted."
                );
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("Unsupported cryptographic operation", e);
            }
            logger.trace(
                () -> new ParameterizedMessage(
                    "Repository [{}] successfully unwrapped DEK [{}] using KEK [{}]",
                    metadata.name(),
                    dekId,
                    kekId
                )
            );
            return dek;
        });
    }

    void copyDek(String dekId, SecureString fromPassword, SecureString toPassword, ActionListener<Void> listener) {
        // TODO list all DEKs and use the grouped listener
        final EncryptedBlobStore encryptedBlobStore = ((EncryptedBlobStore) blobStore());
        encryptedBlobStore.copyDek(
            dekId,
            (dekIdArg) -> getDekUnwrapperUsingPassword(dekIdArg, () -> fromPassword),
            (dekIdArg) -> getDekWrapperUsingPasswords(dekIdArg, () -> List.of(toPassword)),
            true,
            listener
        );
    }

    @Override
    public boolean hasAtomicOverwrites() {
        // TODO look into this
        return delegatedRepository.hasAtomicOverwrites();
    }

    // pkg-private for tests
    static final class EncryptedBlobStore implements BlobStore {
        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final String repositoryName;
        // pkg-private for tests
        final CheckedSupplier<SingleUseKey, IOException> singleUseDekSupplier;
        // pkg-private for tests
        // blocks until DEK is read and decrypted and then it's cached
        final CheckedFunction<String, SecretKey, IOException> getDekById;
        private final ThreadPool threadPool;

        EncryptedBlobStore(
            BlobStore delegatedBlobStore,
            BlobPath delegatedBasePath,
            String repositoryName,
            Function<String, Function<SecretKey, Map<String, byte[]>>> dekWrapper,
            Function<String, Tuple<String, Function<byte[], SecretKey>>> dekUnwrapper,
            Supplier<Tuple<BytesReference, SecretKey>> dekGenerator,
            Cache<String, SecretKey> dekCache,
            ThreadPool threadPool
        ) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.repositoryName = repositoryName;
            // blocks until DEK is encrypted and stored
            this.singleUseDekSupplier = SingleUseKey.createSingleUseKeySupplier(() -> {
                try {
                    Tuple<BytesReference, SecretKey> newDek = dekGenerator.get();
                    PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                    // store and encrypt the newly generated DEK before making it available
                    storeDek(newDek.v1().utf8ToString(), newDek.v2(), dekWrapper, future);
                    FutureUtils.get(future);
                    return newDek;
                } catch (Exception e) {
                    IOException ioe = (IOException) ExceptionsHelper.unwrap(e, IOException.class);
                    ElasticsearchException ee = (ElasticsearchException) ExceptionsHelper.unwrap(e, ElasticsearchException.class);
                    if (ioe != null) {
                        throw ioe;
                    } else if (ee != null) {
                        throw ee;
                    } else {
                        throw new RepositoryException(repositoryName, "Exception storing new DEK", e);
                    }
                }
            });
            // blocks until DEK is read and decrypted, and then caches the result
            this.getDekById = (dekId) -> {
                try {
                    return dekCache.computeIfAbsent(dekId, ignored -> {
                        PlainActionFuture<SecretKey> future = PlainActionFuture.newFuture();
                        loadDek(dekId, dekUnwrapper, future);
                        return FutureUtils.get(future);
                    });
                } catch (ExecutionException e) {
                    // some exception types are to be expected
                    NoSuchFileException nsfe = (NoSuchFileException) ExceptionsHelper.unwrap(e, NoSuchFileException.class);
                    IOException ioe = (IOException) ExceptionsHelper.unwrap(e, IOException.class);
                    ElasticsearchException ee = (ElasticsearchException) ExceptionsHelper.unwrap(e, ElasticsearchException.class);
                    if (nsfe != null) {
                        // do NOT throw or wrap NoSuchFileException when the DEK does not exist, as this is a decryption problem
                        // and IOExceptions can move the repository in the corrupted state, but wrong password doesn't
                        // necessarily mean the repository is corrupted (maybe it's using the wrong name or value for the password)
                        // of course it can also mean that the repository is indeed corrupted because the wrapped DEKs have been removed
                        throw new RepositoryException(
                            repositoryName,
                            "Failure to read and decrypt DEK ["
                                + dekId
                                + "]. Most likely the repository password is incorrect, where previous "
                                + "snapshots have used a different password. Reason: "
                                + nsfe.toString()
                        );
                    } else if (ioe != null) {
                        throw ioe;
                    } else if (ee != null) {
                        throw ee;
                    } else {
                        throw new RepositoryException(repositoryName, "Exception retrieving DEK [" + dekId + "]", e.getCause());
                    }
                }
            };
            this.threadPool = threadPool;
        }

        private void loadDek(
            String dekId,
            Function<String, Tuple<String, Function<byte[], SecretKey>>> getDekUnwrapper,
            ActionListener<SecretKey> listener
        ) {
            final BlobPath dekBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(dekId);
            final BlobContainer dekBlobContainer = delegatedBlobStore.blobContainer(dekBlobPath);
            logger.debug(
                () -> new ParameterizedMessage(
                    "Repository [{}] loading wrapped DEK [{}] from blob path [{}]",
                    repositoryName,
                    dekId,
                    dekBlobPath
                )
            );
            final StepListener<Tuple<String, Function<byte[], SecretKey>>> unwrapperStepListener = new StepListener<>();
            threadPool.executor(SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME)
                .execute(ActionRunnable.supply(unwrapperStepListener, () -> getDekUnwrapper.apply(dekId)));
            final StepListener<byte[]> wrappedDekBytesStepListener = new StepListener<>();
            unwrapperStepListener.whenComplete(unwrapper -> {
                threadPool.generic().execute(ActionRunnable.supply(wrappedDekBytesStepListener, () -> {
                    final byte[] encryptedDEKBytes = new byte[AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES];
                    try (InputStream encryptedDEKInputStream = dekBlobContainer.readBlob(unwrapper.v1())) {
                        final int bytesRead = Streams.readFully(encryptedDEKInputStream, encryptedDEKBytes);
                        if (bytesRead != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                            throw new IllegalStateException(
                                "Wrapped DEK ["
                                    + dekId
                                    + "] has smaller blob size ["
                                    + bytesRead
                                    + "] than expected. "
                                    + "Most likely the encryption metadata in the repository has been corrupted."
                            );
                        }
                        if (encryptedDEKInputStream.read() != -1) {
                            throw new IllegalStateException(
                                "Wrapped DEK ["
                                    + dekId
                                    + "] has larger blob size than expected. "
                                    + "Most likely the encryption metadata in the repository has been corrupted."
                            );
                        }
                    }
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "Repository [{}] successfully read DEK [{}] from path [{}]",
                            repositoryName,
                            dekId,
                            dekBlobPath.add(unwrapper.v1())
                        )
                    );
                    return encryptedDEKBytes;
                }));
            }, listener::onFailure);
            wrappedDekBytesStepListener.whenComplete(encryptedDekBytes -> {
                threadPool.executor(SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME)
                    .execute(ActionRunnable.supply(listener, () -> unwrapperStepListener.asFuture().get().v2().apply(encryptedDekBytes)));
            }, listener::onFailure);
        }

        private void storeDek(
            String dekId,
            SecretKey dek,
            Function<String, Function<SecretKey, Map<String, byte[]>>> getDekWrapper,
            ActionListener<Void> listener
        ) {
            final BlobPath dekBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(dekId);
            final BlobContainer dekBlobContainer = delegatedBlobStore.blobContainer(dekBlobPath);
            logger.debug(
                () -> new ParameterizedMessage(
                    "Repository [{}] storing wrapped DEK [{}] under blob path [{}]",
                    repositoryName,
                    dekId,
                    dekBlobPath
                )
            );
            final StepListener<Map<String, byte[]>> wrappedDekBytesStepListener = new StepListener<>();
            threadPool.executor(SecurityField.SECURITY_CRYPTO_THREAD_POOL_NAME)
                .execute(ActionRunnable.supply(wrappedDekBytesStepListener, () -> getDekWrapper.apply(dekId).apply(dek)));
            wrappedDekBytesStepListener.whenComplete(
                wrappedDekBytesMap -> threadPool.generic().execute(ActionRunnable.supply(listener, () -> {
                    for (Map.Entry<String, byte[]> wrappedDek : getDekWrapper.apply(dekId).apply(dek).entrySet()) {
                        dekBlobContainer.writeBlobAtomic(wrappedDek.getKey(), new BytesArray(wrappedDek.getValue()), true);
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "Repository [{}] successfully stored wrapped DEK [{}] under path [{}]",
                                repositoryName,
                                dekId,
                                dekBlobPath.add(wrappedDek.getKey())
                            )
                        );
                    }
                    return null;
                })),
                listener::onFailure
            );
        }

        void copyDek(
            String dekId,
            Function<String, Tuple<String, Function<byte[], SecretKey>>> getDekUnwrapper,
            Function<String, Function<SecretKey, Map<String, byte[]>>> getDekWrapper,
            boolean ignoreMissing,
            ActionListener<Void> listener
        ) {
            StepListener<SecretKey> dekStep = new StepListener<>();
            loadDek(dekId, getDekUnwrapper, dekStep);
            dekStep.whenComplete((dek) -> storeDek(dekId, dek, getDekWrapper, listener), e -> {
                NoSuchFileException nsfe = (NoSuchFileException) ExceptionsHelper.unwrap(e, NoSuchFileException.class);
                if (ignoreMissing && nsfe != null) {
                    logger.debug(() -> new ParameterizedMessage("Missing DEK [{}] to copy", new Object[] { dekId }, e));
                    listener.onResponse(null);
                } else {
                    listener.onFailure(e);
                }
            });
        }

        Set<String> listAllDekIds() throws IOException {
            BlobPath deksBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER);
            return delegatedBlobStore.blobContainer(deksBlobPath).children().keySet();
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final Iterator<String> pathIterator = path.iterator();
            BlobPath delegatedBlobContainerPath = delegatedBasePath;
            while (pathIterator.hasNext()) {
                delegatedBlobContainerPath = delegatedBlobContainerPath.add(pathIterator.next());
            }
            final BlobContainer delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBlobContainerPath);
            return new EncryptedBlobContainer(path, repositoryName, delegatedBlobContainer, singleUseDekSupplier, getDekById);
        }

        @Override
        public void close() {
            // do NOT close delegatedBlobStore; it will be closed when the inner delegatedRepository is closed
        }
    }

    private static final class EncryptedBlobContainer extends AbstractBlobContainer {
        private final String repositoryName;
        private final BlobContainer delegatedBlobContainer;
        // supplier for the DEK used for encryption (snapshot)
        private final CheckedSupplier<SingleUseKey, IOException> singleUseDEKSupplier;
        // retrieves the DEK required for decryption (restore)
        private final CheckedFunction<String, SecretKey, IOException> getDEKById;

        EncryptedBlobContainer(
            BlobPath path, // this path contains the {@code EncryptedRepository#basePath} which, importantly, is empty
            String repositoryName,
            BlobContainer delegatedBlobContainer,
            CheckedSupplier<SingleUseKey, IOException> singleUseDEKSupplier,
            CheckedFunction<String, SecretKey, IOException> getDEKById
        ) {
            super(path);
            this.repositoryName = repositoryName;
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
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
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
            final BytesReference dekIdBytes = singleUseNonceAndDEK.getKeyId();
            if (dekIdBytes.length() != DEK_ID_LENGTH) {
                throw new RepositoryException(
                    repositoryName,
                    "Unexpected fatal internal error",
                    new IllegalStateException("Unexpected DEK Id length [" + dekIdBytes.length() + "]")
                );
            }
            final long encryptedBlobSize = getEncryptedBlobByteLength(blobSize);
            try (
                InputStream encryptedInputStream = ChainingInputStream.chain(
                    dekIdBytes.streamInput(),
                    new EncryptionPacketsInputStream(
                        inputStream,
                        singleUseNonceAndDEK.getKey(),
                        singleUseNonceAndDEK.getNonce(),
                        PACKET_LENGTH_IN_BYTES
                    )
                )
            ) {
                delegatedBlobContainer.writeBlob(blobName, encryptedInputStream, encryptedBlobSize, failIfAlreadyExists);
            }
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
                        childBlobContainer.getValue(),
                        singleUseDEKSupplier,
                        getDEKById
                    )
                );
            }
            return Map.copyOf(resultBuilder);
        }
    }

    private static String packSeed(String seed, Map<String, String> extra) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput(1024)) {
            out.writeString(seed);
            out.writeMap(extra, StreamOutput::writeString, StreamOutput::writeString);
            byte[] outBytes = BytesReference.toBytes(out.bytes());
            return new String(Base64.getUrlEncoder().withoutPadding().encode(outBytes), StandardCharsets.UTF_8);
        }
    }

    private static Tuple<String, Map<String, String>> unpackSeed(String packedSeed) throws IOException {
        try (
            ByteBufferStreamInput buf = new ByteBufferStreamInput(
                ByteBuffer.wrap(Base64.getUrlDecoder().decode(packedSeed.getBytes(StandardCharsets.UTF_8)))
            )
        ) {
            String seed = buf.readString();
            Map<String, String> meta = buf.readMap(StreamInput::readString, StreamInput::readString);
            return new Tuple<>(seed, meta);
        }
    }
}
