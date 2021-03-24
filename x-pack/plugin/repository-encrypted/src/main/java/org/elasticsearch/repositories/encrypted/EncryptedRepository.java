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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.TriConsumer;
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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xpack.core.security.support.AESKeyUtils;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
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
    protected void executeConsistentStateUpdate(TriConsumer<RepositoryData, RepositoryMetadata,
            ActionListener<ClusterStateUpdateTask>> createUpdateTaskSupplier, String source, Consumer<Exception> onFailure) {
        if (source.startsWith("create_snapshot") &&
                false == licenseStateSupplier.get().isAllowed(XPackLicenseState.Feature.ENCRYPTED_SNAPSHOT)) {
            onFailure.accept(LicenseUtils.newComplianceException("encrypted snapshots"));
            return;
        }
        if (repositoryPasswords.containsPasswordHashes(metadata)) {
            super.executeConsistentStateUpdate(createUpdateTaskSupplier, source, onFailure);
            return;
        }
        final StepListener<RepositoryData> publishHashesStepListener = new StepListener<>();
        super.executeConsistentStateUpdate((latestRepositoryData, latestRepositoryMetadata, updateTaskListener) -> {
            // async compute password hashes for task, given the repository metadata
            repositoryPasswords.computePasswordHashes(latestRepositoryMetadata,
                    ActionListener.wrap(localPasswordHashes -> updateTaskListener.onResponse(new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            // don't ever overwrite hashes
                            if (repositoryPasswords.containsPasswordHashes(latestRepositoryMetadata)) {
                                logger.warn("Repository [" + latestRepositoryMetadata.name() + "] already contains password " +
                                        "hashes");
                                return currentState;
                            }
                            final RepositoryMetadata newRepositoryMetadata =
                                    repositoryPasswords.withPasswordHashes(latestRepositoryMetadata,
                                            localPasswordHashes);
                            if (false == repositoryPasswords.containsRequiredPasswordHashes(newRepositoryMetadata)) {
                                throw new IllegalStateException("Internal consistency error when updating repository [" +
                                        latestRepositoryMetadata.name() + "] password hashes");
                            }
                            final RepositoriesMetadata repositories = currentState.metadata().custom(RepositoriesMetadata.TYPE,
                                    RepositoriesMetadata.EMPTY);
                            final List<RepositoryMetadata> newRepositoriesMetadata =
                                    new ArrayList<>(repositories.repositories().size());
                            boolean found = false;
                            for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                                if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                                    if (found) {
                                        throw new IllegalStateException("Internal consistency error when updating repository " +
                                                "[" +
                                                latestRepositoryMetadata.name() + "] password hashes");
                                    }
                                    found = true;
                                    newRepositoriesMetadata.add(newRepositoryMetadata);
                                } else {
                                    newRepositoriesMetadata.add(repositoryMetadata);
                                }
                            }
                            if (found == false) {
                                throw new IllegalStateException("Internal consistency error when updating repository [" +
                                        latestRepositoryMetadata.name() + "] password hashes");
                            }
                            Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                            mdBuilder.putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(newRepositoriesMetadata));
                            return ClusterState.builder(currentState).metadata(mdBuilder).build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn("failed to " + source, e);
                            onFailure.accept(e);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                            logger.info("Published password hashes for repository [" + latestRepositoryMetadata.name() + "]");
                            publishHashesStepListener.onResponse(latestRepositoryData);
                        }
                    }), onFailure));
        }, "update encrypted repository password hashes [" + metadata.name() + "]", onFailure);
        // go on with the actual task to update the cluster state
        publishHashesStepListener.whenComplete(latestRepositoryData -> super.executeConsistentStateUpdate(createUpdateTaskSupplier,
                source, onFailure), onFailure);
    }

    @Override
    public boolean isCompatible(RepositoryMetadata repositoryMetadata) {
        // this type of repository is designed to work with the password settings changing on the fly
        return repositoryPasswords.equalsIgnorePasswordSettings(getMetadata(), repositoryMetadata);
    }

    @Override
    public String startVerification() {
        // verification bypasses the encrypted blobstore because it must work without published hashes
        return this.delegatedRepository.startVerification();
    }

    @Override
    public void endVerification(String seed) {
        // verification bypasses the encrypted blobstore because it must work without published hashes
        this.delegatedRepository.endVerification(seed);
    }

    @Override
    public void verify(String seed, DiscoveryNode localNode) {
        // verification bypasses the encrypted blobstore because it must work without published hashes
        this.delegatedRepository.verify(seed, localNode);
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
                this::wrapDek,
                this::unWrapDek,
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

    List<Tuple<String, byte[]>> wrapDek(String dekId, SecretKey dek) throws RepositoryException {
        final RepositoryMetadata repositoryMetadata = metadata;
        // check that the local passwords that are used to wrap the DEKs match the repository's
        verifyPublishedPasswordHashes(repositoryPasswords, repositoryMetadata);
        final List<SecureString> passwords = repositoryPasswords.passwordsForDekWrapping(repositoryMetadata);
        final List<Tuple<String, byte[]>> wrappedDeks = new ArrayList<>(passwords.size());
        try {
            for (SecureString password : passwords) {
                // we rely on the DEK Id being generated randomly so it can be used as a salt
                SecretKey kek = AESKeyUtils.generatePasswordBasedKey(password, dekId);
                String kekId = AESKeyUtils.computeId(kek);
                logger.debug("Repository [{}] computed KEK [{}] for DEK [{}] for wrapping", repositoryMetadata.name(), kekId, dekId);
                byte[] encryptedDEKBytes = AESKeyUtils.wrap(kek, dek);
                logger.trace("Repository [{}] successfully wrapped DEK [{}] using KEK [{}]", repositoryMetadata.name(), dekId, kekId);
                if (encryptedDEKBytes.length != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                    throw new IllegalStateException("Wrapped DEK [" + dekId + "] has unexpected length [" + encryptedDEKBytes.length + "]");
                }
                wrappedDeks.add(new Tuple<>(kekId, encryptedDEKBytes));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RepositoryException(repositoryMetadata.name(), "Interrupted while wrapping DEK [" + dekId + "]", e);
        } catch (Exception e) {
            throw new RepositoryException(repositoryMetadata.name(), "Unexpected exception while wrapping DEK [" + dekId + "]", e);
        }
        return wrappedDeks;
    }

    Tuple<String, CheckedFunction<byte[], SecretKey, RepositoryException>> unWrapDek(String dekId) throws RepositoryException {
        final RepositoryMetadata repositoryMetadata = metadata;
        final SecureString currentPassword = repositoryPasswords.currentLocalPassword(repositoryMetadata);
        try {
            final SecretKey kek = AESKeyUtils.generatePasswordBasedKey(currentPassword, dekId);
            final String kekId = AESKeyUtils.computeId(kek);
            logger.debug("Repository [{}] computed KEK [{}] for DEK [{}] for unwrapping", repositoryMetadata.name(), kekId, dekId);
            return new Tuple<>(kekId, (encryptedDEKBytes) -> {
                try {
                    if (encryptedDEKBytes.length != AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES) {
                        throw new IllegalStateException("Wrapped DEK [" + dekId + "] has unexpected length [" +
                                encryptedDEKBytes.length + "]");
                    }
                    SecretKey dek = AESKeyUtils.unwrap(kek, encryptedDEKBytes);
                    logger.trace("Repository [{}] successfully unwrapped DEK [{}] using KEK [{}]", repositoryMetadata.name(), dekId, kekId);
                    return dek;
                } catch (Exception e) {
                    throw new RepositoryException(repositoryMetadata.name(), "Unexpected exception while unwrapping DEK [" + dekId + "]. " +
                            "Most likely the encryption metadata in the repository has been corrupted.", e);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RepositoryException(repositoryMetadata.name(), "Interrupted while unwrapping DEK [" + dekId + "]", e);
        } catch (Exception e) {
            throw new RepositoryException(repositoryMetadata.name(), "Unexpected exception while unwrapping DEK [" + dekId + "]", e);
        }
    }

    // pkg-private for tests
    static final class EncryptedBlobStore implements BlobStore {
        private final BlobStore delegatedBlobStore;
        private final BlobPath delegatedBasePath;
        private final String repositoryName;
        private final CheckedBiFunction<String, SecretKey, List<Tuple<String, byte[]>>, RepositoryException> dekWrapper;
        private final CheckedFunction<String, Tuple<String, CheckedFunction<byte[], SecretKey, RepositoryException>>,
                RepositoryException> dekUnwrapper;
        private final Cache<String, SecretKey> dekCache;
        private final CheckedSupplier<SingleUseKey, IOException> singleUseDEKSupplier;

        EncryptedBlobStore(
            BlobStore delegatedBlobStore,
            BlobPath delegatedBasePath,
            String repositoryName,
            CheckedBiFunction<String, SecretKey, List<Tuple<String, byte[]>>, RepositoryException> dekWrapper,
            CheckedFunction<String, Tuple<String, CheckedFunction<byte[], SecretKey, RepositoryException>>,
                    RepositoryException> dekUnwrapper,
            Supplier<Tuple<BytesReference, SecretKey>> dekGenerator,
            Cache<String, SecretKey> dekCache
        ) {
            this.delegatedBlobStore = delegatedBlobStore;
            this.delegatedBasePath = delegatedBasePath;
            this.repositoryName = repositoryName;
            this.dekWrapper = dekWrapper;
            this.dekUnwrapper = dekUnwrapper;
            this.dekCache = dekCache;
            this.singleUseDEKSupplier = SingleUseKey.createSingleUseKeySupplier(() -> {
                Tuple<BytesReference, SecretKey> newDEK = dekGenerator.get();
                // store the newly generated DEK before making it available
                storeDEK(newDEK.v1().utf8ToString(), newDEK.v2());
                return newDEK;
            });
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

        private SecretKey loadDEK(String dekId) throws IOException, RepositoryException {
            final BlobPath dekBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(dekId);
            logger.debug("Repository [{}] loading wrapped DEK [{}] from blob path {}", repositoryName, dekId, dekBlobPath);
            final BlobContainer dekBlobContainer = delegatedBlobStore.blobContainer(dekBlobPath);
            final Tuple<String, CheckedFunction<byte[], SecretKey, RepositoryException>> unwrapper = dekUnwrapper.apply(dekId);
            final byte[] encryptedDEKBytes = new byte[AESKeyUtils.WRAPPED_KEY_LENGTH_IN_BYTES];
            try (InputStream encryptedDEKInputStream = dekBlobContainer.readBlob(unwrapper.v1())) {
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
                // can move the repository in the corrupted state
                throw new RepositoryException(repositoryName,
                    "Failure to read and decrypt DEK ["
                        + dekId
                        + "] from "
                        + dekBlobContainer.path()
                        + ". Most likely the repository password is incorrect, where previous "
                        + "snapshots have used a different password.",
                    e
                );
            }
            logger.trace(() -> new ParameterizedMessage("Repository [{}] successfully read DEK [{}] from path {} {}", repositoryName,
                    dekId, dekBlobPath.add(unwrapper.v1())));
            return unwrapper.v2().apply(encryptedDEKBytes);
        }

        // pkg-private for tests
        void storeDEK(String dekId, SecretKey dek) throws IOException, RepositoryException {
            final BlobPath dekBlobPath = delegatedBasePath.add(DEK_ROOT_CONTAINER).add(dekId);
            logger.debug("Repository [{}] storing wrapped DEK [{}] under blob path {}", repositoryName, dekId, dekBlobPath);
            final BlobContainer dekBlobContainer = delegatedBlobStore.blobContainer(dekBlobPath);
            for (Tuple<String, byte[]> wrappedDek : dekWrapper.apply(dekId, dek)) {
                dekBlobContainer.writeBlobAtomic(wrappedDek.v1(), new BytesArray(wrappedDek.v2()), true);
                logger.debug(() -> new ParameterizedMessage("Repository [{}] successfully stored wrapped DEK [{}] under path [{}]",
                        repositoryName, dekId, dekBlobPath.add(wrappedDek.v1())));
            }
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final Iterator<String> pathIterator = path.iterator();
            BlobPath delegatedBlobContainerPath = delegatedBasePath;
            while (pathIterator.hasNext()) {
                delegatedBlobContainerPath = delegatedBlobContainerPath.add(pathIterator.next());
            }
            final BlobContainer delegatedBlobContainer = delegatedBlobStore.blobContainer(delegatedBlobContainerPath);
            return new EncryptedBlobContainer(path, repositoryName, delegatedBlobContainer, singleUseDEKSupplier, this::getDEKById);
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

    private static void verifyPublishedPasswordHashes(RepositoryPasswords repositoryPasswords,
                                                      RepositoryMetadata repositoryMetadata) throws RepositoryException {
        final boolean hashesVerified;
        try {
            hashesVerified = repositoryPasswords.verifyPublishedPasswordHashes(repositoryMetadata);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RepositoryException(repositoryMetadata.name(), "Interrupted while verifying password hashes", e);
        } catch (Exception e) {
            throw new RepositoryException(repositoryMetadata.name(), "Unexpected exception while veryfing password hashes", e);
        }
        if (false == hashesVerified) {
            if (false == repositoryPasswords.containsPasswordHashes(repositoryMetadata)) {
                throw new RepositoryException(repositoryMetadata.name(), "No password hashes published");
            }
            if (false == repositoryPasswords.containsRequiredPasswordHashes(repositoryMetadata)) {
                throw new RepositoryException(repositoryMetadata.name(), "Password hashes missing for some passwords");
            }
            throw new RepositoryException(repositoryMetadata.name(), "The repository passwords on the local node are different");
        }
    }
}
