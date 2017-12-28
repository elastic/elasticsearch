/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.XpackField;
import org.elasticsearch.xpack.security.SecurityLifecycleService;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.xpack.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

/**
 * Service responsible for the creation, validation, and other management of {@link UserToken}
 * objects for authentication
 */
public final class TokenService extends AbstractComponent {

    /**
     * The parameters below are used to generate the cryptographic key that is used to encrypt the
     * values returned by this service. These parameters are based off of the
     * <a href="https://www.owasp.org/index.php/Password_Storage_Cheat_Sheet">OWASP Password Storage
     * Cheat Sheet</a> and the <a href="https://pages.nist.gov/800-63-3/sp800-63b.html#sec5">
     * NIST Digital Identity Guidelines</a>
     */
    private static final int ITERATIONS = 100000;
    private static final String KDF_ALGORITHM = "PBKDF2withHMACSHA512";
    private static final int SALT_BYTES = 32;
    private static final int KEY_BYTES = 64;
    private static final int IV_BYTES = 12;
    private static final int VERSION_BYTES = 4;
    private static final String ENCRYPTION_CIPHER = "AES/GCM/NoPadding";
    private static final String EXPIRED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XpackField.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token expired\"";
    private static final String MALFORMED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XpackField.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token is malformed\"";
    private static final String TYPE = "doc";

    public static final String THREAD_POOL_NAME = XpackField.SECURITY + "-token-key";
    public static final Setting<TimeValue> TOKEN_EXPIRATION = Setting.timeSetting("xpack.security.authc.token.timeout",
            TimeValue.timeValueMinutes(20L), TimeValue.timeValueSeconds(1L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting("xpack.security.authc.token.delete.interval",
                    TimeValue.timeValueMinutes(30L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting("xpack.security.authc.token.delete.timeout",
            TimeValue.MINUS_ONE, Property.NodeScope);

    static final String DOC_TYPE = "invalidated-token";
    static final int MINIMUM_BYTES = VERSION_BYTES + SALT_BYTES + IV_BYTES + 1;
    static final int MINIMUM_BASE64_BYTES = Double.valueOf(Math.ceil((4 * MINIMUM_BYTES) / 3)).intValue();

    private final SecureRandom secureRandom = new SecureRandom();
    private final ClusterService clusterService;
    private final Clock clock;
    private final TimeValue expirationDelay;
    private final TimeValue deleteInterval;
    private final Client client;
    private final SecurityLifecycleService lifecycleService;
    private final ExpiredTokenRemover expiredTokenRemover;
    private final boolean enabled;
    private final byte[] currentVersionBytes;
    private volatile TokenKeys keyCache;
    private volatile long lastExpirationRunMs;
    private final AtomicLong createdTimeStamps = new AtomicLong(-1);
    private static final Version TOKEN_SERVICE_VERSION = Version.CURRENT;

    /**
     * Creates a new token service
     * @param settings the node settings
     * @param clock the clock that will be used for comparing timestamps
     * @param client the client to use when checking for revocations
     */
    public TokenService(Settings settings, Clock clock, Client client,
                        SecurityLifecycleService lifecycleService, ClusterService clusterService) throws GeneralSecurityException {
        super(settings);
        byte[] saltArr = new byte[SALT_BYTES];
        secureRandom.nextBytes(saltArr);

        final SecureString tokenPassphrase = generateTokenKey();
        this.clock = clock.withZone(ZoneOffset.UTC);
        this.expirationDelay = TOKEN_EXPIRATION.get(settings);
        this.client = client;
        this.lifecycleService = lifecycleService;
        this.lastExpirationRunMs = client.threadPool().relativeTimeInMillis();
        this.deleteInterval = DELETE_INTERVAL.get(settings);
        this.enabled = XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings);
        this.expiredTokenRemover = new ExpiredTokenRemover(settings, client);
        this.currentVersionBytes = ByteBuffer.allocate(4).putInt(TOKEN_SERVICE_VERSION.id).array();
        ensureEncryptionCiphersSupported();
        KeyAndCache keyAndCache = new KeyAndCache(new KeyAndTimestamp(tokenPassphrase.clone(), createdTimeStamps.incrementAndGet()),
                new BytesKey(saltArr));
        keyCache = new TokenKeys(Collections.singletonMap(keyAndCache.getKeyHash(), keyAndCache), keyAndCache.getKeyHash());
        this.clusterService = clusterService;
        initialize(clusterService);
        getTokenMetaData();
    }


    /**
     * Create a token based on the provided authentication
     */
    public UserToken createUserToken(Authentication authentication)
            throws IOException, GeneralSecurityException {
        ensureEnabled();
        final Instant expiration = getExpirationTime();
        return new UserToken(authentication, expiration);
    }

    /**
     * Looks in the context to see if the request provided a header with a user token
     */
    void getAndValidateToken(ThreadContext ctx, ActionListener<UserToken> listener) {
        if (enabled) {
            final String token = getFromHeader(ctx);
            if (token == null) {
                listener.onResponse(null);
            } else {
                try {
                    decodeToken(token, ActionListener.wrap(userToken -> {
                        if (userToken != null) {
                            Instant currentTime = clock.instant();
                            if (currentTime.isAfter(userToken.getExpirationTime())) {
                                // token expired
                                listener.onFailure(expiredTokenException());
                            } else {
                                checkIfTokenIsRevoked(userToken, listener);
                            }
                        } else {
                            listener.onResponse(null);
                        }
                    }, listener::onFailure));
                } catch (IOException e) {
                    // could happen with a token that is not ours
                    logger.debug("invalid token", e);
                    listener.onResponse(null);
                }
            }
        } else {
            listener.onResponse(null);
        }
    }

    void decodeToken(String token, ActionListener<UserToken> listener) throws IOException {
        // We intentionally do not use try-with resources since we need to keep the stream open if we need to compute a key!
        byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        StreamInput in = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)), bytes.length);
        if (in.available() < MINIMUM_BASE64_BYTES) {
            logger.debug("invalid token");
            listener.onResponse(null);
        } else {
            // the token exists and the value is at least as long as we'd expect
            final Version version = Version.readVersion(in);
            final BytesKey decodedSalt = new BytesKey(in.readByteArray());
            final BytesKey passphraseHash = new BytesKey(in.readByteArray());
            KeyAndCache keyAndCache = keyCache.get(passphraseHash);
            if (keyAndCache != null) {
                final SecretKey decodeKey = keyAndCache.getKey(decodedSalt);
                final byte[] iv = in.readByteArray();
                if (decodeKey != null) {
                    try {
                        decryptToken(in, getDecryptionCipher(iv, decodeKey, version, decodedSalt), version, listener);
                    } catch (GeneralSecurityException e) {
                        // could happen with a token that is not ours
                        logger.warn("invalid token", e);
                        listener.onResponse(null);
                    }
                } else {
                    /* As a measure of protected against DOS, we can pass requests requiring a key
                     * computation off to a single thread executor. For normal usage, the initial
                     * request(s) that require a key computation will be delayed and there will be
                     * some additional latency.
                     */
                    client.threadPool().executor(THREAD_POOL_NAME)
                            .submit(new KeyComputingRunnable(in, iv, version, decodedSalt, listener, keyAndCache));
                }
            } else {
                logger.debug("invalid key {} key: {}", passphraseHash, keyCache.cache.keySet());
                listener.onResponse(null);
            }
        }
    }

    private static void decryptToken(StreamInput in, Cipher cipher, Version version, ActionListener<UserToken> listener) throws
            IOException {
        try (CipherInputStream cis = new CipherInputStream(in, cipher); StreamInput decryptedInput = new InputStreamStreamInput(cis)) {
            decryptedInput.setVersion(version);
            listener.onResponse(new UserToken(decryptedInput));
        }
    }

    /**
     * This method records an entry to indicate that a token with a given id has been expired.
     */
    public void invalidateToken(String tokenString, ActionListener<Boolean> listener) {
        ensureEnabled();
        if (lifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        } else if (lifecycleService.isSecurityIndexWriteable() == false) {
            listener.onFailure(new IllegalStateException("cannot write to the tokens index"));
        } else if (Strings.isNullOrEmpty(tokenString)) {
            listener.onFailure(new IllegalArgumentException("token must be provided"));
        } else {
            maybeStartTokenRemover();
            try {
                decodeToken(tokenString, ActionListener.wrap(userToken -> {
                    if (userToken == null) {
                        listener.onFailure(malformedTokenException());
                    } else if (userToken.getExpirationTime().isBefore(clock.instant())) {
                        // no need to invalidate - it's already expired
                        listener.onResponse(false);
                    } else {
                        final String id = getDocumentId(userToken);
                        lifecycleService.createIndexIfNeededThenExecute(listener, () -> {
                            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                                    client.prepareIndex(SecurityLifecycleService.SECURITY_INDEX_NAME, TYPE, id)
                                        .setOpType(OpType.CREATE)
                                        .setSource("doc_type", DOC_TYPE, "expiration_time", getExpirationTime().toEpochMilli())
                                        .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).request(),
                                    new ActionListener<IndexResponse>() {
                                        @Override
                                        public void onResponse(IndexResponse indexResponse) {
                                            listener.onResponse(indexResponse.getResult() == Result.CREATED);
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            if (e instanceof VersionConflictEngineException) {
                                                // doc already exists
                                                listener.onResponse(false);
                                            } else {
                                                listener.onFailure(e);
                                            }
                                        }
                                    }, client::index);
                        });
                    }
                }, listener::onFailure));
            } catch (IOException e) {
                logger.error("received a malformed token as part of a invalidation request", e);
                listener.onFailure(malformedTokenException());
            }
        }
    }

    private static String getDocumentId(UserToken userToken) {
        return DOC_TYPE + "_" + userToken.getId();
    }

    private void ensureEnabled() {
        if (enabled == false) {
            throw new IllegalStateException("tokens are not enabled");
        }
    }

    /**
     * Checks if the token has been stored as a revoked token to ensure we do not allow tokens that
     * have been explicitly cleared.
     */
    private void checkIfTokenIsRevoked(UserToken userToken, ActionListener<UserToken> listener) {
        if (lifecycleService.isSecurityIndexAvailable()) {
            if (lifecycleService.isSecurityIndexOutOfDate()) {
                listener.onFailure(new IllegalStateException(
                    "Security index is not on the current version - the native realm will not be operational until " +
                    "the upgrade API is run on the security index"));
                return;
            }
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                    client.prepareGet(SecurityLifecycleService.SECURITY_INDEX_NAME, TYPE, getDocumentId(userToken)).request(),
                    new ActionListener<GetResponse>() {

                        @Override
                        public void onResponse(GetResponse response) {
                            if (response.isExists()) {
                                // this token is explicitly expired!
                                listener.onFailure(expiredTokenException());
                            } else {
                                listener.onResponse(userToken);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // if the index or the shard is not there / available we assume that
                            // the token is not valid
                            if (TransportActions.isShardNotAvailableException(e)) {
                                logger.warn("failed to get token [{}] since index is not available", userToken.getId());
                                listener.onResponse(null);
                            } else {
                                logger.error(new ParameterizedMessage("failed to get token [{}]", userToken.getId()), e);
                                listener.onFailure(e);
                            }
                        }
                    }, client::get);
        } else if (lifecycleService.isSecurityIndexExisting()) {
            // index exists but the index isn't available, do not trust the token
            logger.warn("could not validate token as the security index is not available");
            listener.onResponse(null);
        } else {
            // index doesn't exist so the token is considered valid.
            listener.onResponse(userToken);
        }
    }

    public TimeValue getExpirationDelay() {
        return expirationDelay;
    }

    private Instant getExpirationTime() {
        return clock.instant().plusSeconds(expirationDelay.getSeconds());
    }

    private void maybeStartTokenRemover() {
        if (lifecycleService.isSecurityIndexAvailable()) {
            if (client.threadPool().relativeTimeInMillis() - lastExpirationRunMs > deleteInterval.getMillis()) {
                expiredTokenRemover.submit(client.threadPool());
                lastExpirationRunMs = client.threadPool().relativeTimeInMillis();
            }
        }
    }

    /**
     * Gets the token from the <code>Authorization</code> header if the header begins with
     * <code>Bearer </code>
     */
    String getFromHeader(ThreadContext threadContext) {
        String header = threadContext.getHeader("Authorization");
        if (Strings.hasLength(header) && header.startsWith("Bearer ")
                && header.length() > "Bearer ".length()) {
            return header.substring("Bearer ".length());
        }
        return null;
    }

    /**
     * Serializes a token to a String containing an encrypted representation of the token
     */
    public String getUserTokenString(UserToken userToken) throws IOException, GeneralSecurityException {
        // we know that the minimum length is larger than the default of the ByteArrayOutputStream so set the size to this explicitly
        try (ByteArrayOutputStream os = new ByteArrayOutputStream(MINIMUM_BASE64_BYTES);
             OutputStream base64 = Base64.getEncoder().wrap(os);
             StreamOutput out = new OutputStreamStreamOutput(base64)) {
            KeyAndCache keyAndCache = keyCache.activeKeyCache;
            Version.writeVersion(TOKEN_SERVICE_VERSION, out);
            out.writeByteArray(keyAndCache.getSalt().bytes);
            out.writeByteArray(keyAndCache.getKeyHash().bytes); // TODO this requires a BWC layer in 5.6
            final byte[] initializationVector = getNewInitializationVector();
            out.writeByteArray(initializationVector);
            try (CipherOutputStream encryptedOutput = new CipherOutputStream(out, getEncryptionCipher(initializationVector, keyAndCache));
                 StreamOutput encryptedStreamOutput = new OutputStreamStreamOutput(encryptedOutput)) {
                userToken.writeTo(encryptedStreamOutput);
                encryptedStreamOutput.close();
                return new String(os.toByteArray(), StandardCharsets.UTF_8);
            }
        }
    }

    private void ensureEncryptionCiphersSupported() throws NoSuchPaddingException, NoSuchAlgorithmException {
        Cipher.getInstance(ENCRYPTION_CIPHER);
        SecretKeyFactory.getInstance(KDF_ALGORITHM);
    }

    private Cipher getEncryptionCipher(byte[] iv, KeyAndCache keyAndCache) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        BytesKey salt = keyAndCache.getSalt();
        cipher.init(Cipher.ENCRYPT_MODE, keyAndCache.getKey(salt), new GCMParameterSpec(128, iv), secureRandom);
        cipher.updateAAD(currentVersionBytes);
        cipher.updateAAD(salt.bytes);
        return cipher;
    }

    private Cipher getDecryptionCipher(byte[] iv, SecretKey key, Version version,
                                       BytesKey salt) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, iv), secureRandom);
        cipher.updateAAD(ByteBuffer.allocate(4).putInt(version.id).array());
        cipher.updateAAD(salt.bytes);
        return cipher;
    }

    private byte[] getNewInitializationVector() {
        final byte[] initializationVector = new byte[IV_BYTES];
        secureRandom.nextBytes(initializationVector);
        return initializationVector;
    }

    /**
     * Generates a secret key based off of the provided password and salt.
     * This method is computationally expensive.
     */
    static SecretKey computeSecretKey(char[] rawPassword, byte[] salt)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(KDF_ALGORITHM);
        PBEKeySpec keySpec = new PBEKeySpec(rawPassword, salt, ITERATIONS, 128);
        SecretKey tmp = secretKeyFactory.generateSecret(keySpec);
        return new SecretKeySpec(tmp.getEncoded(), "AES");
    }

    /**
     * Creates an {@link ElasticsearchSecurityException} that indicates the token was expired. It
     * is up to the client to re-authenticate and obtain a new token
     */
    private static ElasticsearchSecurityException expiredTokenException() {
        ElasticsearchSecurityException e =
                new ElasticsearchSecurityException("token expired", RestStatus.UNAUTHORIZED);
        e.addHeader("WWW-Authenticate", EXPIRED_TOKEN_WWW_AUTH_VALUE);
        return e;
    }

    /**
     * Creates an {@link ElasticsearchSecurityException} that indicates the token was expired. It
     * is up to the client to re-authenticate and obtain a new token
     */
    private static ElasticsearchSecurityException malformedTokenException() {
        ElasticsearchSecurityException e =
                new ElasticsearchSecurityException("token malformed", RestStatus.UNAUTHORIZED);
        e.addHeader("WWW-Authenticate", MALFORMED_TOKEN_WWW_AUTH_VALUE);
        return e;
    }

    boolean isExpiredTokenException(ElasticsearchSecurityException e) {
        final List<String> headers = e.getHeader("WWW-Authenticate");
        return headers != null && headers.stream().anyMatch(EXPIRED_TOKEN_WWW_AUTH_VALUE::equals);
    }

    boolean isExpirationInProgress() {
        return expiredTokenRemover.isExpirationInProgress();
    }

    private class KeyComputingRunnable extends AbstractRunnable {

        private final StreamInput in;
        private final Version version;
        private final BytesKey decodedSalt;
        private final ActionListener<UserToken> listener;
        private final byte[] iv;
        private final KeyAndCache keyAndCache;

        KeyComputingRunnable(StreamInput input, byte[] iv, Version version, BytesKey decodedSalt, ActionListener<UserToken> listener,
                             KeyAndCache keyAndCache) {
            this.in = input;
            this.version = version;
            this.decodedSalt = decodedSalt;
            this.listener = listener;
            this.iv = iv;
            this.keyAndCache = keyAndCache;
        }

        @Override
        protected void doRun() {
            try {
                final SecretKey computedKey = keyAndCache.getOrComputeKey(decodedSalt);
                decryptToken(in, getDecryptionCipher(iv, computedKey, version, decodedSalt), version, listener);
            } catch (ExecutionException e) {
                if (e.getCause() != null &&
                        (e.getCause() instanceof GeneralSecurityException || e.getCause() instanceof IOException
                                || e.getCause() instanceof IllegalArgumentException)) {
                    // this could happen if another realm supports the Bearer token so we should
                    // see if another realm can use this token!
                    logger.debug("unable to decode bearer token", e);
                    listener.onResponse(null);
                } else {
                    listener.onFailure(e);
                }
            } catch (GeneralSecurityException | IOException e) {
                logger.debug("unable to decode bearer token", e);
                listener.onResponse(null);
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void onAfter() {
            IOUtils.closeWhileHandlingException(in);
        }
    }

    /**
     * Simple wrapper around bytes so that it can be used as a cache key. The hashCode is computed
     * once upon creation and cached.
     */
    static class BytesKey {

        final byte[] bytes;
        private final int hashCode;

        BytesKey(byte[] bytes) {
            this.bytes = bytes;
            this.hashCode = StringHelper.murmurhash3_x86_32(bytes, 0, bytes.length, StringHelper.GOOD_FAST_HASH_SEED);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (other instanceof BytesKey == false) {
                return false;
            }

            BytesKey otherBytes = (BytesKey) other;
            return Arrays.equals(otherBytes.bytes, bytes);
        }

        @Override
        public String toString() {
            return new BytesRef(bytes).toString();
        }
    }

    /**
     * Creates a new key unless present that is newer than the current active key and returns the corresponding metadata. Note:
     * this method doesn't modify the metadata used in this token service. See {@link #refreshMetaData(TokenMetaData)}
     */
    synchronized TokenMetaData generateSpareKey() {
        KeyAndCache maxKey = keyCache.cache.values().stream().max(Comparator.comparingLong(v -> v.keyAndTimestamp.timestamp)).get();
        KeyAndCache currentKey = keyCache.activeKeyCache;
        if (currentKey == maxKey) {
            long timestamp = createdTimeStamps.incrementAndGet();
            while (true) {
                byte[] saltArr = new byte[SALT_BYTES];
                secureRandom.nextBytes(saltArr);
                SecureString tokenKey = generateTokenKey();
                KeyAndCache keyAndCache = new KeyAndCache(new KeyAndTimestamp(tokenKey, timestamp), new BytesKey(saltArr));
                if (keyCache.cache.containsKey(keyAndCache.getKeyHash())) {
                    continue; // collision -- generate a new key
                }
                return newTokenMetaData(keyCache.currentTokenKeyHash, Iterables.concat(keyCache.cache.values(),
                        Collections.singletonList(keyAndCache)));
            }
        }
        return newTokenMetaData(keyCache.currentTokenKeyHash, keyCache.cache.values());
    }

    /**
     * Rotate the current active key to the spare key created in the previous {@link #generateSpareKey()} call.
     */
    synchronized TokenMetaData rotateToSpareKey() {
        KeyAndCache maxKey = keyCache.cache.values().stream().max(Comparator.comparingLong(v -> v.keyAndTimestamp.timestamp)).get();
        if (maxKey == keyCache.activeKeyCache) {
            throw new IllegalStateException("call generateSpareKey first");
        }
        return newTokenMetaData(maxKey.getKeyHash(), keyCache.cache.values());
    }

    /**
     * Prunes the keys and keeps up to the latest N keys around
     * @param numKeysToKeep the number of keys to keep.
     */
    synchronized TokenMetaData pruneKeys(int numKeysToKeep) {
        if (keyCache.cache.size() <= numKeysToKeep) {
            return getTokenMetaData(); // nothing to do
        }
        Map<BytesKey, KeyAndCache> map = new HashMap<>(keyCache.cache.size() + 1);
        KeyAndCache currentKey = keyCache.get(keyCache.currentTokenKeyHash);
        ArrayList<KeyAndCache> entries = new ArrayList<>(keyCache.cache.values());
        Collections.sort(entries,
                (left, right) ->  Long.compare(right.keyAndTimestamp.timestamp, left.keyAndTimestamp.timestamp));
        for (KeyAndCache value: entries) {
            if (map.size() < numKeysToKeep || value.keyAndTimestamp.timestamp >= currentKey
                    .keyAndTimestamp.timestamp) {
                logger.debug("keeping key {} ", value.getKeyHash());
                map.put(value.getKeyHash(), value);
            } else {
                logger.debug("prune key {} ", value.getKeyHash());
            }
        }
        assert map.isEmpty() == false;
        assert map.containsKey(keyCache.currentTokenKeyHash);
        return newTokenMetaData(keyCache.currentTokenKeyHash, map.values());
    }

    /**
     * Returns the current in-use metdata of this {@link TokenService}
     */
    public synchronized TokenMetaData getTokenMetaData() {
        return newTokenMetaData(keyCache.currentTokenKeyHash, keyCache.cache.values());
    }

    private TokenMetaData newTokenMetaData(BytesKey activeTokenKey, Iterable<KeyAndCache> iterable) {
        List<KeyAndTimestamp> list = new ArrayList<>();
        for (KeyAndCache v : iterable) {
            list.add(v.keyAndTimestamp);
        }
        return new TokenMetaData(list, activeTokenKey.bytes);
    }

    /**
     * Refreshes the current in-use metadata.
     */
    synchronized void refreshMetaData(TokenMetaData metaData) {
        BytesKey currentUsedKeyHash = new BytesKey(metaData.currentKeyHash);
        byte[] saltArr = new byte[SALT_BYTES];
        Map<BytesKey, KeyAndCache> map = new HashMap<>(metaData.keys.size());
        long maxTimestamp = createdTimeStamps.get();
        for (KeyAndTimestamp key :  metaData.keys) {
            secureRandom.nextBytes(saltArr);
            KeyAndCache keyAndCache = new KeyAndCache(key, new BytesKey(saltArr));
            maxTimestamp = Math.max(keyAndCache.keyAndTimestamp.timestamp, maxTimestamp);
            if (keyCache.cache.containsKey(keyAndCache.getKeyHash()) == false) {
                map.put(keyAndCache.getKeyHash(), keyAndCache);
            } else {
                map.put(keyAndCache.getKeyHash(), keyCache.get(keyAndCache.getKeyHash())); // maintain the cache we already have
            }
        }
        if (map.containsKey(currentUsedKeyHash) == false) {
            // this won't leak any secrets it's only exposing the current set of hashes
            throw new IllegalStateException("Current key is not in the map: " + map.keySet() + " key: " + currentUsedKeyHash);
        }
        createdTimeStamps.set(maxTimestamp);
        keyCache = new TokenKeys(Collections.unmodifiableMap(map), currentUsedKeyHash);
        logger.debug("refreshed keys current: {}, keys: {}", currentUsedKeyHash, keyCache.cache.keySet());
    }

    private SecureString generateTokenKey() {
        byte[] keyBytes = new byte[KEY_BYTES];
        byte[] encode = new byte[0];
        char[] ref = new char[0];
        try {
            secureRandom.nextBytes(keyBytes);
            encode = Base64.getUrlEncoder().withoutPadding().encode(keyBytes);
            ref = new char[encode.length];
            int len = UnicodeUtil.UTF8toUTF16(encode, 0, encode.length, ref);
            return new SecureString(Arrays.copyOfRange(ref, 0, len));
        } finally {
            Arrays.fill(keyBytes, (byte) 0x00);
            Arrays.fill(encode, (byte) 0x00);
            Arrays.fill(ref, (char) 0x00);
        }
    }

    synchronized String getActiveKeyHash() {
        return new BytesRef(Base64.getUrlEncoder().withoutPadding().encode(this.keyCache.currentTokenKeyHash.bytes)).utf8ToString();
    }

    void rotateKeysOnMaster(ActionListener<ClusterStateUpdateResponse> listener) {
        logger.info("rotate keys on master");
        TokenMetaData tokenMetaData = generateSpareKey();
        clusterService.submitStateUpdateTask("publish next key to prepare key rotation",
                new TokenMetadataPublishAction(
                        ActionListener.wrap((res) -> {
                            if (res.isAcknowledged()) {
                                TokenMetaData metaData = rotateToSpareKey();
                                clusterService.submitStateUpdateTask("publish next key to prepare key rotation",
                                        new TokenMetadataPublishAction(listener, metaData));
                            } else {
                                listener.onFailure(new IllegalStateException("not acked"));
                            }
                        }, listener::onFailure), tokenMetaData));
    }

    private final class TokenMetadataPublishAction extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final TokenMetaData tokenMetaData;

        protected TokenMetadataPublishAction(ActionListener<ClusterStateUpdateResponse> listener, TokenMetaData tokenMetaData) {
            super(new AckedRequest() {
                @Override
                public TimeValue ackTimeout() {
                    return AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
                }

                @Override
                public TimeValue masterNodeTimeout() {
                    return AcknowledgedRequest.DEFAULT_MASTER_NODE_TIMEOUT;
                }
            }, listener);
            this.tokenMetaData = tokenMetaData;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            if (tokenMetaData.equals(currentState.custom(TokenMetaData.TYPE))) {
                return currentState;
            }
            return ClusterState.builder(currentState).putCustom(TokenMetaData.TYPE, tokenMetaData).build();
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

    }

    private void initialize(ClusterService clusterService) {
        clusterService.addListener(event -> {
            ClusterState state = event.state();
            if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                return;
            }

            TokenMetaData custom = event.state().custom(TokenMetaData.TYPE);
            if (custom != null && custom.equals(getTokenMetaData()) == false) {
                logger.info("refresh keys");
                try {
                    refreshMetaData(custom);
                } catch (Exception e) {
                    logger.warn(e);
                }
                logger.info("refreshed keys");
            }
        });
    }

    static final class KeyAndTimestamp implements Writeable {
        private final SecureString key;
        private final long timestamp;

        private KeyAndTimestamp(SecureString key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        KeyAndTimestamp(StreamInput input) throws IOException {
            timestamp = input.readVLong();
            byte[] keyBytes = input.readByteArray();
            final char[] ref = new char[keyBytes.length];
            int len = UnicodeUtil.UTF8toUTF16(keyBytes, 0, keyBytes.length, ref);
            key = new SecureString(Arrays.copyOfRange(ref, 0, len));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(timestamp);
            BytesRef bytesRef = new BytesRef(key);
            out.writeVInt(bytesRef.length);
            out.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyAndTimestamp that = (KeyAndTimestamp) o;

            if (timestamp != that.timestamp) return false;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            return result;
        }
    }

    static final class KeyAndCache implements Closeable {
        private final KeyAndTimestamp keyAndTimestamp;
        private final Cache<BytesKey, SecretKey> keyCache;
        private final BytesKey salt;
        private final BytesKey keyHash;

        private KeyAndCache(KeyAndTimestamp keyAndTimestamp, BytesKey salt) {
            this.keyAndTimestamp = keyAndTimestamp;
            keyCache = CacheBuilder.<BytesKey, SecretKey>builder()
                    .setExpireAfterAccess(TimeValue.timeValueMinutes(60L))
                    .setMaximumWeight(500L)
                    .build();
            try {
                SecretKey secretKey = computeSecretKey(keyAndTimestamp.key.getChars(), salt.bytes);
                keyCache.put(salt, secretKey);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            this.salt = salt;
            this.keyHash = calculateKeyHash(keyAndTimestamp.key);
        }

        private SecretKey getKey(BytesKey salt) {
            return keyCache.get(salt);
        }

        public SecretKey getOrComputeKey(BytesKey decodedSalt) throws ExecutionException {
            return keyCache.computeIfAbsent(decodedSalt, (salt) -> {
                try (SecureString closeableChars = keyAndTimestamp.key.clone()) {
                    return computeSecretKey(closeableChars.getChars(), salt.bytes);
                }
            });
        }

        @Override
        public void close() throws IOException {
            keyAndTimestamp.key.close();
        }

        BytesKey getKeyHash() {
           return keyHash;
        }

        private static BytesKey calculateKeyHash(SecureString key) {
            MessageDigest messageDigest = null;
            try {
                messageDigest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e);
            }
            BytesRefBuilder b = new BytesRefBuilder();
            try {
                b.copyChars(key);
                BytesRef bytesRef = b.toBytesRef();
                try {
                    messageDigest.update(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                    return new BytesKey(Arrays.copyOfRange(messageDigest.digest(), 0, 8));
                } finally {
                    Arrays.fill(bytesRef.bytes, (byte) 0x00);
                }
            } finally {
                Arrays.fill(b.bytes(), (byte) 0x00);
            }
        }

        BytesKey getSalt() {
            return salt;
        }
    }


    private static final class TokenKeys {
        final Map<BytesKey, KeyAndCache> cache;
        final BytesKey currentTokenKeyHash;
        final KeyAndCache activeKeyCache;
        
        private TokenKeys(Map<BytesKey, KeyAndCache> cache, BytesKey currentTokenKeyHash) {
            this.cache = cache;
            this.currentTokenKeyHash = currentTokenKeyHash;
            this.activeKeyCache = cache.get(currentTokenKeyHash);
        }

        KeyAndCache get(BytesKey passphraseHash) {
            return cache.get(passphraseHash);
        }
    }

}
