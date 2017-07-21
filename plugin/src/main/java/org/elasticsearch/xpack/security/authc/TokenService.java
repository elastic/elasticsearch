/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.InternalClient;
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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
    private static final int IV_BYTES = 12;
    private static final int VERSION_BYTES = 4;
    private static final String ENCRYPTION_CIPHER = "AES/GCM/NoPadding";
    private static final String EXPIRED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XPackPlugin.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token expired\"";
    private static final String MALFORMED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XPackPlugin.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token is malformed\"";
    private static final String TYPE = "doc";

    public static final String THREAD_POOL_NAME = XPackPlugin.SECURITY + "-token-key";
    public static final Setting<SecureString> TOKEN_PASSPHRASE = SecureSetting.secureString("xpack.security.authc.token.passphrase", null);
    public static final Setting<TimeValue> TOKEN_EXPIRATION = Setting.timeSetting("xpack.security.authc.token.timeout",
            TimeValue.timeValueMinutes(20L), TimeValue.timeValueSeconds(1L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting("xpack.security.authc.token.delete.interval",
                    TimeValue.timeValueMinutes(30L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting("xpack.security.authc.token.delete.timeout",
            TimeValue.MINUS_ONE, Property.NodeScope);
    public static final String DEFAULT_PASSPHRASE = "changeme is a terrible password, so let's not use it anymore!";

    static final String DOC_TYPE = "invalidated-token";
    static final int MINIMUM_BYTES = VERSION_BYTES + SALT_BYTES + IV_BYTES + 1;
    static final int MINIMUM_BASE64_BYTES = Double.valueOf(Math.ceil((4 * MINIMUM_BYTES) / 3)).intValue();

    private final SecureRandom secureRandom = new SecureRandom();
    private final Cache<BytesKey, SecretKey> keyCache;
    private final SecureString tokenPassphrase;
    private final Clock clock;
    private final TimeValue expirationDelay;
    private final TimeValue deleteInterval;
    private final BytesKey salt;
    private final InternalClient internalClient;
    private final SecurityLifecycleService lifecycleService;
    private final ExpiredTokenRemover expiredTokenRemover;
    private final boolean enabled;
    private final byte[] currentVersionBytes;

    private volatile long lastExpirationRunMs;


    /**
     * Creates a new token service
     * @param settings the node settings
     * @param clock the clock that will be used for comparing timestamps
     * @param internalClient the client to use when checking for revocations
     */
    public TokenService(Settings settings, Clock clock, InternalClient internalClient,
                        SecurityLifecycleService lifecycleService) throws GeneralSecurityException {
        super(settings);
        byte[] saltArr = new byte[SALT_BYTES];
        secureRandom.nextBytes(saltArr);
        this.salt = new BytesKey(saltArr);
        this.keyCache = CacheBuilder.<BytesKey, SecretKey>builder()
                .setExpireAfterAccess(TimeValue.timeValueMinutes(60L))
                .setMaximumWeight(500L)
                .build();
        final SecureString tokenPassphraseValue = TOKEN_PASSPHRASE.get(settings);
        if (tokenPassphraseValue.length() == 0) {
            // setting didn't exist - we should only be in a non-production mode for this
            this.tokenPassphrase = new SecureString(DEFAULT_PASSPHRASE.toCharArray());
        } else {
            this.tokenPassphrase = tokenPassphraseValue;
        }

        this.clock = clock.withZone(ZoneOffset.UTC);
        this.expirationDelay = TOKEN_EXPIRATION.get(settings);
        this.internalClient = internalClient;
        this.lifecycleService = lifecycleService;
        this.lastExpirationRunMs = internalClient.threadPool().relativeTimeInMillis();
        this.deleteInterval = DELETE_INTERVAL.get(settings);
        this.enabled = XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings);
        this.expiredTokenRemover = new ExpiredTokenRemover(settings, internalClient);
        this.currentVersionBytes = ByteBuffer.allocate(4).putInt(Version.CURRENT.id).array();
        ensureEncryptionCiphersSupported();
        try (SecureString closeableChars = tokenPassphrase.clone()) {
            keyCache.put(salt, computeSecretKey(closeableChars.getChars(), salt.bytes));
        }
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
        StreamInput in = new InputStreamStreamInput(
                Base64.getDecoder().wrap(new ByteArrayInputStream(token.getBytes(StandardCharsets.UTF_8))));
        if (in.available() < MINIMUM_BASE64_BYTES) {
            logger.debug("invalid token");
            listener.onResponse(null);
        } else {
            // the token exists and the value is at least as long as we'd expect
            final Version version = Version.readVersion(in);
            if (version.before(Version.V_5_5_0)) {
                listener.onResponse(null);
            } else {
                final BytesKey decodedSalt = new BytesKey(in.readByteArray());
                final SecretKey decodeKey = keyCache.get(decodedSalt);
                final byte[] iv = in.readByteArray();
                if (decodeKey != null) {
                    try {
                        decryptToken(in, getDecryptionCipher(iv, decodeKey, version, decodedSalt), version, listener);
                    } catch (GeneralSecurityException e) {
                        // could happen with a token that is not ours
                        logger.debug("invalid token", e);
                        listener.onResponse(null);
                    }
                } else {
                    /* As a measure of protected against DOS, we can pass requests requiring a key
                     * computation off to a single thread executor. For normal usage, the initial
                     * request(s) that require a key computation will be delayed and there will be
                     * some additional latency.
                     */
                    internalClient.threadPool().executor(THREAD_POOL_NAME)
                            .submit(new KeyComputingRunnable(in, iv, version, decodedSalt, listener));
                }
            }
        }
    }

    private void decryptToken(StreamInput in, Cipher cipher, Version version, ActionListener<UserToken> listener) throws IOException {
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
                            internalClient.prepareIndex(SecurityLifecycleService.SECURITY_INDEX_NAME, TYPE, id)
                                .setOpType(OpType.CREATE)
                                .setSource("doc_type", DOC_TYPE, "expiration_time", getExpirationTime().toEpochMilli())
                                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                                .execute(new ActionListener<IndexResponse>() {
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
                                });
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
            internalClient.prepareGet(SecurityLifecycleService.SECURITY_INDEX_NAME, TYPE, getDocumentId(userToken))
                    .execute(new ActionListener<GetResponse>() {

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
                    });
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
            if (internalClient.threadPool().relativeTimeInMillis() - lastExpirationRunMs > deleteInterval.getMillis()) {
                expiredTokenRemover.submit(internalClient.threadPool());
                lastExpirationRunMs = internalClient.threadPool().relativeTimeInMillis();
            }
        }
    }

    /**
     * Gets the token from the <code>Authorization</code> header if the header begins with
     * <code>Bearer </code>
     */
    private String getFromHeader(ThreadContext threadContext) {
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
            Version.writeVersion(Version.CURRENT, out);
            out.writeByteArray(salt.bytes);
            final byte[] initializationVector = getNewInitializationVector();
            out.writeByteArray(initializationVector);
            try (CipherOutputStream encryptedOutput = new CipherOutputStream(out, getEncryptionCipher(initializationVector));
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

    private Cipher getEncryptionCipher(byte[] iv) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        cipher.init(Cipher.ENCRYPT_MODE, keyCache.get(salt), new GCMParameterSpec(128, iv), secureRandom);
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

        KeyComputingRunnable(StreamInput input, byte[] iv, Version version, BytesKey decodedSalt, ActionListener<UserToken> listener) {
            this.in = input;
            this.version = version;
            this.decodedSalt = decodedSalt;
            this.listener = listener;
            this.iv = iv;
        }

        @Override
        protected void doRun() {
            try {
                final SecretKey computedKey = keyCache.computeIfAbsent(decodedSalt, (salt) -> {
                    try (SecureString closeableChars = tokenPassphrase.clone()) {
                        return computeSecretKey(closeableChars.getChars(), decodedSalt.bytes);
                    }
                });
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
    }
}
