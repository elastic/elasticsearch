/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.KeyAndTimestamp;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

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
import java.io.UncheckedIOException;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Service responsible for the creation, validation, and other management of {@link UserToken}
 * objects for authentication
 */
public final class TokenService {

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
    private static final String EXPIRED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XPackField.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token expired\"";
    private static final String MALFORMED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XPackField.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token is malformed\"";
    private static final String TYPE = "doc";

    public static final String THREAD_POOL_NAME = XPackField.SECURITY + "-token-key";
    public static final Setting<TimeValue> TOKEN_EXPIRATION = Setting.timeSetting("xpack.security.authc.token.timeout",
            TimeValue.timeValueMinutes(20L), TimeValue.timeValueSeconds(1L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting("xpack.security.authc.token.delete.interval",
            TimeValue.timeValueMinutes(30L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting("xpack.security.authc.token.delete.timeout",
            TimeValue.MINUS_ONE, Property.NodeScope);

    private static final String TOKEN_DOC_TYPE = "token";
    private static final String TOKEN_DOC_ID_PREFIX = TOKEN_DOC_TYPE + "_";
    static final int MINIMUM_BYTES = VERSION_BYTES + SALT_BYTES + IV_BYTES + 1;
    private static final int MINIMUM_BASE64_BYTES = Double.valueOf(Math.ceil((4 * MINIMUM_BYTES) / 3)).intValue();
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Logger logger = LogManager.getLogger(TokenService.class);

    private final SecureRandom secureRandom = new SecureRandom();
    private final Settings settings;
    private final ClusterService clusterService;
    private final Clock clock;
    private final TimeValue expirationDelay;
    private final TimeValue deleteInterval;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ExpiredTokenRemover expiredTokenRemover;
    private final boolean enabled;
    private volatile TokenKeys keyCache;
    private volatile long lastExpirationRunMs;
    private final AtomicLong createdTimeStamps = new AtomicLong(-1);

    /**
     * Creates a new token service
     *
     * @param settings the node settings
     * @param clock    the clock that will be used for comparing timestamps
     * @param client   the client to use when checking for revocations
     */
    public TokenService(Settings settings, Clock clock, Client client,
                        SecurityIndexManager securityIndex, ClusterService clusterService) throws GeneralSecurityException {
        byte[] saltArr = new byte[SALT_BYTES];
        secureRandom.nextBytes(saltArr);

        final SecureString tokenPassphrase = generateTokenKey();
        this.settings = settings;
        this.clock = clock.withZone(ZoneOffset.UTC);
        this.expirationDelay = TOKEN_EXPIRATION.get(settings);
        this.client = client;
        this.securityIndex = securityIndex;
        this.lastExpirationRunMs = client.threadPool().relativeTimeInMillis();
        this.deleteInterval = DELETE_INTERVAL.get(settings);
        this.enabled = isTokenServiceEnabled(settings);
        this.expiredTokenRemover = new ExpiredTokenRemover(settings, client);
        ensureEncryptionCiphersSupported();
        KeyAndCache keyAndCache = new KeyAndCache(new KeyAndTimestamp(tokenPassphrase, createdTimeStamps.incrementAndGet()),
                new BytesKey(saltArr));
        keyCache = new TokenKeys(Collections.singletonMap(keyAndCache.getKeyHash(), keyAndCache), keyAndCache.getKeyHash());
        this.clusterService = clusterService;
        initialize(clusterService);
        getTokenMetaData();
    }

    public static Boolean isTokenServiceEnabled(Settings settings) {
        return XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings);
    }

    /**
     * Create a token based on the provided authentication and metadata.
     * The created token will be stored in the security index.
     */
    public void createUserToken(Authentication authentication, Authentication originatingClientAuth,
                                ActionListener<Tuple<UserToken, String>> listener, Map<String, Object> metadata,
                                boolean includeRefreshToken) throws IOException {
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(traceLog("create token", new IllegalArgumentException("authentication must be provided")));
        } else if (originatingClientAuth == null) {
            listener.onFailure(traceLog("create token",
                new IllegalArgumentException("originating client authentication must be provided")));
        } else {
            final Instant created = clock.instant();
            final Instant expiration = getExpirationTime(created);
            final Version version = clusterService.state().nodes().getMinNodeVersion();
            final Authentication matchingVersionAuth = version.equals(authentication.getVersion()) ? authentication :
                    new Authentication(authentication.getUser(), authentication.getAuthenticatedBy(), authentication.getLookedUpBy(),
                            version);
            final UserToken userToken = new UserToken(version, matchingVersionAuth, expiration, metadata);
            final String refreshToken = includeRefreshToken ? UUIDs.randomBase64UUID() : null;

            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                builder.field("doc_type", TOKEN_DOC_TYPE);
                builder.field("creation_time", created.toEpochMilli());
                if (includeRefreshToken) {
                    builder.startObject("refresh_token")
                        .field("token", refreshToken)
                        .field("invalidated", false)
                        .field("refreshed", false)
                        .startObject("client")
                            .field("type", "unassociated_client")
                            .field("user", originatingClientAuth.getUser().principal())
                            .field("realm", originatingClientAuth.getAuthenticatedBy().getName())
                        .endObject()
                        .endObject();
                }
                builder.startObject("access_token")
                        .field("invalidated", false)
                        .field("user_token", userToken)
                        .field("realm", authentication.getAuthenticatedBy().getName())
                        .endObject();
                builder.endObject();
                final String documentId = getTokenDocumentId(userToken);
                IndexRequest request =
                        client.prepareIndex(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE, documentId)
                                .setOpType(OpType.CREATE)
                                .setSource(builder)
                                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                                .request();
                securityIndex.prepareIndexIfNeededThenExecute(ex -> listener.onFailure(traceLog("prepare security index", documentId, ex)),
                    () -> executeAsyncWithOrigin(client, SECURITY_ORIGIN, IndexAction.INSTANCE, request,
                        ActionListener.wrap(indexResponse -> listener.onResponse(new Tuple<>(userToken, refreshToken)),
                            listener::onFailure))
                );
            }
        }
    }

    /**
     * Looks in the context to see if the request provided a header with a user token and if so the
     * token is validated, which includes authenticated decryption and verification that the token
     * has not been revoked or is expired.
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
                            checkIfTokenIsValid(userToken, listener);
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

    /**
     * Reads the authentication and metadata from the given token.
     * This method does not validate whether the token is expired or not.
     */
    public void getAuthenticationAndMetaData(String token, ActionListener<Tuple<Authentication, Map<String, Object>>> listener)
            throws IOException {
        decodeToken(token, ActionListener.wrap(
                userToken -> {
                    if (userToken == null) {
                        listener.onFailure(new ElasticsearchSecurityException("supplied token is not valid"));
                    } else {
                        listener.onResponse(new Tuple<>(userToken.getAuthentication(), userToken.getMetadata()));
                    }
                },
                listener::onFailure
        ));
    }

    /*
     * Asynchronously decodes the string representation of a {@link UserToken}. The process for
     * this is asynchronous as we may need to compute a key, which can be computationally expensive
     * so this should not block the current thread, which is typically a network thread. A second
     * reason for being asynchronous is that we can restrain the amount of resources consumed by
     * the key computation to a single thread.
     */
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
            in.setVersion(version);
            final BytesKey decodedSalt = new BytesKey(in.readByteArray());
            final BytesKey passphraseHash = new BytesKey(in.readByteArray());
            KeyAndCache keyAndCache = keyCache.get(passphraseHash);
            if (keyAndCache != null) {
                getKeyAsync(decodedSalt, keyAndCache, ActionListener.wrap(decodeKey -> {
                    try {
                        final byte[] iv = in.readByteArray();
                        final Cipher cipher = getDecryptionCipher(iv, decodeKey, version, decodedSalt);
                        decryptTokenId(in, cipher, version, ActionListener.wrap(tokenId -> {
                            if (securityIndex.isAvailable() == false) {
                                logger.warn("failed to get token [{}] since index is not available", tokenId);
                                listener.onResponse(null);
                            } else {
                                securityIndex.checkIndexVersionThenExecute(
                                    ex -> listener.onFailure(traceLog("prepare security index", tokenId, ex)),
                                    () -> {
                                        final GetRequest getRequest = client.prepareGet(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE,
                                            getTokenDocumentId(tokenId)).request();
                                        Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("decode token", tokenId, ex));
                                        executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, getRequest,
                                            ActionListener.<GetResponse>wrap(response -> {
                                                if (response.isExists()) {
                                                    Map<String, Object> accessTokenSource =
                                                        (Map<String, Object>) response.getSource().get("access_token");
                                                    if (accessTokenSource == null) {
                                                        onFailure.accept(new IllegalStateException(
                                                            "token document is missing the access_token field"));
                                                    } else if (accessTokenSource.containsKey("user_token") == false) {
                                                        onFailure.accept(new IllegalStateException(
                                                            "token document is missing the user_token field"));
                                                    } else {
                                                        Map<String, Object> userTokenSource =
                                                            (Map<String, Object>) accessTokenSource.get("user_token");
                                                        listener.onResponse(UserToken.fromSourceMap(userTokenSource));
                                                    }
                                                } else {
                                                    onFailure.accept(
                                                        new IllegalStateException("token document is missing and must be present"));
                                                }
                                            }, e -> {
                                                // if the index or the shard is not there / available we assume that
                                                // the token is not valid
                                                if (isShardNotAvailableException(e)) {
                                                    logger.warn("failed to get token [{}] since index is not available", tokenId);
                                                    listener.onResponse(null);
                                                } else {
                                                    logger.error(new ParameterizedMessage("failed to get token [{}]", tokenId), e);
                                                    listener.onFailure(e);
                                                }
                                            }), client::get);
                                    });
                            }
                        }, listener::onFailure));
                    } catch (GeneralSecurityException e) {
                        // could happen with a token that is not ours
                        logger.warn("invalid token", e);
                        listener.onResponse(null);
                    } finally {
                        in.close();
                    }
                }, e -> {
                    IOUtils.closeWhileHandlingException(in);
                    listener.onFailure(e);
                }));
            } else {
                IOUtils.closeWhileHandlingException(in);
                logger.debug("invalid key {} key: {}", passphraseHash, keyCache.cache.keySet());
                listener.onResponse(null);
            }
        }
    }

    private void getKeyAsync(BytesKey decodedSalt, KeyAndCache keyAndCache, ActionListener<SecretKey> listener) {
        final SecretKey decodeKey = keyAndCache.getKey(decodedSalt);
        if (decodeKey != null) {
            listener.onResponse(decodeKey);
        } else {
            /* As a measure of protected against DOS, we can pass requests requiring a key
             * computation off to a single thread executor. For normal usage, the initial
             * request(s) that require a key computation will be delayed and there will be
             * some additional latency.
             */
            client.threadPool().executor(THREAD_POOL_NAME)
                    .submit(new KeyComputingRunnable(decodedSalt, listener, keyAndCache));
        }
    }

    private static void decryptTokenId(StreamInput in, Cipher cipher, Version version, ActionListener<String> listener) throws IOException {
        try (CipherInputStream cis = new CipherInputStream(in, cipher); StreamInput decryptedInput = new InputStreamStreamInput(cis)) {
            decryptedInput.setVersion(version);
            listener.onResponse(decryptedInput.readString());
        }
    }

    /**
     * This method performs the steps necessary to invalidate a token so that it may no longer be
     * used. The process of invalidation involves performing an update to
     * the token document and setting the <code>invalidated</code> field to <code>true</code>
     */
    public void invalidateAccessToken(String tokenString, ActionListener<TokensInvalidationResult> listener) {
        ensureEnabled();
        if (Strings.isNullOrEmpty(tokenString)) {
            logger.trace("No token-string provided");
            listener.onFailure(new IllegalArgumentException("token must be provided"));
        } else {
            maybeStartTokenRemover();
            try {
                decodeToken(tokenString, ActionListener.wrap(userToken -> {
                    if (userToken == null) {
                        listener.onFailure(traceLog("invalidate token", tokenString, malformedTokenException()));
                    } else {
                        indexInvalidation(Collections.singleton(userToken.getId()), listener, new AtomicInteger(0),
                            "access_token", null);
                    }
                }, listener::onFailure));
            } catch (IOException e) {
                logger.error("received a malformed token as part of a invalidation request", e);
                listener.onFailure(malformedTokenException());
            }
        }
    }

    /**
     * This method performs the steps necessary to invalidate a token so that it may no longer be used.
     *
     * @see #invalidateAccessToken(String, ActionListener)
     */
    public void invalidateAccessToken(UserToken userToken, ActionListener<TokensInvalidationResult> listener) {
        ensureEnabled();
        if (userToken == null) {
            logger.trace("No access token provided");
            listener.onFailure(new IllegalArgumentException("token must be provided"));
        } else {
            maybeStartTokenRemover();
            indexInvalidation(Collections.singleton(userToken.getId()), listener, new AtomicInteger(0), "access_token", null);
        }
    }

    /**
     * This method performs the steps necessary to invalidate a refresh token so that it may no longer be used.
     *
     * @param refreshToken The string representation of the refresh token
     * @param listener  the listener to notify upon completion
     */
    public void invalidateRefreshToken(String refreshToken, ActionListener<TokensInvalidationResult> listener) {
        ensureEnabled();
        if (Strings.isNullOrEmpty(refreshToken)) {
            logger.trace("No refresh token provided");
            listener.onFailure(new IllegalArgumentException("refresh token must be provided"));
        } else {
            maybeStartTokenRemover();
            findTokenFromRefreshToken(refreshToken,
                    ActionListener.wrap(tuple -> {
                        final String docId = getTokenIdFromDocumentId(tuple.v1().getHits().getAt(0).getId());
                        indexInvalidation(Collections.singletonList(docId), listener, tuple.v2(), "refresh_token", null);
                    }, listener::onFailure), new AtomicInteger(0));
        }
    }

    /**
     * Invalidate all access tokens and all refresh tokens of a given {@code realmName} and/or of a given
     * {@code username} so that they may no longer be used
     *
     * @param realmName the realm of which the tokens should be invalidated
     * @param username the username for which the tokens should be invalidated
     * @param listener  the listener to notify upon completion
     */
    public void invalidateActiveTokensForRealmAndUser(@Nullable String realmName, @Nullable String username,
                                                      ActionListener<TokensInvalidationResult> listener) {
        ensureEnabled();
        if (Strings.isNullOrEmpty(realmName) && Strings.isNullOrEmpty(username)) {
            logger.trace("No realm name or username provided");
            listener.onFailure(new IllegalArgumentException("realm name or username must be provided"));
        } else {
            if (Strings.isNullOrEmpty(realmName)) {
                findActiveTokensForUser(username, ActionListener.wrap(tokenTuples -> {
                    if (tokenTuples.isEmpty()) {
                        logger.warn("No tokens to invalidate for realm [{}] and username [{}]", realmName, username);
                        listener.onResponse(TokensInvalidationResult.emptyResult());
                    } else {
                        invalidateAllTokens(tokenTuples.stream().map(t -> t.v1().getId()).collect(Collectors.toList()), listener);
                    }
                }, listener::onFailure));
            } else {
                Predicate filter = null;
                if (Strings.hasText(username)) {
                    filter = isOfUser(username);
                }
                findActiveTokensForRealm(realmName, ActionListener.wrap(tokenTuples -> {
                    if (tokenTuples.isEmpty()) {
                        logger.warn("No tokens to invalidate for realm [{}] and username [{}]", realmName, username);
                        listener.onResponse(TokensInvalidationResult.emptyResult());
                    } else {
                        invalidateAllTokens(tokenTuples.stream().map(t -> t.v1().getId()).collect(Collectors.toList()), listener);
                    }
                }, listener::onFailure), filter);
            }
        }
    }

    /**
     * Invalidates a collection of access_token and refresh_token that were retrieved by
     * {@link TokenService#invalidateActiveTokensForRealmAndUser}
     *
     * @param accessTokenIds The ids of the access tokens which should be invalidated (along with the respective refresh_token)
     * @param listener  the listener to notify upon completion
     */
    private void invalidateAllTokens(Collection<String> accessTokenIds, ActionListener<TokensInvalidationResult> listener) {
        maybeStartTokenRemover();
        // Invalidate the refresh tokens first so that they cannot be used to get new
        // access tokens while we invalidate the access tokens we currently know about
        indexInvalidation(accessTokenIds, ActionListener.wrap(result ->
                indexInvalidation(accessTokenIds, listener, new AtomicInteger(result.getAttemptCount()),
                    "access_token", result),
            listener::onFailure), new AtomicInteger(0), "refresh_token", null);
    }

    /**
     * Performs the actual invalidation of a collection of tokens
     *
     * @param tokenIds        the tokens to invalidate
     * @param listener        the listener to notify upon completion
     * @param attemptCount    the number of attempts to invalidate that have already been tried
     * @param srcPrefix       the prefix to use when constructing the doc to update, either refresh_token or access_token depending on
     *                        what type of tokens should be invalidated
     * @param previousResult  if this not the initial attempt for invalidation, it contains the result of invalidating
     *                        tokens up to the point of the retry. This result is added to the result of the current attempt
     */
    private void indexInvalidation(Collection<String> tokenIds, ActionListener<TokensInvalidationResult> listener,
                                   AtomicInteger attemptCount, String srcPrefix, @Nullable TokensInvalidationResult previousResult) {
        if (tokenIds.isEmpty()) {
            logger.warn("No [{}] tokens provided for invalidation", srcPrefix);
            listener.onFailure(invalidGrantException("No tokens provided for invalidation"));
        } else if (attemptCount.get() > MAX_RETRY_ATTEMPTS) {
            logger.warn("Failed to invalidate [{}] tokens after [{}] attempts", tokenIds.size(),
                attemptCount.get());
            listener.onFailure(invalidGrantException("failed to invalidate tokens"));
        } else {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (String tokenId : tokenIds) {
                UpdateRequest request = client.prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE, getTokenDocumentId(tokenId))
                    .setDoc(srcPrefix, Collections.singletonMap("invalidated", true))
                    .setFetchSource(srcPrefix, null)
                    .request();
                bulkRequestBuilder.add(request);
            }
            bulkRequestBuilder.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
            securityIndex.prepareIndexIfNeededThenExecute(ex -> listener.onFailure(traceLog("prepare security index", ex)),
                () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, bulkRequestBuilder.request(),
                    ActionListener.<BulkResponse>wrap(bulkResponse -> {
                        ArrayList<String> retryTokenDocIds = new ArrayList<>();
                        ArrayList<ElasticsearchException> failedRequestResponses = new ArrayList<>();
                        ArrayList<String> previouslyInvalidated = new ArrayList<>();
                        ArrayList<String> invalidated = new ArrayList<>();
                        if (null != previousResult) {
                            failedRequestResponses.addAll((previousResult.getErrors()));
                            previouslyInvalidated.addAll(previousResult.getPreviouslyInvalidatedTokens());
                            invalidated.addAll(previousResult.getInvalidatedTokens());
                        }
                        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                            if (bulkItemResponse.isFailed()) {
                                Throwable cause = bulkItemResponse.getFailure().getCause();
                                final String failedTokenDocId = getTokenIdFromDocumentId(bulkItemResponse.getFailure().getId());
                                if (isShardNotAvailableException(cause)) {
                                    retryTokenDocIds.add(failedTokenDocId);
                                }
                                else {
                                    traceLog("invalidate access token", failedTokenDocId, cause);
                                    failedRequestResponses.add(new ElasticsearchException("Error invalidating " + srcPrefix + ": ", cause));
                                }
                            } else {
                                UpdateResponse updateResponse = bulkItemResponse.getResponse();
                                if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                                    logger.debug("Invalidated [{}] for doc [{}]", srcPrefix, updateResponse.getGetResult().getId());
                                    invalidated.add(updateResponse.getGetResult().getId());
                                } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                                    previouslyInvalidated.add(updateResponse.getGetResult().getId());
                                }
                            }
                        }
                        if (retryTokenDocIds.isEmpty() == false) {
                            TokensInvalidationResult incompleteResult = new TokensInvalidationResult(invalidated, previouslyInvalidated,
                                failedRequestResponses, attemptCount.get());
                            attemptCount.incrementAndGet();
                            indexInvalidation(retryTokenDocIds, listener, attemptCount, srcPrefix, incompleteResult);
                        }
                        TokensInvalidationResult result = new TokensInvalidationResult(invalidated, previouslyInvalidated,
                            failedRequestResponses, attemptCount.get());
                        listener.onResponse(result);
                    }, e -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        traceLog("invalidate tokens", cause);
                        if (isShardNotAvailableException(cause)) {
                            attemptCount.incrementAndGet();
                            indexInvalidation(tokenIds, listener, attemptCount, srcPrefix, previousResult);
                        } else {
                            listener.onFailure(e);
                        }
                    }), client::bulk));
        }
    }

    /**
     * Uses the refresh token to refresh its associated token and returns the new token with an
     * updated expiration date to the listener
     */
    public void refreshToken(String refreshToken, ActionListener<Tuple<UserToken, String>> listener) {
        ensureEnabled();
        findTokenFromRefreshToken(refreshToken,
            ActionListener.wrap(tuple -> {
                final Authentication userAuth = Authentication.readFromContext(client.threadPool().getThreadContext());
                final String tokenDocId = tuple.v1().getHits().getHits()[0].getId();
                innerRefresh(tokenDocId, userAuth, listener, tuple.v2());
            }, listener::onFailure),
            new AtomicInteger(0));
    }

    private void findTokenFromRefreshToken(String refreshToken, ActionListener<Tuple<SearchResponse, AtomicInteger>> listener,
                                           AtomicInteger attemptCount) {
        if (attemptCount.get() > MAX_RETRY_ATTEMPTS) {
            logger.warn("Failed to find token for refresh token [{}] after [{}] attempts", refreshToken, attemptCount.get());
            listener.onFailure(invalidGrantException("could not refresh the requested token"));
        } else {
            SearchRequest request = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                .setQuery(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", TOKEN_DOC_TYPE))
                    .filter(QueryBuilders.termQuery("refresh_token.token", refreshToken)))
                .setVersion(true)
                .request();

            final SecurityIndexManager frozenSecurityIndex = securityIndex.freeze();
            if (frozenSecurityIndex.indexExists() == false) {
                logger.warn("security index does not exist therefore refresh token [{}] cannot be validated", refreshToken);
                listener.onFailure(invalidGrantException("could not refresh the requested token"));
            } else if (frozenSecurityIndex.isAvailable() == false) {
                logger.debug("security index is not available to find token from refresh token, retrying");
                attemptCount.incrementAndGet();
                findTokenFromRefreshToken(refreshToken, listener, attemptCount);
            } else {
                Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("find by refresh token", refreshToken, ex));
                securityIndex.checkIndexVersionThenExecute(listener::onFailure, () ->
                    executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
                        ActionListener.<SearchResponse>wrap(searchResponse -> {
                            if (searchResponse.isTimedOut()) {
                                attemptCount.incrementAndGet();
                                findTokenFromRefreshToken(refreshToken, listener, attemptCount);
                            } else if (searchResponse.getHits().getHits().length < 1) {
                                logger.info("could not find token document with refresh_token [{}]", refreshToken);
                                onFailure.accept(invalidGrantException("could not refresh the requested token"));
                            } else if (searchResponse.getHits().getHits().length > 1) {
                                onFailure.accept(new IllegalStateException("multiple tokens share the same refresh token"));
                            } else {
                                listener.onResponse(new Tuple<>(searchResponse, attemptCount));
                            }
                        }, e -> {
                            if (isShardNotAvailableException(e)) {
                                logger.debug("failed to search for token document, retrying", e);
                                attemptCount.incrementAndGet();
                                findTokenFromRefreshToken(refreshToken, listener, attemptCount);
                            } else {
                                onFailure.accept(e);
                            }
                        }),
                        client::search));
            }
        }
    }

    /**
     * Performs the actual refresh of the token with retries in case of certain exceptions that
     * may be recoverable. The refresh involves retrieval of the token document and then
     * updating the token document to indicate that the document has been refreshed.
     */
    private void innerRefresh(String tokenDocId, Authentication userAuth, ActionListener<Tuple<UserToken, String>> listener,
                              AtomicInteger attemptCount) {
        if (attemptCount.getAndIncrement() > MAX_RETRY_ATTEMPTS) {
            logger.warn("Failed to refresh token for doc [{}] after [{}] attempts", tokenDocId, attemptCount.get());
            listener.onFailure(invalidGrantException("could not refresh the requested token"));
        } else {
            Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("refresh token", tokenDocId, ex));
            GetRequest getRequest = client.prepareGet(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE, tokenDocId).request();
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, getRequest,
                ActionListener.<GetResponse>wrap(response -> {
                    if (response.isExists()) {
                        final Map<String, Object> source = response.getSource();
                        final Optional<ElasticsearchSecurityException> invalidSource = checkTokenDocForRefresh(source, userAuth);

                        if (invalidSource.isPresent()) {
                            onFailure.accept(invalidSource.get());
                        } else {
                            final Map<String, Object> userTokenSource = (Map<String, Object>)
                                ((Map<String, Object>) source.get("access_token")).get("user_token");
                            final String authString = (String) userTokenSource.get("authentication");
                            final Integer version = (Integer) userTokenSource.get("version");
                            final Map<String, Object> metadata = (Map<String, Object>) userTokenSource.get("metadata");

                            Version authVersion = Version.fromId(version);
                            try (StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(authString))) {
                                in.setVersion(authVersion);
                                Authentication authentication = new Authentication(in);
                                UpdateRequest updateRequest =
                                    client.prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE, tokenDocId)
                                        .setVersion(response.getVersion())
                                        .setDoc("refresh_token", Collections.singletonMap("refreshed", true))
                                        .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                                        .request();
                                executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, updateRequest,
                                    ActionListener.<UpdateResponse>wrap(
                                        updateResponse -> createUserToken(authentication, userAuth, listener, metadata, true),
                                        e -> {
                                            Throwable cause = ExceptionsHelper.unwrapCause(e);
                                            if (cause instanceof VersionConflictEngineException ||
                                                isShardNotAvailableException(e)) {
                                                innerRefresh(tokenDocId, userAuth,
                                                    listener, attemptCount);
                                            } else {
                                                onFailure.accept(e);
                                            }
                                        }),
                                    client::update);
                            }
                        }
                    } else {
                        logger.info("could not find token document [{}] for refresh", tokenDocId);
                        onFailure.accept(invalidGrantException("could not refresh the requested token"));
                    }
                }, e -> {
                    if (isShardNotAvailableException(e)) {
                        innerRefresh(tokenDocId, userAuth, listener, attemptCount);
                    } else {
                        listener.onFailure(e);
                    }
                }), client::get);
        }
    }

    /**
     * Performs checks on the retrieved source and returns an {@link Optional} with the exception
     * if there is an issue
     */
    private Optional<ElasticsearchSecurityException> checkTokenDocForRefresh(Map<String, Object> source, Authentication userAuth) {
        final Map<String, Object> refreshTokenSrc = (Map<String, Object>) source.get("refresh_token");
        final Map<String, Object> accessTokenSrc = (Map<String, Object>) source.get("access_token");
        if (refreshTokenSrc == null || refreshTokenSrc.isEmpty()) {
            return Optional.of(invalidGrantException("token document is missing the refresh_token object"));
        } else if (accessTokenSrc == null || accessTokenSrc.isEmpty()) {
            return Optional.of(invalidGrantException("token document is missing the access_token object"));
        } else {
            final Boolean refreshed = (Boolean) refreshTokenSrc.get("refreshed");
            final Boolean invalidated = (Boolean) refreshTokenSrc.get("invalidated");
            final Long creationEpochMilli = (Long) source.get("creation_time");
            final Instant creationTime = creationEpochMilli == null ? null : Instant.ofEpochMilli(creationEpochMilli);
            final Map<String, Object> userTokenSrc = (Map<String, Object>) accessTokenSrc.get("user_token");
            if (refreshed == null) {
                return Optional.of(invalidGrantException("token document is missing refreshed value"));
            } else if (invalidated == null) {
                return Optional.of(invalidGrantException("token document is missing invalidated value"));
            } else if (creationEpochMilli == null) {
                return Optional.of(invalidGrantException("token document is missing creation time value"));
            } else if (refreshed) {
                return Optional.of(invalidGrantException("token has already been refreshed"));
            } else if (invalidated) {
                return Optional.of(invalidGrantException("token has been invalidated"));
            } else if (clock.instant().isAfter(creationTime.plus(24L, ChronoUnit.HOURS))) {
                return Optional.of(invalidGrantException("refresh token is expired"));
            } else if (userTokenSrc == null || userTokenSrc.isEmpty()) {
                return Optional.of(invalidGrantException("token document is missing the user token info"));
            } else if (userTokenSrc.get("authentication") == null) {
                return Optional.of(invalidGrantException("token is missing authentication info"));
            } else if (userTokenSrc.get("version") == null) {
                return Optional.of(invalidGrantException("token is missing version value"));
            } else if (userTokenSrc.get("metadata") == null) {
                return Optional.of(invalidGrantException("token is missing metadata"));
            } else {
                return checkClient(refreshTokenSrc, userAuth);
            }
        }
    }

    private Optional<ElasticsearchSecurityException> checkClient(Map<String, Object> refreshTokenSource, Authentication userAuth) {
        Map<String, Object> clientInfo = (Map<String, Object>) refreshTokenSource.get("client");
        if (clientInfo == null) {
            return Optional.of(invalidGrantException("token is missing client information"));
        } else if (userAuth.getUser().principal().equals(clientInfo.get("user")) == false) {
            return Optional.of(invalidGrantException("tokens must be refreshed by the creating client"));
        } else if (userAuth.getAuthenticatedBy().getName().equals(clientInfo.get("realm")) == false) {
            return Optional.of(invalidGrantException("tokens must be refreshed by the creating client"));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Find stored refresh and access tokens that have not been invalidated or expired, and were issued against
     *  the specified realm.
     *
     * @param realmName The name of the realm for which to get the tokens
     * @param listener The listener to notify upon completion
     * @param filter an optional Predicate to test the source of the found documents against
     */
    public void findActiveTokensForRealm(String realmName, ActionListener<Collection<Tuple<UserToken, String>>> listener,
                                         @Nullable Predicate<Map<String, Object>> filter) {
        ensureEnabled();
        final SecurityIndexManager frozenSecurityIndex = securityIndex.freeze();
        if (Strings.isNullOrEmpty(realmName)) {
            listener.onFailure(new IllegalArgumentException("Realm name is required"));
        } else if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            final Instant now = clock.instant();
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("doc_type", TOKEN_DOC_TYPE))
                .filter(QueryBuilders.termQuery("access_token.realm", realmName))
                .filter(QueryBuilders.boolQuery()
                    .should(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("access_token.invalidated", false))
                        .must(QueryBuilders.rangeQuery("access_token.user_token.expiration_time").gte(now.toEpochMilli()))
                    )
                    .should(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("refresh_token.invalidated", false))
                        .must(QueryBuilders.rangeQuery("creation_time").gte(now.toEpochMilli() - TimeValue.timeValueHours(24).millis()))
                    )
                );

            final SearchRequest request = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                .setQuery(boolQuery)
                .setVersion(false)
                .setSize(1000)
                .setFetchSource(true)
                .request();
            securityIndex.checkIndexVersionThenExecute(listener::onFailure,
                () -> ScrollHelper.fetchAllByEntity(client, request, listener, (SearchHit hit) -> filterAndParseHit(hit, filter)));
        }
    }

    /**
     * Find stored refresh and access tokens that have not been invalidated or expired, and were issued for
     * the specified user.
     *
     * @param username The user for which to get the tokens
     * @param listener The listener to notify upon completion
     */
    public void findActiveTokensForUser(String username, ActionListener<Collection<Tuple<UserToken, String>>> listener) {
        ensureEnabled();

        final SecurityIndexManager frozenSecurityIndex = securityIndex.freeze();
        if (Strings.isNullOrEmpty(username)) {
            listener.onFailure(new IllegalArgumentException("username is required"));
        } else if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            final Instant now = clock.instant();
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("doc_type", TOKEN_DOC_TYPE))
                .filter(QueryBuilders.boolQuery()
                    .should(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("access_token.invalidated", false))
                        .must(QueryBuilders.rangeQuery("access_token.user_token.expiration_time").gte(now.toEpochMilli()))
                    )
                    .should(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("refresh_token.invalidated", false))
                        .must(QueryBuilders.rangeQuery("creation_time").gte(now.toEpochMilli() - TimeValue.timeValueHours(24).millis()))
                    )
                );

            final SearchRequest request = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                .setQuery(boolQuery)
                .setVersion(false)
                .setSize(1000)
                .setFetchSource(true)
                .request();
            securityIndex.checkIndexVersionThenExecute(listener::onFailure,
                () -> ScrollHelper.fetchAllByEntity(client, request, listener,
                    (SearchHit hit) -> filterAndParseHit(hit, isOfUser(username))));
        }
    }

    private static Predicate<Map<String, Object>> isOfUser(String username) {
        return source -> {
            String auth = (String) source.get("authentication");
            Integer version = (Integer) source.get("version");
            Version authVersion = Version.fromId(version);
            try (StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(auth))) {
                in.setVersion(authVersion);
                Authentication authentication = new Authentication(in);
                return authentication.getUser().principal().equals(username);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }


    private Tuple<UserToken, String> filterAndParseHit(SearchHit hit, @Nullable Predicate<Map<String, Object>> filter) {
        final Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) {
            throw new IllegalStateException("token document did not have source but source should have been fetched");
        }
        try {
            return parseTokensFromDocument(source, filter);
        } catch (IOException e) {
            throw invalidGrantException("cannot read token from document");
        }
    }

    /**
     *
     * Parses a token document into a Tuple of a {@link UserToken} and a String representing the corresponding refresh_token
     *
     * @param source The token document source as retrieved
     * @param filter an optional Predicate to test the source of the UserToken against
     * @return A {@link Tuple} of access-token and refresh-token-id or null if a Predicate is defined and the userToken source doesn't
     *         satisfy it
     */
    private Tuple<UserToken, String> parseTokensFromDocument(Map<String, Object> source, @Nullable Predicate<Map<String, Object>> filter)
        throws IOException {

        final String refreshToken = (String) ((Map<String, Object>) source.get("refresh_token")).get("token");
        final Map<String, Object> userTokenSource = (Map<String, Object>)
            ((Map<String, Object>) source.get("access_token")).get("user_token");
        if (null != filter && filter.test(userTokenSource) == false) {
            return null;
        }
        final String id = (String) userTokenSource.get("id");
        final Integer version = (Integer) userTokenSource.get("version");
        final String authString = (String) userTokenSource.get("authentication");
        final Long expiration = (Long) userTokenSource.get("expiration_time");
        final Map<String, Object> metadata = (Map<String, Object>) userTokenSource.get("metadata");

        Version authVersion = Version.fromId(version);
        try (StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(authString))) {
            in.setVersion(authVersion);
            Authentication authentication = new Authentication(in);
            return new Tuple<>(new UserToken(id, Version.fromId(version), authentication, Instant.ofEpochMilli(expiration), metadata),
                    refreshToken);
        }
    }

    private static String getTokenDocumentId(UserToken userToken) {
        return getTokenDocumentId(userToken.getId());
    }

    private static String getTokenDocumentId(String id) {
        return TOKEN_DOC_ID_PREFIX + id;
    }

    private static String getTokenIdFromDocumentId(String docId) {
        if (docId.startsWith(TOKEN_DOC_ID_PREFIX) == false) {
            throw new IllegalStateException("TokenDocument ID [" + docId + "] has unexpected value");
        } else {
            return docId.substring(TOKEN_DOC_ID_PREFIX.length());
        }
    }

    private void ensureEnabled() {
        if (enabled == false) {
            throw new IllegalStateException("tokens are not enabled");
        }
    }

    /**
     * Checks if the access token has been explicitly invalidated
     */
    private void checkIfTokenIsValid(UserToken userToken, ActionListener<UserToken> listener) {
        Instant currentTime = clock.instant();
        if (currentTime.isAfter(userToken.getExpirationTime())) {
            listener.onFailure(traceLog("validate token", userToken.getId(), expiredTokenException()));
        } else if (securityIndex.indexExists() == false) {
            // index doesn't exist so the token is considered invalid as we cannot verify its validity
            logger.warn("failed to validate token [{}] since the security index doesn't exist", userToken.getId());
            listener.onResponse(null);
        } else {
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final GetRequest getRequest = client.prepareGet(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE,
                    getTokenDocumentId(userToken)).request();
                Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("check token state", userToken.getId(), ex));
                executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, getRequest,
                    ActionListener.<GetResponse>wrap(response -> {
                        if (response.isExists()) {
                            Map<String, Object> source = response.getSource();
                            Map<String, Object> accessTokenSource = (Map<String, Object>) source.get("access_token");
                            if (accessTokenSource == null) {
                                onFailure.accept(new IllegalStateException("token document is missing access_token field"));
                            } else {
                                Boolean invalidated = (Boolean) accessTokenSource.get("invalidated");
                                if (invalidated == null) {
                                    onFailure.accept(new IllegalStateException("token document is missing invalidated field"));
                                } else if (invalidated) {
                                    onFailure.accept(expiredTokenException());
                                } else {
                                    listener.onResponse(userToken);
                                }
                            }
                        } else {
                            onFailure.accept(new IllegalStateException("token document is missing and must be present"));
                        }
                    }, e -> {
                        // if the index or the shard is not there / available we assume that
                        // the token is not valid
                        if (isShardNotAvailableException(e)) {
                            logger.warn("failed to get token [{}] since index is not available", userToken.getId());
                            listener.onResponse(null);
                        } else {
                            logger.error(new ParameterizedMessage("failed to get token [{}]", userToken.getId()), e);
                            listener.onFailure(e);
                        }
                    }), client::get);
            });
        }
    }


    public TimeValue getExpirationDelay() {
        return expirationDelay;
    }

    private Instant getExpirationTime(Instant now) {
        return now.plusSeconds(expirationDelay.getSeconds());
    }

    private void maybeStartTokenRemover() {
        if (securityIndex.isAvailable()) {
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
    private String getFromHeader(ThreadContext threadContext) {
        String header = threadContext.getHeader("Authorization");
        if (Strings.hasText(header) && header.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())
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
            out.setVersion(userToken.getVersion());
            KeyAndCache keyAndCache = keyCache.activeKeyCache;
            Version.writeVersion(userToken.getVersion(), out);
            out.writeByteArray(keyAndCache.getSalt().bytes);
            out.writeByteArray(keyAndCache.getKeyHash().bytes);
            final byte[] initializationVector = getNewInitializationVector();
            out.writeByteArray(initializationVector);
            try (CipherOutputStream encryptedOutput =
                         new CipherOutputStream(out, getEncryptionCipher(initializationVector, keyAndCache, userToken.getVersion()));
                 StreamOutput encryptedStreamOutput = new OutputStreamStreamOutput(encryptedOutput)) {
                encryptedStreamOutput.setVersion(userToken.getVersion());
                encryptedStreamOutput.writeString(userToken.getId());
                encryptedStreamOutput.close();
                return new String(os.toByteArray(), StandardCharsets.UTF_8);
            }
        }
    }

    private void ensureEncryptionCiphersSupported() throws NoSuchPaddingException, NoSuchAlgorithmException {
        Cipher.getInstance(ENCRYPTION_CIPHER);
        SecretKeyFactory.getInstance(KDF_ALGORITHM);
    }

    private Cipher getEncryptionCipher(byte[] iv, KeyAndCache keyAndCache, Version version) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        BytesKey salt = keyAndCache.getSalt();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, keyAndCache.getOrComputeKey(salt), new GCMParameterSpec(128, iv), secureRandom);
        } catch (ExecutionException e) {
            throw new ElasticsearchSecurityException("Failed to compute secret key for active salt", e);
        }
        cipher.updateAAD(ByteBuffer.allocate(4).putInt(version.id).array());
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
     * is up to the client to re-authenticate and obtain a new token. The format for this response
     * is defined in <a href="https://tools.ietf.org/html/rfc6750#section-3.1"></a>
     */
    private static ElasticsearchSecurityException expiredTokenException() {
        ElasticsearchSecurityException e =
                new ElasticsearchSecurityException("token expired", RestStatus.UNAUTHORIZED);
        e.addHeader("WWW-Authenticate", EXPIRED_TOKEN_WWW_AUTH_VALUE);
        return e;
    }

    /**
     * Creates an {@link ElasticsearchSecurityException} that indicates the token was malformed. It
     * is up to the client to re-authenticate and obtain a new token. The format for this response
     * is defined in <a href="https://tools.ietf.org/html/rfc6750#section-3.1"></a>
     */
    private static ElasticsearchSecurityException malformedTokenException() {
        ElasticsearchSecurityException e =
                new ElasticsearchSecurityException("token malformed", RestStatus.UNAUTHORIZED);
        e.addHeader("WWW-Authenticate", MALFORMED_TOKEN_WWW_AUTH_VALUE);
        return e;
    }

    /**
     * Creates an {@link ElasticsearchSecurityException} that indicates the request contained an invalid grant
     */
    private static ElasticsearchSecurityException invalidGrantException(String detail) {
        ElasticsearchSecurityException e =
            new ElasticsearchSecurityException("invalid_grant", RestStatus.BAD_REQUEST);
        e.addHeader("error_description", detail);
        return e;
    }

    /**
     * Logs an exception concerning a specific Token at TRACE level (if enabled)
     */
    private <E extends Throwable> E traceLog(String action, String identifier, E exception) {
        if (logger.isTraceEnabled()) {
            if (exception instanceof ElasticsearchException) {
                final ElasticsearchException esEx = (ElasticsearchException) exception;
                final Object detail = esEx.getHeader("error_description");
                if (detail != null) {
                    logger.trace(() -> new ParameterizedMessage("Failure in [{}] for id [{}] - [{}]", action, identifier, detail),
                        esEx);
                } else {
                    logger.trace(() -> new ParameterizedMessage("Failure in [{}] for id [{}]", action, identifier),
                        esEx);
                }
            } else {
                logger.trace(() -> new ParameterizedMessage("Failure in [{}] for id [{}]", action, identifier), exception);
            }
        }
        return exception;
    }

    /**
     * Logs an exception at TRACE level (if enabled)
     */
    private <E extends Throwable> E traceLog(String action, E exception) {
        if (logger.isTraceEnabled()) {
            if (exception instanceof ElasticsearchException) {
                final ElasticsearchException esEx = (ElasticsearchException) exception;
                final Object detail = esEx.getHeader("error_description");
                if (detail != null) {
                    logger.trace(() -> new ParameterizedMessage("Failure in [{}] - [{}]", action, detail), esEx);
                } else {
                    logger.trace(() -> new ParameterizedMessage("Failure in [{}]", action), esEx);
                }
            } else {
                logger.trace(() -> new ParameterizedMessage("Failure in [{}]", action), exception);
            }
        }
        return exception;
    }

    boolean isExpiredTokenException(ElasticsearchSecurityException e) {
        final List<String> headers = e.getHeader("WWW-Authenticate");
        return headers != null && headers.stream().anyMatch(EXPIRED_TOKEN_WWW_AUTH_VALUE::equals);
    }

    boolean isExpirationInProgress() {
        return expiredTokenRemover.isExpirationInProgress();
    }

    private class KeyComputingRunnable extends AbstractRunnable {

        private final BytesKey decodedSalt;
        private final ActionListener<SecretKey> listener;
        private final KeyAndCache keyAndCache;

        KeyComputingRunnable(BytesKey decodedSalt, ActionListener<SecretKey> listener, KeyAndCache keyAndCache) {
            this.decodedSalt = decodedSalt;
            this.listener = listener;
            this.keyAndCache = keyAndCache;
        }

        @Override
        protected void doRun() {
            try {
                final SecretKey computedKey = keyAndCache.getOrComputeKey(decodedSalt);
                listener.onResponse(computedKey);
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
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Creates a new key unless present that is newer than the current active key and returns the corresponding metadata. Note:
     * this method doesn't modify the metadata used in this token service. See {@link #refreshMetaData(TokenMetaData)}
     */
    synchronized TokenMetaData generateSpareKey() {
        KeyAndCache maxKey = keyCache.cache.values().stream().max(Comparator.comparingLong(v -> v.keyAndTimestamp.getTimestamp())).get();
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
        KeyAndCache maxKey = keyCache.cache.values().stream().max(Comparator.comparingLong(v -> v.keyAndTimestamp.getTimestamp())).get();
        if (maxKey == keyCache.activeKeyCache) {
            throw new IllegalStateException("call generateSpareKey first");
        }
        return newTokenMetaData(maxKey.getKeyHash(), keyCache.cache.values());
    }

    /**
     * Prunes the keys and keeps up to the latest N keys around
     *
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
                (left, right) ->  Long.compare(right.keyAndTimestamp.getTimestamp(), left.keyAndTimestamp.getTimestamp()));
        for (KeyAndCache value : entries) {
            if (map.size() < numKeysToKeep || value.keyAndTimestamp.getTimestamp() >= currentKey
                    .keyAndTimestamp.getTimestamp()) {
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
        BytesKey currentUsedKeyHash = new BytesKey(metaData.getCurrentKeyHash());
        byte[] saltArr = new byte[SALT_BYTES];
        Map<BytesKey, KeyAndCache> map = new HashMap<>(metaData.getKeys().size());
        long maxTimestamp = createdTimeStamps.get();
        for (KeyAndTimestamp key : metaData.getKeys()) {
            secureRandom.nextBytes(saltArr);
            KeyAndCache keyAndCache = new KeyAndCache(key, new BytesKey(saltArr));
            maxTimestamp = Math.max(keyAndCache.keyAndTimestamp.getTimestamp(), maxTimestamp);
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
            XPackPlugin.checkReadyForXPackCustomMetadata(currentState);

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

            if (state.nodes().isLocalNodeElectedMaster()) {
                if (XPackPlugin.isReadyForXPackCustomMetadata(state)) {
                    installTokenMetadata(state.metaData());
                } else {
                    logger.debug("cannot add token metadata to cluster as the following nodes might not understand the metadata: {}",
                        () -> XPackPlugin.nodesNotReadyForXPackCustomMetadata(state));
                }
            }

            TokenMetaData custom = event.state().custom(TokenMetaData.TYPE);
            if (custom != null && custom.equals(getTokenMetaData()) == false) {
                logger.info("refresh keys");
                try {
                    refreshMetaData(custom);
                } catch (Exception e) {
                    logger.warn("refreshing metadata failed", e);
                }
                logger.info("refreshed keys");
            }
        });
    }

    // to prevent too many cluster state update tasks to be queued for doing the same update
    private final AtomicBoolean installTokenMetadataInProgress = new AtomicBoolean(false);

    private void installTokenMetadata(MetaData metaData) {
        if (metaData.custom(TokenMetaData.TYPE) == null) {
            if (installTokenMetadataInProgress.compareAndSet(false, true)) {
                clusterService.submitStateUpdateTask("install-token-metadata", new ClusterStateUpdateTask(Priority.URGENT) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);

                        if (currentState.custom(TokenMetaData.TYPE) == null) {
                            return ClusterState.builder(currentState).putCustom(TokenMetaData.TYPE, getTokenMetaData()).build();
                        } else {
                            return currentState;
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        installTokenMetadataInProgress.set(false);
                        logger.error("unable to install token metadata", e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        installTokenMetadataInProgress.set(false);
                    }
                });
            }
        }
    }

    /**
     * For testing
     */
    void clearActiveKeyCache() {
        this.keyCache.activeKeyCache.keyCache.invalidateAll();
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
                SecretKey secretKey = computeSecretKey(keyAndTimestamp.getKey().getChars(), salt.bytes);
                keyCache.put(salt, secretKey);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            this.salt = salt;
            this.keyHash = calculateKeyHash(keyAndTimestamp.getKey());
        }

        private SecretKey getKey(BytesKey salt) {
            return keyCache.get(salt);
        }

        public SecretKey getOrComputeKey(BytesKey decodedSalt) throws ExecutionException {
            return keyCache.computeIfAbsent(decodedSalt, (salt) -> {
                try (SecureString closeableChars = keyAndTimestamp.getKey().clone()) {
                    return computeSecretKey(closeableChars.getChars(), salt.bytes);
                }
            });
        }

        @Override
        public void close() throws IOException {
            keyAndTimestamp.getKey().close();
        }

        BytesKey getKeyHash() {
            return keyHash;
        }

        private static BytesKey calculateKeyHash(SecureString key) {
            MessageDigest messageDigest = MessageDigests.sha256();
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
