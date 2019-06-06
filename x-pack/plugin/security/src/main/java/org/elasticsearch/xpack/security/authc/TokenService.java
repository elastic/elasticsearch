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
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.KeyAndTimestamp;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
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
import java.time.DateTimeException;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

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
    static final int TOKEN_SERVICE_KEY_ITERATIONS = 100000;
    static final int TOKENS_ENCRYPTION_KEY_ITERATIONS = 1024;
    private static final String KDF_ALGORITHM = "PBKDF2withHMACSHA512";
    static final int SALT_BYTES = 32;
    private static final int KEY_BYTES = 64;
    static final int IV_BYTES = 12;
    private static final int VERSION_BYTES = 4;
    private static final String ENCRYPTION_CIPHER = "AES/GCM/NoPadding";
    private static final String EXPIRED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XPackField.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token expired\"";
    private static final String MALFORMED_TOKEN_WWW_AUTH_VALUE = "Bearer realm=\"" + XPackField.SECURITY +
            "\", error=\"invalid_token\", error_description=\"The access token is malformed\"";
    private static final BackoffPolicy DEFAULT_BACKOFF = BackoffPolicy.exponentialBackoff();

    public static final String THREAD_POOL_NAME = XPackField.SECURITY + "-token-key";
    public static final Setting<TimeValue> TOKEN_EXPIRATION = Setting.timeSetting("xpack.security.authc.token.timeout",
            TimeValue.timeValueMinutes(20L), TimeValue.timeValueSeconds(1L), TimeValue.timeValueHours(1L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting("xpack.security.authc.token.delete.interval",
            TimeValue.timeValueMinutes(30L), Property.NodeScope);
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting("xpack.security.authc.token.delete.timeout",
            TimeValue.MINUS_ONE, Property.NodeScope);

    static final String TOKEN_DOC_TYPE = "token";
    private static final int HASHED_TOKEN_LENGTH = 43;
    // UUIDs are 16 bytes encoded base64 without padding, therefore the length is (16 / 3) * 4 + ((16 % 3) * 8 + 5) / 6 chars
    private static final int TOKEN_LENGTH = 22;
    private static final String TOKEN_DOC_ID_PREFIX = TOKEN_DOC_TYPE + "_";
    static final int LEGACY_MINIMUM_BYTES = VERSION_BYTES + SALT_BYTES + IV_BYTES + 1;
    static final int MINIMUM_BYTES = VERSION_BYTES + TOKEN_LENGTH + 1;
    static final int LEGACY_MINIMUM_BASE64_BYTES = Double.valueOf(Math.ceil((4 * LEGACY_MINIMUM_BYTES) / 3)).intValue();
    static final int MINIMUM_BASE64_BYTES = Double.valueOf(Math.ceil((4 * MINIMUM_BYTES) / 3)).intValue();
    static final Version VERSION_HASHED_TOKENS = Version.V_7_2_0;
    static final Version VERSION_TOKENS_INDEX_INTRODUCED = Version.V_7_2_0;
    static final Version VERSION_ACCESS_TOKENS_AS_UUIDS = Version.V_7_2_0;
    static final Version VERSION_MULTIPLE_CONCURRENT_REFRESHES = Version.V_7_2_0;
    private static final Logger logger = LogManager.getLogger(TokenService.class);

    private final SecureRandom secureRandom = new SecureRandom();
    private final Settings settings;
    private final ClusterService clusterService;
    private final Clock clock;
    private final TimeValue expirationDelay;
    private final TimeValue deleteInterval;
    private final Client client;
    private final SecurityIndexManager securityMainIndex;
    private final SecurityIndexManager securityTokensIndex;
    private final ExpiredTokenRemover expiredTokenRemover;
    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private volatile TokenKeys keyCache;
    private volatile long lastExpirationRunMs;
    private final AtomicLong createdTimeStamps = new AtomicLong(-1);

    /**
     * Creates a new token service
     */
    public TokenService(Settings settings, Clock clock, Client client, XPackLicenseState licenseState,
                        SecurityIndexManager securityMainIndex, SecurityIndexManager securityTokensIndex,
                        ClusterService clusterService) throws GeneralSecurityException {
        byte[] saltArr = new byte[SALT_BYTES];
        secureRandom.nextBytes(saltArr);
        final SecureString tokenPassphrase = generateTokenKey();
        this.settings = settings;
        this.clock = clock.withZone(ZoneOffset.UTC);
        this.expirationDelay = TOKEN_EXPIRATION.get(settings);
        this.client = client;
        this.licenseState = licenseState;
        this.securityMainIndex = securityMainIndex;
        this.securityTokensIndex = securityTokensIndex;
        this.lastExpirationRunMs = client.threadPool().relativeTimeInMillis();
        this.deleteInterval = DELETE_INTERVAL.get(settings);
        this.enabled = isTokenServiceEnabled(settings);
        this.expiredTokenRemover = new ExpiredTokenRemover(settings, client, this.securityMainIndex, securityTokensIndex);
        ensureEncryptionCiphersSupported();
        KeyAndCache keyAndCache = new KeyAndCache(new KeyAndTimestamp(tokenPassphrase, createdTimeStamps.incrementAndGet()),
                new BytesKey(saltArr));
        keyCache = new TokenKeys(Collections.singletonMap(keyAndCache.getKeyHash(), keyAndCache), keyAndCache.getKeyHash());
        this.clusterService = clusterService;
        initialize(clusterService);
        getTokenMetaData();
    }

    /**
     * Creates an access token and optionally a refresh token as well, based on the provided authentication and metadata with
     * auto-generated values. The created tokens are stored in the security index for versions up to
     * {@link #VERSION_TOKENS_INDEX_INTRODUCED} and to a specific security tokens index for later versions.
     */
    public void createOAuth2Tokens(Authentication authentication, Authentication originatingClientAuth, Map<String, Object> metadata,
                                   boolean includeRefreshToken, ActionListener<Tuple<String, String>> listener) {
        // the created token is compatible with the oldest node version in the cluster
        final Version tokenVersion = getTokenVersionCompatibility();
        // tokens moved to a separate index in newer versions
        final SecurityIndexManager tokensIndex = getTokensIndexForVersion(tokenVersion);
        // the id of the created tokens ought be unguessable
        final String accessToken = UUIDs.randomBase64UUID();
        final String refreshToken = includeRefreshToken ? UUIDs.randomBase64UUID() : null;
        createOAuth2Tokens(accessToken, refreshToken, tokenVersion, tokensIndex, authentication, originatingClientAuth, metadata, listener);
    }

    /**
     * Creates an access token and optionally a refresh token as well from predefined values, based on the provided authentication and
     * metadata. The created tokens are stored in the security index for versions up to {@link #VERSION_TOKENS_INDEX_INTRODUCED} and to a
     * specific security tokens index for later versions.
     */
    //public for testing
    public void createOAuth2Tokens(String accessToken, String refreshToken, Authentication authentication,
                                   Authentication originatingClientAuth,
                            Map<String, Object> metadata, ActionListener<Tuple<String, String>> listener) {
        // the created token is compatible with the oldest node version in the cluster
        final Version tokenVersion = getTokenVersionCompatibility();
        // tokens moved to a separate index in newer versions
        final SecurityIndexManager tokensIndex = getTokensIndexForVersion(tokenVersion);
        createOAuth2Tokens(accessToken, refreshToken, tokenVersion, tokensIndex, authentication, originatingClientAuth, metadata, listener);
    }

    /**
     * Create an access token and optionally a refresh token as well from predefined values, based on the provided authentication and
     * metadata.
     *
     * @param accessToken The predefined seed value for the access token. This will then be
     *                    <ul>
     *                      <li> Encrypted before stored for versions before {@link #VERSION_TOKENS_INDEX_INTRODUCED} </li>
     *                      <li> Hashed before stored for versions after {@link #VERSION_TOKENS_INDEX_INTRODUCED} </li>
     *                      <li> Stored in the security index for versions up to {@link #VERSION_TOKENS_INDEX_INTRODUCED}</li>
     *                      <li> Stored in a specific security tokens index for versions after {@link #VERSION_TOKENS_INDEX_INTRODUCED}</li>
     *                      <li> Prepended with a version ID and encoded with Base64 before returned to the caller of the APIs</li>
     *                    </ul>
     * @param refreshToken The predefined seed value for the access token. This will then be
     *                    <ul>
     *                      <li> Hashed before stored for versions after {@link #VERSION_TOKENS_INDEX_INTRODUCED} </li>
     *                      <li> Stored in the security index for versions up to {@link #VERSION_TOKENS_INDEX_INTRODUCED}</li>
     *                      <li> Stored in a specific security tokens index for versions after {@link #VERSION_TOKENS_INDEX_INTRODUCED}</li>
     *                      <li> Prepended with a version ID and encoded with Base64 before returned to the caller of the APIs for
     *                      versions after {@link #VERSION_TOKENS_INDEX_INTRODUCED}</li>
     *                    </ul>
     * @param tokenVersion The version of the nodes with which these tokens will be compatible.
     * @param tokensIndex The security tokens index
     * @param authentication The authentication object representing the user for which the tokens are created
     * @param originatingClientAuth The authentication object representing the client that called the related API
     * @param metadata A map with metadata to be stored in the token document
     * @param listener The listener to call upon completion with a {@link Tuple} containing the
     *                 serialized access token and serialized refresh token as these will be returned to the client
     */
    private void createOAuth2Tokens(String accessToken, String refreshToken, Version tokenVersion, SecurityIndexManager tokensIndex,
                                    Authentication authentication, Authentication originatingClientAuth, Map<String, Object> metadata,
                                    ActionListener<Tuple<String, String>> listener) {
        assert accessToken.length() == TOKEN_LENGTH : "We assume token ids have a fixed length for nodes of a certain version."
            + " When changing the token length, be careful that the inferences about its length still hold.";
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(traceLog("create token", new IllegalArgumentException("authentication must be provided")));
        } else if (originatingClientAuth == null) {
            listener.onFailure(traceLog("create token",
                new IllegalArgumentException("originating client authentication must be provided")));
        } else {
            final Authentication tokenAuth = new Authentication(authentication.getUser(), authentication.getAuthenticatedBy(),
                authentication.getLookedUpBy(), tokenVersion, AuthenticationType.TOKEN, authentication.getMetadata());
            final String storedAccessToken;
            final String storedRefreshToken;
            if (tokenVersion.onOrAfter(VERSION_HASHED_TOKENS)) {
                storedAccessToken = hashTokenString(accessToken);
                storedRefreshToken = (null == refreshToken) ? null : hashTokenString(refreshToken);
            } else {
                storedAccessToken = accessToken;
                storedRefreshToken = refreshToken;
            }
            final UserToken userToken = new UserToken(storedAccessToken, tokenVersion, tokenAuth, getExpirationTime(), metadata);
            final BytesReference tokenDocument = createTokenDocument(userToken, storedRefreshToken, originatingClientAuth);
            final String documentId = getTokenDocumentId(storedAccessToken);

            final IndexRequest indexTokenRequest = client.prepareIndex(tokensIndex.aliasName(), SINGLE_MAPPING_NAME, documentId)
                    .setOpType(OpType.CREATE)
                    .setSource(tokenDocument, XContentType.JSON)
                    .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                    .request();
            tokensIndex.prepareIndexIfNeededThenExecute(
                    ex -> listener.onFailure(traceLog("prepare tokens index [" + tokensIndex.aliasName() + "]", documentId, ex)),
                    () -> executeAsyncWithOrigin(client, SECURITY_ORIGIN, IndexAction.INSTANCE, indexTokenRequest,
                            ActionListener.wrap(indexResponse -> {
                                if (indexResponse.getResult() == Result.CREATED) {
                                    final String versionedAccessToken = prependVersionAndEncodeAccessToken(tokenVersion, accessToken);
                                    if (tokenVersion.onOrAfter(VERSION_TOKENS_INDEX_INTRODUCED)) {
                                        final String versionedRefreshToken = refreshToken != null
                                            ? prependVersionAndEncodeRefreshToken(tokenVersion, refreshToken)
                                            : null;
                                        listener.onResponse(new Tuple<>(versionedAccessToken, versionedRefreshToken));
                                    } else {
                                        // prior versions of the refresh token are not version-prepended, as nodes on those
                                        // versions don't expect it.
                                        // Such nodes might exist in a mixed cluster during a rolling upgrade.
                                        listener.onResponse(new Tuple<>(versionedAccessToken, refreshToken));
                                    }
                                } else {
                                    listener.onFailure(traceLog("create token",
                                            new ElasticsearchException("failed to create token document [{}]", indexResponse)));
                                }
                            }, listener::onFailure)));
        }
    }

    /**
     * Hashes an access or refresh token String so that it can safely be persisted in the index. We don't salt
     * the values as these are v4 UUIDs that have enough entropy by themselves.
     */
    // public for testing
    public static String hashTokenString(String accessTokenString) {
        return new String(Hasher.SHA256.hash(new SecureString(accessTokenString.toCharArray())));
    }

    /**
     * Looks in the context to see if the request provided a header with a user token and if so the
     * token is validated, which might include authenticated decryption and verification that the token
     * has not been revoked or is expired.
     */
    void getAndValidateToken(ThreadContext ctx, ActionListener<UserToken> listener) {
        if (isEnabled()) {
            final String token = getFromHeader(ctx);
            if (token == null) {
                listener.onResponse(null);
            } else {
                decodeToken(token, ActionListener.wrap(userToken -> {
                    if (userToken != null) {
                        checkIfTokenIsValid(userToken, listener);
                    } else {
                        listener.onResponse(null);
                    }
                }, listener::onFailure));
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Reads the authentication and metadata from the given token.
     * This method does not validate whether the token is expired or not.
     */
    public void getAuthenticationAndMetaData(String token, ActionListener<Tuple<Authentication, Map<String, Object>>> listener) {
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

    /**
     * Gets the {@link UserToken} with the given {@code userTokenId} and {@code tokenVersion} by fetching and parsing the corresponding
     * token document.
     */
    private void getUserTokenFromId(String userTokenId, Version tokenVersion, ActionListener<UserToken> listener) {
        final SecurityIndexManager tokensIndex = getTokensIndexForVersion(tokenVersion);
        if (tokensIndex.isAvailable() == false) {
            logger.warn("failed to get access token [{}] because index [{}] is not available", userTokenId, tokensIndex.aliasName());
            listener.onResponse(null);
        } else {
            final GetRequest getRequest = client.prepareGet(tokensIndex.aliasName(), SINGLE_MAPPING_NAME,
                    getTokenDocumentId(userTokenId)).request();
            final Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("decode token", userTokenId, ex));
            tokensIndex.checkIndexVersionThenExecute(
                ex -> listener.onFailure(traceLog("prepare tokens index [" + tokensIndex.aliasName() +"]", userTokenId, ex)),
                () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, getRequest,
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
                                logger.warn("failed to get access token [{}] because index [{}] is not available", userTokenId,
                                        tokensIndex.aliasName());
                                listener.onResponse(null);
                            } else {
                                logger.error(new ParameterizedMessage("failed to get access token [{}]", userTokenId), e);
                                listener.onFailure(e);
                            }
                        }), client::get)
                );
        }
    }

    /**
     * If needed, for tokens that were created in a pre {@code #VERSION_ACCESS_TOKENS_UUIDS} cluster, it asynchronously decodes the token to
     * get the token document id. The process for this is asynchronous as we may need to compute a key, which can be computationally
     * expensive so this should not block the current thread, which is typically a network thread. A second reason for being asynchronous is
     * that we can restrain the amount of resources consumed by the key computation to a single thread. For tokens created in an after
     * {@code #VERSION_ACCESS_TOKENS_UUIDS} cluster, the token is just the token document Id so this is used directly without decryption
     */
    void decodeToken(String token, ActionListener<UserToken> listener) {
        final byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        try (StreamInput in = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)), bytes.length)) {
            final Version version = Version.readVersion(in);
            in.setVersion(version);
            if (version.onOrAfter(VERSION_ACCESS_TOKENS_AS_UUIDS)) {
                // The token was created in a > VERSION_ACCESS_TOKENS_UUIDS cluster
                if (in.available() < MINIMUM_BYTES) {
                    logger.debug("invalid token, smaller than [{}] bytes", MINIMUM_BYTES);
                    listener.onResponse(null);
                    return;
                }
                final String accessToken = in.readString();
                // TODO Remove this conditional after backporting to 7.x
                if (version.onOrAfter(VERSION_HASHED_TOKENS)) {
                    final String userTokenId = hashTokenString(accessToken);
                    getUserTokenFromId(userTokenId, version, listener);
                } else {
                    getUserTokenFromId(accessToken, version, listener);
                }
            } else {
                // The token was created in a < VERSION_ACCESS_TOKENS_UUIDS cluster so we need to decrypt it to get the tokenId
                if (in.available() < LEGACY_MINIMUM_BYTES) {
                    logger.debug("invalid token, smaller than [{}] bytes", LEGACY_MINIMUM_BYTES);
                    listener.onResponse(null);
                    return;
                }
                final BytesKey decodedSalt = new BytesKey(in.readByteArray());
                final BytesKey passphraseHash = new BytesKey(in.readByteArray());
                final byte[] iv = in.readByteArray();
                final BytesStreamOutput out = new BytesStreamOutput();
                Streams.copy(in, out);
                final byte[] encryptedTokenId = BytesReference.toBytes(out.bytes());
                final KeyAndCache keyAndCache = keyCache.get(passphraseHash);
                if (keyAndCache != null) {
                    getKeyAsync(decodedSalt, keyAndCache, ActionListener.wrap(decodeKey -> {
                        if (decodeKey != null) {
                            try {
                                final Cipher cipher = getDecryptionCipher(iv, decodeKey, version, decodedSalt);
                                final String tokenId = decryptTokenId(encryptedTokenId, cipher, version);
                                getUserTokenFromId(tokenId, version, listener);
                            } catch (IOException | GeneralSecurityException e) {
                                // could happen with a token that is not ours
                                logger.warn("invalid token", e);
                                listener.onResponse(null);
                            }
                        } else {
                            // could happen with a token that is not ours
                            listener.onResponse(null);
                            return;
                        }
                    }, listener::onFailure));
                } else {
                    logger.debug(() -> new ParameterizedMessage("invalid key {} key: {}", passphraseHash, keyCache.cache.keySet()));
                    listener.onResponse(null);
                }
            }
        } catch (IOException e) {
            // could happen with a token that is not ours
            if (logger.isDebugEnabled()) {
               logger.debug("built in token service unable to decode token", e);
            } else {
                logger.warn("built in token service unable to decode token");
            }
            listener.onResponse(null);
        }
    }

    /**
     * This method performs the steps necessary to invalidate an access token so that it may no longer be
     * used. The process of invalidation involves performing an update to the token document and setting
     * the {@code access_token.invalidated} field to {@code true}
     */
    public void invalidateAccessToken(String accessToken, ActionListener<TokensInvalidationResult> listener) {
        ensureEnabled();
        if (Strings.isNullOrEmpty(accessToken)) {
            listener.onFailure(traceLog("no access token provided", new IllegalArgumentException("access token must be provided")));
        } else {
            maybeStartTokenRemover();
            final Iterator<TimeValue> backoff = DEFAULT_BACKOFF.iterator();
            decodeToken(accessToken, ActionListener.wrap(userToken -> {
                if (userToken == null) {
                    listener.onFailure(traceLog("invalidate token", accessToken, malformedTokenException()));
                } else {
                    indexInvalidation(Collections.singleton(userToken), backoff, "access_token", null, listener);
                }
            }, listener::onFailure));
        }
    }

    /**
     * This method performs the steps necessary to invalidate a token so that it may no longer be used.
     */
    public void invalidateAccessToken(UserToken userToken, ActionListener<TokensInvalidationResult> listener) {
        ensureEnabled();
        if (userToken == null) {
            logger.trace("No access token provided");
            listener.onFailure(new IllegalArgumentException("access token must be provided"));
        } else {
            maybeStartTokenRemover();
            final Iterator<TimeValue> backoff = DEFAULT_BACKOFF.iterator();
            indexInvalidation(Collections.singleton(userToken), backoff, "access_token", null, listener);
        }
    }

    /**
     * This method invalidates a refresh token so that it may no longer be used. Invalidation involves performing an update to the token
     * document and setting the <code>refresh_token.invalidated</code> field to <code>true</code>
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
            final Iterator<TimeValue> backoff = DEFAULT_BACKOFF.iterator();
            findTokenFromRefreshToken(refreshToken,
                backoff, ActionListener.wrap(searchResponse -> {
                    final Tuple<UserToken, String> parsedTokens = parseTokensFromDocument(searchResponse.getSourceAsMap(), null);
                    indexInvalidation(Collections.singletonList(parsedTokens.v1()), backoff, "refresh_token", null, listener);
                }, listener::onFailure));
        }
    }

    /**
     * Invalidates all access tokens and all refresh tokens of a given {@code realmName} and/or of a given
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
                        invalidateAllTokens(tokenTuples.stream().map(t -> t.v1()).collect(Collectors.toList()), listener);
                    }
                }, listener::onFailure));
            } else {
                Predicate filter = null;
                if (Strings.hasText(username)) {
                    filter = isOfUser(username);
                }
                findActiveTokensForRealm(realmName, filter, ActionListener.wrap(tokenTuples -> {
                    if (tokenTuples.isEmpty()) {
                        logger.warn("No tokens to invalidate for realm [{}] and username [{}]", realmName, username);
                        listener.onResponse(TokensInvalidationResult.emptyResult());
                    } else {
                        invalidateAllTokens(tokenTuples.stream().map(t -> t.v1()).collect(Collectors.toList()), listener);
                    }
                }, listener::onFailure));
            }
        }
    }

    /**
     * Invalidates a collection of access_token and refresh_token that were retrieved by
     * {@link TokenService#invalidateActiveTokensForRealmAndUser}
     *
     * @param userTokens The user tokens for which access and refresh tokens should be invalidated
     * @param listener  the listener to notify upon completion
     */
    private void invalidateAllTokens(Collection<UserToken> userTokens, ActionListener<TokensInvalidationResult> listener) {
        maybeStartTokenRemover();
        // Invalidate the refresh tokens first so that they cannot be used to get new
        // access tokens while we invalidate the access tokens we currently know about
        final Iterator<TimeValue> backoff = DEFAULT_BACKOFF.iterator();
        indexInvalidation(userTokens, backoff, "refresh_token", null, ActionListener.wrap(result ->
                    indexInvalidation(userTokens, backoff, "access_token", result, listener),
                listener::onFailure));
    }

    /**
     * Invalidates access and/or refresh tokens associated to a user token (coexisting in the same token document) 
     */
    private void indexInvalidation(Collection<UserToken> userTokens, Iterator<TimeValue> backoff, String srcPrefix,
            @Nullable TokensInvalidationResult previousResult, ActionListener<TokensInvalidationResult> listener) {
        final Set<String> idsOfRecentTokens = new HashSet<>();
        final Set<String> idsOfOlderTokens = new HashSet<>();
        for (UserToken userToken : userTokens) {
            if (userToken.getVersion().onOrAfter(VERSION_TOKENS_INDEX_INTRODUCED)) {
                idsOfRecentTokens.add(userToken.getId());
            } else {
                idsOfOlderTokens.add(userToken.getId());
            }
        }
        if (false == idsOfOlderTokens.isEmpty()) {
            indexInvalidation(idsOfOlderTokens, securityMainIndex, backoff, srcPrefix, previousResult, ActionListener.wrap(newResult -> {
                if (false == idsOfRecentTokens.isEmpty()) {
                    // carry-over result of the invalidation for the tokens security index
                    indexInvalidation(idsOfRecentTokens, securityTokensIndex, backoff, srcPrefix, newResult, listener);
                } else {
                    listener.onResponse(newResult);
                }
            }, listener::onFailure));
        } else {
            indexInvalidation(idsOfRecentTokens, securityTokensIndex, backoff, srcPrefix, previousResult, listener);
        }
    }

    /**
     * Performs the actual invalidation of a collection of tokens. In case of recoverable errors ( see
     * {@link TransportActions#isShardNotAvailableException} ) the UpdateRequests to mark the tokens as invalidated are retried using
     * an exponential backoff policy.
     *
     * @param tokenIds              the tokens to invalidate
     * @param tokensIndexManager    the manager for the index where the tokens are stored
     * @param backoff               the amount of time to delay between attempts
     * @param srcPrefix             the prefix to use when constructing the doc to update, either refresh_token or access_token depending on
     *                              what type of tokens should be invalidated
     * @param previousResult        if this not the initial attempt for invalidation, it contains the result of invalidating
     *                              tokens up to the point of the retry. This result is added to the result of the current attempt
     * @param listener              the listener to notify upon completion
     */
    private void indexInvalidation(Collection<String> tokenIds, SecurityIndexManager tokensIndexManager, Iterator<TimeValue> backoff,
                                   String srcPrefix, @Nullable TokensInvalidationResult previousResult,
                                   ActionListener<TokensInvalidationResult> listener) {
        if (tokenIds.isEmpty()) {
            logger.warn("No [{}] tokens provided for invalidation", srcPrefix);
            listener.onFailure(invalidGrantException("No tokens provided for invalidation"));
        } else {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (String tokenId : tokenIds) {
                UpdateRequest request = client
                        .prepareUpdate(tokensIndexManager.aliasName(), SINGLE_MAPPING_NAME, getTokenDocumentId(tokenId))
                        .setDoc(srcPrefix, Collections.singletonMap("invalidated", true))
                        .setFetchSource(srcPrefix, null)
                        .request();
                bulkRequestBuilder.add(request);
            }
            bulkRequestBuilder.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
            tokensIndexManager.prepareIndexIfNeededThenExecute(
                ex -> listener.onFailure(traceLog("prepare index [" + tokensIndexManager.aliasName() + "]", ex)),
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
                                } else {
                                    traceLog("invalidate access token", failedTokenDocId, cause);
                                    failedRequestResponses.add(new ElasticsearchException("Error invalidating " + srcPrefix + ": ", cause));
                                }
                            } else {
                                UpdateResponse updateResponse = bulkItemResponse.getResponse();
                                if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                                    logger.debug(() -> new ParameterizedMessage("Invalidated [{}] for doc [{}]",
                                            srcPrefix, updateResponse.getGetResult().getId()));
                                    invalidated.add(updateResponse.getGetResult().getId());
                                } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                                    previouslyInvalidated.add(updateResponse.getGetResult().getId());
                                }
                            }
                        }
                        if (retryTokenDocIds.isEmpty() == false && backoff.hasNext()) {
                            logger.debug("failed to invalidate [{}] tokens out of [{}], retrying to invalidate these too",
                                    retryTokenDocIds.size(), tokenIds.size());
                            final TokensInvalidationResult incompleteResult = new TokensInvalidationResult(invalidated,
                                        previouslyInvalidated, failedRequestResponses);
                            final Runnable retryWithContextRunnable = client.threadPool().getThreadContext()
                                    .preserveContext(() -> indexInvalidation(retryTokenDocIds, tokensIndexManager, backoff,
                                            srcPrefix, incompleteResult, listener));
                            client.threadPool().schedule(retryWithContextRunnable, backoff.next(), GENERIC);
                        } else {
                            if (retryTokenDocIds.isEmpty() == false) {
                                logger.warn("failed to invalidate [{}] tokens out of [{}] after all retries", retryTokenDocIds.size(),
                                        tokenIds.size());
                                for (String retryTokenDocId : retryTokenDocIds) {
                                    failedRequestResponses.add(
                                            new ElasticsearchException("Error invalidating [{}] with doc id [{}] after retries exhausted",
                                                    srcPrefix, retryTokenDocId));
                                }
                            }
                            final TokensInvalidationResult result = new TokensInvalidationResult(invalidated, previouslyInvalidated,
                                    failedRequestResponses);
                            listener.onResponse(result);
                        }
                    }, e -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        traceLog("invalidate tokens", cause);
                        if (isShardNotAvailableException(cause) && backoff.hasNext()) {
                            logger.debug("failed to invalidate tokens, retrying ");
                            final Runnable retryWithContextRunnable = client.threadPool().getThreadContext()
                                    .preserveContext(() -> indexInvalidation(tokenIds, tokensIndexManager, backoff, srcPrefix,
                                            previousResult, listener));
                            client.threadPool().schedule(retryWithContextRunnable, backoff.next(), GENERIC);
                        } else {
                            listener.onFailure(e);
                        }
                    }), client::bulk));
        }
    }

    /**
     * Called by the transport action in order to start the process of refreshing a token.
     *
     * @param refreshToken The refresh token as provided by the client
     * @param listener The listener to call upon completion with a {@link Tuple} containing the
     *                 serialized access token and serialized refresh token as these will be returned to the client
     */
    public void refreshToken(String refreshToken, ActionListener<Tuple<String, String>> listener) {
        ensureEnabled();
        final Instant refreshRequested = clock.instant();
        final Iterator<TimeValue> backoff = DEFAULT_BACKOFF.iterator();
        findTokenFromRefreshToken(refreshToken,
            backoff,
            ActionListener.wrap(tokenDocHit -> {
                final Authentication clientAuth = Authentication.readFromContext(client.threadPool().getThreadContext());
                innerRefresh(refreshToken, tokenDocHit.getId(), tokenDocHit.getSourceAsMap(), tokenDocHit.getSeqNo(),
                    tokenDocHit.getPrimaryTerm(),
                    clientAuth, backoff, refreshRequested, listener);
            }, listener::onFailure));
    }

    /**
     * Infers the format and version of the passed in {@code refreshToken}. Delegates the actual search of the token document to
     * {@code #findTokenFromRefreshToken(String, SecurityIndexManager, Iterator, ActionListener)} .
     */
    private void findTokenFromRefreshToken(String refreshToken, Iterator<TimeValue> backoff, ActionListener<SearchHit> listener) {
        if (refreshToken.length() == TOKEN_LENGTH) {
            // first check if token has the old format before the new version-prepended one
            logger.debug("Assuming an unversioned refresh token [{}], generated for node versions"
                + " prior to the introduction of the version-header format.", refreshToken);
            findTokenFromRefreshToken(refreshToken, securityMainIndex, backoff, listener);
        } else {
            if (refreshToken.length() == HASHED_TOKEN_LENGTH) {
                logger.debug("Assuming a hashed refresh token [{}] retrieved from the tokens index", refreshToken);
                findTokenFromRefreshToken(refreshToken, securityTokensIndex, backoff, listener);
            } else {
                logger.debug("Assuming a refresh token [{}] provided from a client", refreshToken);
                try {
                    final Tuple<Version, String> versionAndRefreshTokenTuple = unpackVersionAndPayload(refreshToken);
                    final Version refreshTokenVersion = versionAndRefreshTokenTuple.v1();
                    final String unencodedRefreshToken = versionAndRefreshTokenTuple.v2();
                    if (refreshTokenVersion.before(VERSION_TOKENS_INDEX_INTRODUCED) || unencodedRefreshToken.length() != TOKEN_LENGTH) {
                        logger.debug("Decoded refresh token [{}] with version [{}] is invalid.", unencodedRefreshToken,
                            refreshTokenVersion);
                        listener.onFailure(malformedTokenException());
                    } else {
                        // TODO Remove this conditional after backporting to 7.x
                        if (refreshTokenVersion.onOrAfter(VERSION_HASHED_TOKENS)) {
                            final String hashedRefreshToken = hashTokenString(unencodedRefreshToken);
                            findTokenFromRefreshToken(hashedRefreshToken, securityTokensIndex, backoff, listener);
                        } else {
                            findTokenFromRefreshToken(unencodedRefreshToken, securityTokensIndex, backoff, listener);
                        }
                    }
                } catch (IOException e) {
                    logger.debug(() -> new ParameterizedMessage("Could not decode refresh token [{}].", refreshToken), e);
                    listener.onFailure(malformedTokenException());
                }
            }
        }
    }

    /**
     * Performs an asynchronous search request for the token document that contains the {@code refreshToken} and calls the {@code listener}
     * with the resulting {@link SearchResponse}. In case of recoverable errors the {@code SearchRequest} is retried using an exponential
     * backoff policy. This method requires the tokens index where the token document, pointed to by the refresh token, resides.
     */
    private void findTokenFromRefreshToken(String refreshToken, SecurityIndexManager tokensIndexManager, Iterator<TimeValue> backoff,
                                           ActionListener<SearchHit> listener) {
        final Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("find token by refresh token", refreshToken, ex));
        final Consumer<Exception> maybeRetryOnFailure = ex -> {
            if (backoff.hasNext()) {
                final TimeValue backofTimeValue = backoff.next();
                logger.debug("retrying after [{}] back off", backofTimeValue);
                final Runnable retryWithContextRunnable = client.threadPool().getThreadContext()
                        .preserveContext(() -> findTokenFromRefreshToken(refreshToken, tokensIndexManager, backoff, listener));
                client.threadPool().schedule(retryWithContextRunnable, backofTimeValue, GENERIC);
            } else {
                logger.warn("failed to find token from refresh token after all retries");
                onFailure.accept(ex);
            }
        };
        final SecurityIndexManager frozenTokensIndex = tokensIndexManager.freeze();
        if (frozenTokensIndex.indexExists() == false) {
            logger.warn("index [{}] does not exist therefore refresh token cannot be validated", frozenTokensIndex.aliasName());
            listener.onFailure(invalidGrantException("could not refresh the requested token"));
        } else if (frozenTokensIndex.isAvailable() == false) {
            logger.debug("index [{}] is not available to find token from refresh token, retrying", frozenTokensIndex.aliasName());
            maybeRetryOnFailure.accept(invalidGrantException("could not refresh the requested token"));
        } else {
            final SearchRequest request = client.prepareSearch(tokensIndexManager.aliasName())
                    .setQuery(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery("doc_type", TOKEN_DOC_TYPE))
                            .filter(QueryBuilders.termQuery("refresh_token.token", refreshToken)))
                    .seqNoAndPrimaryTerm(true)
                    .request();
            tokensIndexManager.checkIndexVersionThenExecute(listener::onFailure, () ->
                executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
                    ActionListener.<SearchResponse>wrap(searchResponse -> {
                        if (searchResponse.isTimedOut()) {
                            logger.debug("find token from refresh token response timed out, retrying");
                            maybeRetryOnFailure.accept(invalidGrantException("could not refresh the requested token"));
                        } else if (searchResponse.getHits().getHits().length < 1) {
                            logger.warn("could not find token document for refresh token");
                            onFailure.accept(invalidGrantException("could not refresh the requested token"));
                        } else if (searchResponse.getHits().getHits().length > 1) {
                            onFailure.accept(new IllegalStateException("multiple tokens share the same refresh token"));
                        } else {
                            listener.onResponse(searchResponse.getHits().getAt(0));
                        }
                    }, e -> {
                        if (isShardNotAvailableException(e)) {
                            logger.debug("find token from refresh token request failed because of unavailable shards, retrying");
                            maybeRetryOnFailure.accept(invalidGrantException("could not refresh the requested token"));
                        } else {
                            onFailure.accept(e);
                        }
                    }),
                    client::search));
        }
    }

    /**
     * Performs the actual refresh of the token with retries in case of certain exceptions that may be recoverable. The refresh involves two
     * steps: First, we check if the token document is still valid for refresh. Then, in the case that the token has been refreshed within
     * the previous 30 seconds, we do not create a new token document but instead retrieve the one that was created by the original refresh
     * and return an access token and refresh token based on that. Otherwise this token document gets its refresh_token marked as refreshed,
     * while also storing the Instant when it was refreshed along with a pointer to the new token document that holds the refresh_token that
     * supersedes this one. The new document that contains the new access token and refresh token is created and finally the new access
     * token and refresh token are returned to the listener.
     */
    private void innerRefresh(String refreshToken, String tokenDocId, Map<String, Object> source, long seqNo, long primaryTerm,
                              Authentication clientAuth, Iterator<TimeValue> backoff, Instant refreshRequested,
                              ActionListener<Tuple<String, String>> listener) {
        logger.debug("Attempting to refresh token stored in token document [{}]", tokenDocId);
        final Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("refresh token", tokenDocId, ex));
        final Tuple<RefreshTokenStatus, Optional<ElasticsearchSecurityException>> checkRefreshResult;
        try {
            checkRefreshResult = checkTokenDocumentForRefresh(refreshRequested, clientAuth, source);
        } catch (DateTimeException | IllegalStateException e) {
            onFailure.accept(new ElasticsearchSecurityException("invalid token document", e));
            return;
        }
        if (checkRefreshResult.v2().isPresent()) {
            onFailure.accept(checkRefreshResult.v2().get());
            return;
        }
        final RefreshTokenStatus refreshTokenStatus = checkRefreshResult.v1();
        if (refreshTokenStatus.isRefreshed()) {
            logger.debug("Token document [{}] was recently refreshed, when a new token document was generated. Reusing that result.",
                tokenDocId);
            decryptAndReturnSupersedingTokens(refreshToken, refreshTokenStatus, listener);
        } else {
            final String newAccessTokenString = UUIDs.randomBase64UUID();
            final String newRefreshTokenString = UUIDs.randomBase64UUID();
            final Version newTokenVersion = getTokenVersionCompatibility();
            final Map<String, Object> updateMap = new HashMap<>();
            updateMap.put("refreshed", true);
            if (newTokenVersion.onOrAfter(VERSION_MULTIPLE_CONCURRENT_REFRESHES)) {
                updateMap.put("refresh_time", clock.instant().toEpochMilli());
                try {
                    final byte[] iv = getRandomBytes(IV_BYTES);
                    final byte[] salt = getRandomBytes(SALT_BYTES);
                    String encryptedAccessAndRefreshToken = encryptSupersedingTokens(newAccessTokenString,
                        newRefreshTokenString, refreshToken, iv, salt);
                    updateMap.put("superseding.encrypted_tokens", encryptedAccessAndRefreshToken);
                    updateMap.put("superseding.encryption_iv", Base64.getEncoder().encodeToString(iv));
                    updateMap.put("superseding.encryption_salt", Base64.getEncoder().encodeToString(salt));
                } catch (GeneralSecurityException e) {
                    logger.warn("could not encrypt access token and refresh token string", e);
                    onFailure.accept(invalidGrantException("could not refresh the requested token"));
                }
            }
            assert seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO : "expected an assigned sequence number";
            assert primaryTerm != SequenceNumbers.UNASSIGNED_PRIMARY_TERM : "expected an assigned primary term";
            final SecurityIndexManager refreshedTokenIndex = getTokensIndexForVersion(refreshTokenStatus.getVersion());
            final UpdateRequestBuilder updateRequest = client
                    .prepareUpdate(refreshedTokenIndex.aliasName(), SINGLE_MAPPING_NAME, tokenDocId)
                    .setDoc("refresh_token", updateMap)
                    .setFetchSource(true)
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .setIfSeqNo(seqNo)
                    .setIfPrimaryTerm(primaryTerm);
            refreshedTokenIndex.prepareIndexIfNeededThenExecute(
                    ex -> listener.onFailure(traceLog("prepare index [" + refreshedTokenIndex.aliasName() + "]", ex)),
                    () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, updateRequest.request(),
                    ActionListener.<UpdateResponse>wrap(updateResponse -> {
                        if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                            logger.debug(() -> new ParameterizedMessage("updated the original token document to {}",
                                    updateResponse.getGetResult().sourceAsMap()));
                            final Tuple<UserToken, String> parsedTokens = parseTokensFromDocument(source, null);
                            final UserToken toRefreshUserToken = parsedTokens.v1();
                            createOAuth2Tokens(newAccessTokenString, newRefreshTokenString, newTokenVersion,
                                getTokensIndexForVersion(newTokenVersion), toRefreshUserToken.getAuthentication(), clientAuth,
                                toRefreshUserToken.getMetadata(), listener);
                        } else if (backoff.hasNext()) {
                            logger.info("failed to update the original token document [{}], the update result was [{}]. Retrying",
                                    tokenDocId, updateResponse.getResult());
                            final Runnable retryWithContextRunnable = client.threadPool().getThreadContext()
                                .preserveContext(() -> innerRefresh(refreshToken, tokenDocId, source, seqNo, primaryTerm, clientAuth,
                                    backoff, refreshRequested, listener));
                            client.threadPool().schedule(retryWithContextRunnable, backoff.next(), GENERIC);
                        } else {
                            logger.info("failed to update the original token document [{}] after all retries, the update result was [{}]. ",
                                    tokenDocId, updateResponse.getResult());
                            listener.onFailure(invalidGrantException("could not refresh the requested token"));
                        }
                    }, e -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof VersionConflictEngineException) {
                            // The document has been updated by another thread, get it again.
                            logger.debug("version conflict while updating document [{}], attempting to get it again", tokenDocId);
                            getTokenDocAsync(tokenDocId, refreshedTokenIndex, new ActionListener<GetResponse>() {
                                @Override
                                public void onResponse(GetResponse response) {
                                    if (response.isExists()) {
                                        innerRefresh(refreshToken, tokenDocId, response.getSource(), response.getSeqNo(),
                                            response.getPrimaryTerm(), clientAuth, backoff, refreshRequested, listener);
                                    } else {
                                        logger.warn("could not find token document [{}] for refresh", tokenDocId);
                                        onFailure.accept(invalidGrantException("could not refresh the requested token"));
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (isShardNotAvailableException(e)) {
                                        if (backoff.hasNext()) {
                                            logger.info("could not get token document [{}] for refresh, retrying", tokenDocId);
                                            final Runnable retryWithContextRunnable = client.threadPool().getThreadContext()
                                                    .preserveContext(() -> getTokenDocAsync(tokenDocId, refreshedTokenIndex, this));
                                            client.threadPool().schedule(retryWithContextRunnable, backoff.next(), GENERIC);
                                        } else {
                                            logger.warn("could not get token document [{}] for refresh after all retries", tokenDocId);
                                            onFailure.accept(invalidGrantException("could not refresh the requested token"));
                                        }
                                    } else {
                                        onFailure.accept(e);
                                    }
                                }
                            });
                        } else if (isShardNotAvailableException(e)) {
                            if (backoff.hasNext()) {
                                logger.debug("failed to update the original token document [{}], retrying", tokenDocId);
                                final Runnable retryWithContextRunnable = client.threadPool().getThreadContext()
                                    .preserveContext(() -> innerRefresh(refreshToken, tokenDocId, source, seqNo, primaryTerm,
                                        clientAuth, backoff, refreshRequested, listener));
                                client.threadPool().schedule(retryWithContextRunnable, backoff.next(), GENERIC);
                            } else {
                                logger.warn("failed to update the original token document [{}], after all retries", tokenDocId);
                                onFailure.accept(invalidGrantException("could not refresh the requested token"));
                            }
                        } else {
                            onFailure.accept(e);
                        }
                    }), client::update));
        }
    }

    /**
     * Decrypts the values of the superseding access token and the refresh token, using a key derived from the superseded refresh token. It
     * encodes the version and serializes the tokens before calling the listener, in the same manner as {@link #createOAuth2Tokens } does.
     *
     * @param refreshToken The refresh token that the user sent in the request, used to derive the decryption key
     * @param refreshTokenStatus The {@link RefreshTokenStatus} containing information about the superseding tokens as retrieved from the
     *                          index
     * @param listener The listener to call upon completion with a {@link Tuple} containing the
     *                 serialized access token and serialized refresh token as these will be returned to the client
     */
    void decryptAndReturnSupersedingTokens(String refreshToken, RefreshTokenStatus refreshTokenStatus,
                                           ActionListener<Tuple<String, String>> listener) {
        final byte[] iv = Base64.getDecoder().decode(refreshTokenStatus.getIv());
        final byte[] salt = Base64.getDecoder().decode(refreshTokenStatus.getSalt());
        final byte[] encryptedSupersedingTokens = Base64.getDecoder().decode(refreshTokenStatus.getSupersedingTokens());
        try {
            Cipher cipher = getDecryptionCipher(iv, refreshToken, salt);
            final String supersedingTokens = new String(cipher.doFinal(encryptedSupersedingTokens), StandardCharsets.UTF_8);
            final String[] decryptedTokens = supersedingTokens.split("\\|");
            if (decryptedTokens.length != 2) {
                logger.warn("Decrypted tokens string is not correctly formatted");
                listener.onFailure(invalidGrantException("could not refresh the requested token"));
            }
            listener.onResponse(new Tuple<>(prependVersionAndEncodeAccessToken(refreshTokenStatus.getVersion(), decryptedTokens[0]),
                prependVersionAndEncodeRefreshToken(refreshTokenStatus.getVersion(), decryptedTokens[1])));
        } catch (GeneralSecurityException | IOException e) {
            logger.warn("Could not get stored superseding token values", e);
            listener.onFailure(invalidGrantException("could not refresh the requested token"));
        }
    }

    /*
     * Encrypts the values of the superseding access token and the refresh token, using a key derived from the superseded refresh token.
     * The tokens are concatenated to a string separated with `|` before encryption so that we only perform one encryption operation
     * and that we only need to store one field
     */
    String encryptSupersedingTokens(String supersedingAccessToken, String supersedingRefreshToken,
                                    String refreshToken, byte[] iv, byte[] salt) throws GeneralSecurityException {
        Cipher cipher = getEncryptionCipher(iv, refreshToken, salt);
        final String supersedingTokens = supersedingAccessToken + "|" + supersedingRefreshToken;
        return Base64.getEncoder().encodeToString(cipher.doFinal(supersedingTokens.getBytes(StandardCharsets.UTF_8)));
    }

    private void getTokenDocAsync(String tokenDocId, SecurityIndexManager tokensIndex, ActionListener<GetResponse> listener) {
        final GetRequest getRequest = client.prepareGet(tokensIndex.aliasName(), SINGLE_MAPPING_NAME, tokenDocId).request();
        tokensIndex.checkIndexVersionThenExecute(
                ex -> listener.onFailure(traceLog("prepare tokens index [" + tokensIndex.aliasName() + "]", tokenDocId, ex)),
                () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, getRequest, listener, client::get));
    }

    Version getTokenVersionCompatibility() {
        // newly minted tokens are compatible with the min node version in the cluster
        return clusterService.state().nodes().getMinNodeVersion();
    }

    public static Boolean isTokenServiceEnabled(Settings settings) {
        return XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.get(settings);
    }

    /**
     * A refresh token has a fixed maximum lifetime of {@code ExpiredTokenRemover#MAXIMUM_TOKEN_LIFETIME_HOURS} hours. This checks if the
     * token document represents a valid token wrt this time interval.
     */
    private static Optional<ElasticsearchSecurityException> checkTokenDocumentExpired(Instant refreshRequested, Map<String, Object> src) {
        final Long creationEpochMilli = (Long) src.get("creation_time");
        if (creationEpochMilli == null) {
            throw new IllegalStateException("token document is missing creation time value");
        } else {
            final Instant creationTime = Instant.ofEpochMilli(creationEpochMilli);
            if (refreshRequested.isAfter(creationTime.plus(ExpiredTokenRemover.MAXIMUM_TOKEN_LIFETIME_HOURS, ChronoUnit.HOURS))) {
                return Optional.of(invalidGrantException("token document has expired"));
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Parses the {@code RefreshTokenStatus} from the token document and throws an exception if the document is malformed. Returns the
     * parsed {@code RefreshTokenStatus} together with an {@code Optional} validation exception that encapsulates the various logic about
     * when and by who a token can be refreshed.
     */
    private static Tuple<RefreshTokenStatus, Optional<ElasticsearchSecurityException>> checkTokenDocumentForRefresh(
        Instant refreshRequested, Authentication clientAuth, Map<String, Object> source) throws IllegalStateException, DateTimeException {
        final RefreshTokenStatus refreshTokenStatus = RefreshTokenStatus.fromSourceMap(getRefreshTokenSourceMap(source));
        final UserToken userToken = UserToken.fromSourceMap(getUserTokenSourceMap(source));
        refreshTokenStatus.setVersion(userToken.getVersion());
        final ElasticsearchSecurityException validationException = checkTokenDocumentExpired(refreshRequested, source).orElseGet(() -> {
            if (refreshTokenStatus.isInvalidated()) {
                return invalidGrantException("token has been invalidated");
            } else {
                return checkClientCanRefresh(refreshTokenStatus, clientAuth)
                    .orElse(checkMultipleRefreshes(refreshRequested, refreshTokenStatus).orElse(null));
            }
        });
        return new Tuple<>(refreshTokenStatus, Optional.ofNullable(validationException));
    }

    /**
     * Refresh tokens are bound to be used only by the client that originally created them. This check validates this condition, given the
     * {@code Authentication} of the client that attempted the refresh operation.
     */
    private static Optional<ElasticsearchSecurityException> checkClientCanRefresh(RefreshTokenStatus refreshToken,
                                                                                  Authentication clientAuthentication) {
        if (clientAuthentication.getUser().principal().equals(refreshToken.getAssociatedUser()) == false) {
            logger.warn("Token was originally created by [{}] but [{}] attempted to refresh it", refreshToken.getAssociatedUser(),
                    clientAuthentication.getUser().principal());
            return Optional.of(invalidGrantException("tokens must be refreshed by the creating client"));
        } else if (clientAuthentication.getAuthenticatedBy().getName().equals(refreshToken.getAssociatedRealm()) == false) {
            logger.warn("[{}] created the refresh token while authenticated by [{}] but is now authenticated by [{}]",
                    refreshToken.getAssociatedUser(), refreshToken.getAssociatedRealm(),
                    clientAuthentication.getAuthenticatedBy().getName());
            return Optional.of(invalidGrantException("tokens must be refreshed by the creating client"));
        } else {
            return Optional.empty();
        }
    }

    private static Map<String, Object> getRefreshTokenSourceMap(Map<String, Object> source) {
        final Map<String, Object> refreshTokenSource = (Map<String, Object>) source.get("refresh_token");
        if (refreshTokenSource == null || refreshTokenSource.isEmpty()) {
            throw new IllegalStateException("token document is missing the refresh_token object");
        }
        return refreshTokenSource;
    }

    private static Map<String, Object> getUserTokenSourceMap(Map<String, Object> source) {
        final Map<String, Object> accessTokenSource = (Map<String, Object>) source.get("access_token");
        if (accessTokenSource == null || accessTokenSource.isEmpty()) {
            throw new IllegalStateException("token document is missing the access_token object");
        }
        final Map<String, Object> userTokenSource = (Map<String, Object>) accessTokenSource.get("user_token");
        if (userTokenSource == null || userTokenSource.isEmpty()) {
            throw new IllegalStateException("token document is missing the user token info");
        }
        return userTokenSource;
    }

    /**
     * Checks if the token can be refreshed once more. If a token has previously been refreshed, it can only by refreshed again inside a
     * short span of time (30 s).
     * 
     * @return An {@code Optional} containing the exception in case this refresh token cannot be reused, or an empty <b>Optional</b> if
     *         refreshing is allowed.
     */
    private static Optional<ElasticsearchSecurityException> checkMultipleRefreshes(Instant refreshRequested,
                                                                                   RefreshTokenStatus refreshTokenStatus) {
        if (refreshTokenStatus.isRefreshed()) {
            if (refreshTokenStatus.getVersion().onOrAfter(VERSION_MULTIPLE_CONCURRENT_REFRESHES)) {
                if (refreshRequested.isAfter(refreshTokenStatus.getRefreshInstant().plus(30L, ChronoUnit.SECONDS))) {
                    return Optional.of(invalidGrantException("token has already been refreshed more than 30 seconds in the past"));
                }
                if (refreshRequested.isBefore(refreshTokenStatus.getRefreshInstant().minus(30L, ChronoUnit.SECONDS))) {
                    return Optional
                            .of(invalidGrantException("token has been refreshed more than 30 seconds in the future, clock skew too great"));
                }
            } else {
                return Optional.of(invalidGrantException("token has already been refreshed"));
            }
        }
        return Optional.empty();
    }

    /**
     * Find stored refresh and access tokens that have not been invalidated or expired, and were issued against
     * the specified realm.
     *
     * @param realmName The name of the realm for which to get the tokens
     * @param filter    an optional Predicate to test the source of the found documents against
     * @param listener  The listener to notify upon completion
     */
    public void findActiveTokensForRealm(String realmName, @Nullable Predicate<Map<String, Object>> filter,
                                         ActionListener<Collection<Tuple<UserToken, String>>> listener) {
        ensureEnabled();
        if (Strings.isNullOrEmpty(realmName)) {
            listener.onFailure(new IllegalArgumentException("realm name is required"));
            return;
        }
        sourceIndicesWithTokensAndRun(ActionListener.wrap(indicesWithTokens -> {
            if (indicesWithTokens.isEmpty()) {
                listener.onResponse(Collections.emptyList());
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
                                        .must(QueryBuilders.rangeQuery("creation_time").gte(now.toEpochMilli()
                                                - TimeValue.timeValueHours(ExpiredTokenRemover.MAXIMUM_TOKEN_LIFETIME_HOURS).millis()))
                                        )
                                );
                final SearchRequest request = client.prepareSearch(indicesWithTokens.toArray(new String[0]))
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                        .setQuery(boolQuery)
                        .setVersion(false)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                ScrollHelper.fetchAllByEntity(client, request, listener, (SearchHit hit) -> filterAndParseHit(hit, filter));
            }
        }, listener::onFailure));

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
        if (Strings.isNullOrEmpty(username)) {
            listener.onFailure(new IllegalArgumentException("username is required"));
            return;
        }
        sourceIndicesWithTokensAndRun(ActionListener.wrap(indicesWithTokens -> {
            if (indicesWithTokens.isEmpty()) {
                listener.onResponse(Collections.emptyList());
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
                                        .must(QueryBuilders.rangeQuery("creation_time").gte(now.toEpochMilli()
                                                - TimeValue.timeValueHours(ExpiredTokenRemover.MAXIMUM_TOKEN_LIFETIME_HOURS).millis()))
                                        )
                                );
                final SearchRequest request = client.prepareSearch(indicesWithTokens.toArray(new String[0]))
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                        .setQuery(boolQuery)
                        .setVersion(false)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                ScrollHelper.fetchAllByEntity(client, request, listener, (SearchHit hit) -> filterAndParseHit(hit, isOfUser(username)));
            }
        }, listener::onFailure));
    }

    /**
     * Security tokens were traditionally stored on the main security index but after version {@code #VERSION_TOKENS_INDEX_INTRODUCED} they
     * have been stored on a dedicated separate index. This move has been implemented without requiring user intervention, so the newly
     * created tokens started to be created in the new index, while the old tokens were still usable out of the main security index, subject
     * to their maximum lifetime of {@code ExpiredTokenRemover#MAXIMUM_TOKEN_LIFETIME_HOURS} hours. Once the dedicated tokens index has been
     * automatically created, all the onwards created tokens will be stored inside it. This function returns the list of the indices names
     * that might contain tokens. Unless there are availability or version issues, the dedicated tokens index always contains tokens. The
     * main security index <i>might</i> contain tokens if the tokens index has not been created yet, or if it has been created recently so
     * that there might still be tokens that have not yet exceeded their maximum lifetime.
     */
    private void sourceIndicesWithTokensAndRun(ActionListener<List<String>> listener) {
        final List<String> indicesWithTokens = new ArrayList<>(2);
        final SecurityIndexManager frozenTokensIndex = securityTokensIndex.freeze();
        if (frozenTokensIndex.indexExists()) {
            // an existing tokens index always contains tokens (if available and version allows)
            if (false == frozenTokensIndex.isAvailable()) {
                listener.onFailure(frozenTokensIndex.getUnavailableReason());
                return;
            }
            if (false == frozenTokensIndex.isIndexUpToDate()) {
                listener.onFailure(new IllegalStateException(
                        "Index [" + frozenTokensIndex.aliasName() + "] is not on the current version. Features relying on the index"
                                + " will not be available until the upgrade API is run on the index"));
                return;
            }
            indicesWithTokens.add(frozenTokensIndex.aliasName());
        }
        final SecurityIndexManager frozenMainIndex = securityMainIndex.freeze();
        if (frozenMainIndex.indexExists()) {
            // main security index _might_ contain tokens if the tokens index has been created recently 
            if (false == frozenTokensIndex.indexExists() || frozenTokensIndex.getCreationTime()
                    .isAfter(clock.instant().minus(ExpiredTokenRemover.MAXIMUM_TOKEN_LIFETIME_HOURS, ChronoUnit.HOURS))) {
                if (false == frozenMainIndex.isAvailable()) {
                    listener.onFailure(frozenMainIndex.getUnavailableReason());
                    return;
                }
                if (false == frozenMainIndex.isIndexUpToDate()) {
                    listener.onFailure(new IllegalStateException(
                            "Index [" + frozenMainIndex.aliasName() + "] is not on the current version. Features relying on the index"
                                    + " will not be available until the upgrade API is run on the index"));
                    return;
                }
                indicesWithTokens.add(frozenMainIndex.aliasName());
            }
        }
        listener.onResponse(indicesWithTokens);
    }

    private BytesReference createTokenDocument(UserToken userToken, @Nullable String refreshToken,
                                               @Nullable Authentication originatingClientAuth) {
        assert refreshToken == null || originatingClientAuth != null : "non-null refresh token " + refreshToken
            + " requires non-null client authn " + originatingClientAuth;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("doc_type", TOKEN_DOC_TYPE);
            builder.field("creation_time", getCreationTime(userToken.getExpirationTime()).toEpochMilli());
            if (refreshToken != null) {
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
                    .field("realm", userToken.getAuthentication().getAuthenticatedBy().getName())
                    .endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception when constructing a JSON document.", e);
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

    private Tuple<UserToken, String> filterAndParseHit(SearchHit hit, @Nullable Predicate<Map<String, Object>> filter)
            throws IllegalStateException, DateTimeException {
        final Map<String, Object> source = hit.getSourceAsMap();
        if (source == null) {
            throw new IllegalStateException("token document did not have source but source should have been fetched");
        }
        return parseTokensFromDocument(source, filter);
    }

    /**
     * Parses a token document into a Tuple of a {@link UserToken} and a String representing the corresponding refresh_token
     *
     * @param source The token document source as retrieved
     * @param filter an optional Predicate to test the source of the UserToken against
     * @return A {@link Tuple} of access-token and refresh-token-id or null if a Predicate is defined and the userToken source doesn't
     * satisfy it
     */
    private Tuple<UserToken, String> parseTokensFromDocument(Map<String, Object> source, @Nullable Predicate<Map<String, Object>> filter)
            throws IllegalStateException, DateTimeException {
        final String hashedRefreshToken = (String) ((Map<String, Object>) source.get("refresh_token")).get("token");
        final Map<String, Object> userTokenSource = (Map<String, Object>)
            ((Map<String, Object>) source.get("access_token")).get("user_token");
        if (null != filter && filter.test(userTokenSource) == false) {
            return null;
        }
        final UserToken userToken = UserToken.fromSourceMap(userTokenSource);
        return new Tuple<>(userToken, hashedRefreshToken);
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

    private boolean isEnabled() {
        return enabled && licenseState.isTokenServiceAllowed();
    }

    private void ensureEnabled() {
        if (licenseState.isTokenServiceAllowed() == false) {
            throw LicenseUtils.newComplianceException("security tokens");
        }
        if (enabled == false) {
            throw new IllegalStateException("security tokens are not enabled");
        }
    }

    /**
     * In version {@code #VERSION_TOKENS_INDEX_INTRODUCED} security tokens were moved into a separate index, away from the other entities in
     * the main security index, due to their ephemeral nature. They moved "seamlessly" - without manual user intervention. In this way, new
     * tokens are created in the new index, while the existing ones were left in place - to be accessed from the old index - and due to be
     * removed automatically by the {@code ExpiredTokenRemover} periodic job. Therefore, in general, when searching for a token we need to
     * consider both the new and the old indices.
     */
    private SecurityIndexManager getTokensIndexForVersion(Version version) {
        if (version.onOrAfter(VERSION_TOKENS_INDEX_INTRODUCED)) {
            return securityTokensIndex;
        } else {
            return securityMainIndex;
        }
    }

    /**
     * Checks if the access token has been explicitly invalidated
     */
    private void checkIfTokenIsValid(UserToken userToken, ActionListener<UserToken> listener) {
        if (clock.instant().isAfter(userToken.getExpirationTime())) {
            listener.onFailure(traceLog("validate token", userToken.getId(), expiredTokenException()));
            return;
        }
        final SecurityIndexManager tokensIndex = getTokensIndexForVersion(userToken.getVersion());
        if (tokensIndex.indexExists() == false) {
            // index doesn't exist so the token is considered invalid as we cannot verify its validity
            logger.warn("failed to validate access token because the index [" + tokensIndex.aliasName() + "] doesn't exist");
            listener.onResponse(null);
        } else {
            final GetRequest getRequest = client
                    .prepareGet(tokensIndex.aliasName(), SINGLE_MAPPING_NAME, getTokenDocumentId(userToken)).request();
            Consumer<Exception> onFailure = ex -> listener.onFailure(traceLog("check token state", userToken.getId(), ex));
            tokensIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
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
                            logger.warn("failed to get access token because index is not available");
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

    Instant getExpirationTime() {
        return clock.instant().plusSeconds(expirationDelay.getSeconds());
    }

    private Instant getCreationTime(Instant expire) {
        return expire.minusSeconds(expirationDelay.getSeconds());
    }

    private void maybeStartTokenRemover() {
        if (client.threadPool().relativeTimeInMillis() - lastExpirationRunMs > deleteInterval.getMillis()) {
            expiredTokenRemover.submit(client.threadPool());
            lastExpirationRunMs = client.threadPool().relativeTimeInMillis();
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

    String prependVersionAndEncodeAccessToken(Version version, String accessToken) throws IOException, GeneralSecurityException {
        if (version.onOrAfter(VERSION_ACCESS_TOKENS_AS_UUIDS)) {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream(MINIMUM_BASE64_BYTES);
                 OutputStream base64 = Base64.getEncoder().wrap(os);
                 StreamOutput out = new OutputStreamStreamOutput(base64)) {
                out.setVersion(version);
                Version.writeVersion(version, out);
                out.writeString(accessToken);
                return new String(os.toByteArray(), StandardCharsets.UTF_8);
            }
        } else {
            // we know that the minimum length is larger than the default of the ByteArrayOutputStream so set the size to this explicitly
            try (ByteArrayOutputStream os = new ByteArrayOutputStream(LEGACY_MINIMUM_BASE64_BYTES);
                 OutputStream base64 = Base64.getEncoder().wrap(os);
                 StreamOutput out = new OutputStreamStreamOutput(base64)) {
                out.setVersion(version);
                KeyAndCache keyAndCache = keyCache.activeKeyCache;
                Version.writeVersion(version, out);
                out.writeByteArray(keyAndCache.getSalt().bytes);
                out.writeByteArray(keyAndCache.getKeyHash().bytes);
                final byte[] initializationVector = getRandomBytes(IV_BYTES);
                out.writeByteArray(initializationVector);
                try (CipherOutputStream encryptedOutput =
                         new CipherOutputStream(out, getEncryptionCipher(initializationVector, keyAndCache, version));
                     StreamOutput encryptedStreamOutput = new OutputStreamStreamOutput(encryptedOutput)) {
                    encryptedStreamOutput.setVersion(version);
                    encryptedStreamOutput.writeString(accessToken);
                    // StreamOutput needs to be closed explicitly because it wraps CipherOutputStream
                    encryptedStreamOutput.close();
                    return new String(os.toByteArray(), StandardCharsets.UTF_8);
                }
            }
        }
    }

    static String prependVersionAndEncodeRefreshToken(Version version, String payload) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
                OutputStream base64 = Base64.getEncoder().wrap(os);
                StreamOutput out = new OutputStreamStreamOutput(base64)) {
            out.setVersion(version);
            Version.writeVersion(version, out);
            out.writeString(payload);
            return new String(os.toByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception when working with small in-memory streams", e);
        }
    }

    // public for testing
    /**
     * Unpacks a base64 encoded pair of a version tag and String payload.
     */
    public static Tuple<Version, String> unpackVersionAndPayload(String encodedPack) throws IOException {
        final byte[] bytes = encodedPack.getBytes(StandardCharsets.UTF_8);
        try (StreamInput in = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)), bytes.length)) {
            final Version version = Version.readVersion(in);
            in.setVersion(version);
            final String payload = in.readString();
            return new Tuple<Version, String>(version, payload);
        }
    }

    private void ensureEncryptionCiphersSupported() throws NoSuchPaddingException, NoSuchAlgorithmException {
        Cipher.getInstance(ENCRYPTION_CIPHER);
        SecretKeyFactory.getInstance(KDF_ALGORITHM);
    }

    // Package private for testing
    Cipher getEncryptionCipher(byte[] iv, KeyAndCache keyAndCache, Version version) throws GeneralSecurityException {
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

    /**
     * Initialize the encryption cipher using the provided password to derive the encryption key.
     */
    Cipher getEncryptionCipher(byte[] iv, String password, byte[] salt) throws GeneralSecurityException {
        SecretKey key = computeSecretKey(password.toCharArray(), salt, TOKENS_ENCRYPTION_KEY_ITERATIONS);
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, iv), secureRandom);
        cipher.updateAAD(salt);
        return cipher;
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
                    .submit(new KeyComputingRunnable(decodedSalt, keyAndCache, listener));
        }
    }

    private static String decryptTokenId(byte[] encryptedTokenId, Cipher cipher, Version version) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(encryptedTokenId);
                CipherInputStream cis = new CipherInputStream(bais, cipher);
                StreamInput decryptedInput = new InputStreamStreamInput(cis)) {
            decryptedInput.setVersion(version);
            return decryptedInput.readString();
        }
    }

    private Cipher getDecryptionCipher(byte[] iv, SecretKey key, Version version, BytesKey salt) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, iv), secureRandom);
        cipher.updateAAD(ByteBuffer.allocate(4).putInt(version.id).array());
        cipher.updateAAD(salt.bytes);
        return cipher;
    }

    /**
     * Initialize the decryption cipher using the provided password to derive the decryption key.
     */
    private Cipher getDecryptionCipher(byte[] iv, String password, byte[] salt) throws GeneralSecurityException {
        SecretKey key = computeSecretKey(password.toCharArray(), salt, TOKENS_ENCRYPTION_KEY_ITERATIONS);
        Cipher cipher = Cipher.getInstance(ENCRYPTION_CIPHER);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, iv), secureRandom);
        cipher.updateAAD(salt);
        return cipher;
    }

    byte[] getRandomBytes(int length) {
        final byte[] bytes = new byte[length];
        secureRandom.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generates a secret key based off of the provided password and salt.
     * This method can be computationally expensive.
     */
    static SecretKey computeSecretKey(char[] rawPassword, byte[] salt, int iterations)
        throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(KDF_ALGORITHM);
        PBEKeySpec keySpec = new PBEKeySpec(rawPassword, salt, iterations, 128);
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
        private final KeyAndCache keyAndCache;
        private final ActionListener<SecretKey> listener;

        KeyComputingRunnable(BytesKey decodedSalt, KeyAndCache keyAndCache, ActionListener<SecretKey> listener) {
            this.decodedSalt = decodedSalt;
            this.keyAndCache = keyAndCache;
            this.listener = listener;
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
            (left, right) -> Long.compare(right.keyAndTimestamp.getTimestamp(), left.keyAndTimestamp.getTimestamp()));
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
        logger.debug(() -> new ParameterizedMessage("refreshed keys current: {}, keys: {}", currentUsedKeyHash, keyCache.cache.keySet()));
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
                tokenMetaData, ActionListener.wrap((res) -> {
                    if (res.isAcknowledged()) {
                        TokenMetaData metaData = rotateToSpareKey();
                        clusterService.submitStateUpdateTask("publish next key to prepare key rotation",
                            new TokenMetadataPublishAction(metaData, listener));
                    } else {
                        listener.onFailure(new IllegalStateException("not acked"));
                    }
                }, listener::onFailure)));
    }

    private final class TokenMetadataPublishAction extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final TokenMetaData tokenMetaData;

        protected TokenMetadataPublishAction(TokenMetaData tokenMetaData, ActionListener<ClusterStateUpdateResponse> listener) {
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
     * Package private for testing
     */
    void clearActiveKeyCache() {
        this.keyCache.activeKeyCache.keyCache.invalidateAll();
    }

    /**
     * Package private for testing
     */
    KeyAndCache getActiveKeyCache() {
        return this.keyCache.activeKeyCache;
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
                SecretKey secretKey = computeSecretKey(keyAndTimestamp.getKey().getChars(), salt.bytes, TOKEN_SERVICE_KEY_ITERATIONS);
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
                    return computeSecretKey(closeableChars.getChars(), salt.bytes, TOKEN_SERVICE_KEY_ITERATIONS);
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

    /**
     * Contains metadata associated with the refresh token that is used for validity checks, but does not contain the proper token string.
     */
    static final class RefreshTokenStatus {

        private final boolean invalidated;
        private final String associatedUser;
        private final String associatedRealm;
        private final boolean refreshed;
        @Nullable private final Instant refreshInstant;
        @Nullable
        private final String supersedingTokens;
        @Nullable
        private final String iv;
        @Nullable
        private final String salt;
        private Version version;

        // pkg-private for testing
        RefreshTokenStatus(boolean invalidated, String associatedUser, String associatedRealm, boolean refreshed, Instant refreshInstant,
                           String supersedingTokens, String iv, String salt) {
            this.invalidated = invalidated;
            this.associatedUser = associatedUser;
            this.associatedRealm = associatedRealm;
            this.refreshed = refreshed;
            this.refreshInstant = refreshInstant;
            this.supersedingTokens = supersedingTokens;
            this.iv = iv;
            this.salt = salt;
        }

        boolean isInvalidated() {
            return invalidated;
        }

        String getAssociatedUser() {
            return associatedUser;
        }

        String getAssociatedRealm() {
            return associatedRealm;
        }

        boolean isRefreshed() {
            return refreshed;
        }

        @Nullable Instant getRefreshInstant() {
            return refreshInstant;
        }

        @Nullable
        String getSupersedingTokens() {
            return supersedingTokens;
        }

        @Nullable
        String getIv() {
            return iv;
        }

        @Nullable
        String getSalt() {
            return salt;
        }

        Version getVersion() {
            return version;
        }

        void setVersion(Version version) {
            this.version = version;
        }

        static RefreshTokenStatus fromSourceMap(Map<String, Object> refreshTokenSource) {
            final Boolean invalidated = (Boolean) refreshTokenSource.get("invalidated");
            if (invalidated == null) {
                throw new IllegalStateException("token document is missing the \"invalidated\" field");
            }
            final Map<String, Object> clientInfo = (Map<String, Object>) refreshTokenSource.get("client");
            if (clientInfo == null) {
                throw new IllegalStateException("token document is missing the \"client\" field");
            }
            if (false == clientInfo.containsKey("user")) {
                throw new IllegalStateException("token document is missing the \"client.user\" field");
            }
            final String associatedUser = (String) clientInfo.get("user");
            if (false == clientInfo.containsKey("realm")) {
                throw new IllegalStateException("token document is missing the \"client.realm\" field");
            }
            final String associatedRealm = (String) clientInfo.get("realm");
            final Boolean refreshed = (Boolean) refreshTokenSource.get("refreshed");
            if (refreshed == null) {
                throw new IllegalStateException("token document is missing the \"refreshed\" field");
            }
            final Long refreshEpochMilli = (Long) refreshTokenSource.get("refresh_time");
            final Instant refreshInstant = refreshEpochMilli == null ? null : Instant.ofEpochMilli(refreshEpochMilli);
            final String supersedingTokens = (String) refreshTokenSource.get("superseding.encrypted_tokens");
            final String iv = (String) refreshTokenSource.get("superseding.encryption_iv");
            final String salt = (String) refreshTokenSource.get("superseding.encryption_salt");
            return new RefreshTokenStatus(invalidated, associatedUser, associatedRealm, refreshed, refreshInstant, supersedingTokens,
                iv, salt);
        }
    }

}
