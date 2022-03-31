/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification.RemovalReason;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException.Feature;
import org.elasticsearch.xpack.security.support.LockingAtomicCounter;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.crypto.SecretKeyFactory;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class ApiKeyService {

    private static final Logger logger = LogManager.getLogger(ApiKeyService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ApiKeyService.class);

    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = new Setting<>(
        "xpack.security.authc.api_key.hashing.algorithm",
        "pbkdf2",
        Function.identity(),
        v -> {
            if (Hasher.getAvailableAlgoStoredHash().contains(v.toLowerCase(Locale.ROOT)) == false) {
                throw new IllegalArgumentException(
                    "Invalid algorithm: " + v + ". Valid values for password hashing are " + Hasher.getAvailableAlgoStoredHash().toString()
                );
            } else if (v.regionMatches(true, 0, "pbkdf2", 0, "pbkdf2".length())) {
                try {
                    SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalArgumentException(
                        "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the "
                            + "PBKDF2 algorithms for the [xpack.security.authc.api_key.hashing.algorithm] setting.",
                        e
                    );
                }
            }
        },
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting(
        "xpack.security.authc.api_key.delete.timeout",
        TimeValue.MINUS_ONE,
        Property.NodeScope
    );
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting(
        "xpack.security.authc.api_key.delete.interval",
        TimeValue.timeValueHours(24L),
        Property.NodeScope
    );
    public static final Setting<String> CACHE_HASH_ALGO_SETTING = Setting.simpleString(
        "xpack.security.authc.api_key.cache.hash_algo",
        "ssha256",
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.authc.api_key.cache.ttl",
        TimeValue.timeValueHours(24L),
        Property.NodeScope
    );
    public static final Setting<Integer> CACHE_MAX_KEYS_SETTING = Setting.intSetting(
        "xpack.security.authc.api_key.cache.max_keys",
        25000,
        Property.NodeScope
    );
    public static final Setting<TimeValue> DOC_CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.authc.api_key.doc_cache.ttl",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMinutes(0),
        TimeValue.timeValueMinutes(15),
        Property.NodeScope
    );

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Hasher hasher;
    private final boolean enabled;
    private final Settings settings;
    private final ExpiredApiKeysRemover expiredApiKeysRemover;
    private final TimeValue deleteInterval;
    private final Cache<String, ListenableFuture<CachedApiKeyHashResult>> apiKeyAuthCache;
    private final Hasher cacheHasher;
    private final ThreadPool threadPool;
    private final ApiKeyDocCache apiKeyDocCache;

    private volatile long lastExpirationRunMs;

    private static final long EVICTION_MONITOR_INTERVAL_SECONDS = 300L; // 5 minutes
    private static final long EVICTION_MONITOR_INTERVAL_NANOS = EVICTION_MONITOR_INTERVAL_SECONDS * 1_000_000_000L;
    private static final long EVICTION_WARNING_THRESHOLD = 15L * EVICTION_MONITOR_INTERVAL_SECONDS; // 15 eviction per sec = 4500 in 5 min
    private final AtomicLong lastEvictionCheckedAt = new AtomicLong(0);
    private final LongAdder evictionCounter = new LongAdder();

    public ApiKeyService(
        Settings settings,
        Clock clock,
        Client client,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        CacheInvalidatorRegistry cacheInvalidatorRegistry,
        ThreadPool threadPool
    ) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings);
        this.hasher = Hasher.resolve(PASSWORD_HASHING_ALGORITHM.get(settings));
        this.settings = settings;
        this.deleteInterval = DELETE_INTERVAL.get(settings);
        this.expiredApiKeysRemover = new ExpiredApiKeysRemover(settings, client);
        this.threadPool = threadPool;
        this.cacheHasher = Hasher.resolve(CACHE_HASH_ALGO_SETTING.get(settings));
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        final int maximumWeight = CACHE_MAX_KEYS_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            this.apiKeyAuthCache = CacheBuilder.<String, ListenableFuture<CachedApiKeyHashResult>>builder()
                .setExpireAfterAccess(ttl)
                .setMaximumWeight(maximumWeight)
                .removalListener(getAuthCacheRemovalListener(maximumWeight))
                .build();
            final TimeValue doc_ttl = DOC_CACHE_TTL_SETTING.get(settings);
            this.apiKeyDocCache = doc_ttl.getNanos() == 0 ? null : new ApiKeyDocCache(doc_ttl, maximumWeight);
            cacheInvalidatorRegistry.registerCacheInvalidator("api_key", new CacheInvalidatorRegistry.CacheInvalidator() {
                @Override
                public void invalidate(Collection<String> keys) {
                    if (apiKeyDocCache != null) {
                        apiKeyDocCache.invalidate(keys);
                    }
                    keys.forEach(apiKeyAuthCache::invalidate);
                }

                @Override
                public void invalidateAll() {
                    if (apiKeyDocCache != null) {
                        apiKeyDocCache.invalidateAll();
                    }
                    apiKeyAuthCache.invalidateAll();
                }
            });
        } else {
            this.apiKeyAuthCache = null;
            this.apiKeyDocCache = null;
        }
    }

    /**
     * Asynchronously creates a new API key based off of the request and authentication
     * @param authentication the authentication that this api key should be based off of
     * @param request the request to create the api key included any permission restrictions
     * @param userRoles the user's actual roles that we always enforce
     * @param listener the listener that will be used to notify of completion
     */
    public void createApiKey(
        Authentication authentication,
        CreateApiKeyRequest request,
        Set<RoleDescriptor> userRoles,
        ActionListener<CreateApiKeyResponse> listener
    ) {
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(new IllegalArgumentException("authentication must be provided"));
        } else {
            createApiKeyAndIndexIt(authentication, request, userRoles, listener);
        }
    }

    private void createApiKeyAndIndexIt(
        Authentication authentication,
        CreateApiKeyRequest request,
        Set<RoleDescriptor> roleDescriptorSet,
        ActionListener<CreateApiKeyResponse> listener
    ) {
        final Instant created = clock.instant();
        final Instant expiration = getApiKeyExpiration(created, request);
        final SecureString apiKey = UUIDs.randomBase64UUIDSecureString();
        final Version version = clusterService.state().nodes().getMinNodeVersion();

        computeHashForApiKey(apiKey, listener.delegateFailure((l, apiKeyHashChars) -> {
            try (
                XContentBuilder builder = newDocument(
                    apiKeyHashChars,
                    request.getName(),
                    authentication,
                    roleDescriptorSet,
                    created,
                    expiration,
                    request.getRoleDescriptors(),
                    version,
                    request.getMetadata()
                )
            ) {

                final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                    .setSource(builder)
                    .setId(request.getId())
                    .setRefreshPolicy(request.getRefreshPolicy())
                    .request();
                final BulkRequest bulkRequest = toSingleItemBulkRequest(indexRequest);

                securityIndex.prepareIndexIfNeededThenExecute(
                    listener::onFailure,
                    () -> executeAsyncWithOrigin(
                        client,
                        SECURITY_ORIGIN,
                        BulkAction.INSTANCE,
                        bulkRequest,
                        TransportSingleItemBulkWriteAction.<IndexResponse>wrapBulkResponse(ActionListener.wrap(indexResponse -> {
                            assert request.getId().equals(indexResponse.getId());
                            final ListenableFuture<CachedApiKeyHashResult> listenableFuture = new ListenableFuture<>();
                            listenableFuture.onResponse(new CachedApiKeyHashResult(true, apiKey));
                            apiKeyAuthCache.put(request.getId(), listenableFuture);
                            listener.onResponse(new CreateApiKeyResponse(request.getName(), request.getId(), apiKey, expiration));
                        }, listener::onFailure))
                    )
                );
            } catch (IOException e) {
                listener.onFailure(e);
            } finally {
                Arrays.fill(apiKeyHashChars, (char) 0);
            }
        }));
    }

    /**
     * package-private for testing
     */
    static XContentBuilder newDocument(
        char[] apiKeyHashChars,
        String name,
        Authentication authentication,
        Set<RoleDescriptor> userRoles,
        Instant created,
        Instant expiration,
        List<RoleDescriptor> keyRoles,
        Version version,
        @Nullable Map<String, Object> metadata
    ) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
            .field("doc_type", "api_key")
            .field("creation_time", created.toEpochMilli())
            .field("expiration_time", expiration == null ? null : expiration.toEpochMilli())
            .field("api_key_invalidated", false);

        byte[] utf8Bytes = null;
        try {
            utf8Bytes = CharArrays.toUtf8Bytes(apiKeyHashChars);
            builder.field("api_key_hash").utf8Value(utf8Bytes, 0, utf8Bytes.length);
        } finally {
            if (utf8Bytes != null) {
                Arrays.fill(utf8Bytes, (byte) 0);
            }
        }

        // Save role_descriptors
        builder.startObject("role_descriptors");
        if (keyRoles != null && keyRoles.isEmpty() == false) {
            for (RoleDescriptor descriptor : keyRoles) {
                builder.field(descriptor.getName(), (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
            }
        }
        builder.endObject();

        // Save limited_by_role_descriptors
        builder.startObject("limited_by_role_descriptors");
        for (RoleDescriptor descriptor : userRoles) {
            builder.field(descriptor.getName(), (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
        }
        builder.endObject();

        builder.field("name", name).field("version", version.id).field("metadata_flattened", metadata);
        {
            builder.startObject("creator")
                .field("principal", authentication.getUser().principal())
                .field("full_name", authentication.getUser().fullName())
                .field("email", authentication.getUser().email())
                .field("metadata", authentication.getUser().metadata())
                .field("realm", authentication.getSourceRealm().getName())
                .field("realm_type", authentication.getSourceRealm().getType());
            if (authentication.getSourceRealm().getDomain() != null) {
                builder.field("realm_domain", authentication.getSourceRealm().getDomain());
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    void tryAuthenticate(ThreadContext ctx, ApiKeyCredentials credentials, ActionListener<AuthenticationResult<User>> listener) {
        if (false == isEnabled()) {
            listener.onResponse(AuthenticationResult.notHandled());
        }
        assert credentials != null : "api key credentials must not be null";
        loadApiKeyAndValidateCredentials(ctx, credentials, ActionListener.wrap(response -> {
            credentials.close();
            listener.onResponse(response);
        }, e -> {
            credentials.close();
            listener.onFailure(e);
        }));
    }

    void loadApiKeyAndValidateCredentials(
        ThreadContext ctx,
        ApiKeyCredentials credentials,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        final String docId = credentials.getId();

        Consumer<ApiKeyDoc> validator = apiKeyDoc -> validateApiKeyCredentials(
            docId,
            apiKeyDoc,
            credentials,
            clock,
            listener.delegateResponse((l, e) -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof EsRejectedExecutionException) {
                    l.onResponse(AuthenticationResult.terminate("server is too busy to respond", e));
                } else {
                    l.onFailure(e);
                }
            })
        );

        final long invalidationCount;
        if (apiKeyDocCache != null) {
            ApiKeyDoc existing = apiKeyDocCache.get(docId);
            if (existing != null) {
                validator.accept(existing);
                return;
            }
            // API key doc not found in cache, take a record of the current invalidation count to prepare for caching
            invalidationCount = apiKeyDocCache.getInvalidationCount();
        } else {
            invalidationCount = -1;
        }

        final GetRequest getRequest = client.prepareGet(SECURITY_MAIN_ALIAS, docId).setFetchSource(true).request();
        executeAsyncWithOrigin(ctx, SECURITY_ORIGIN, getRequest, ActionListener.<GetResponse>wrap(response -> {
            if (response.isExists()) {
                final ApiKeyDoc apiKeyDoc;
                try (
                    XContentParser parser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        response.getSourceAsBytesRef(),
                        XContentType.JSON
                    )
                ) {
                    apiKeyDoc = ApiKeyDoc.fromXContent(parser);
                }
                if (invalidationCount != -1) {
                    apiKeyDocCache.putIfNoInvalidationSince(docId, apiKeyDoc, invalidationCount);
                }
                validator.accept(apiKeyDoc);
            } else {
                if (apiKeyAuthCache != null) {
                    apiKeyAuthCache.invalidate(docId);
                }
                listener.onResponse(AuthenticationResult.unsuccessful("unable to find apikey with id " + credentials.getId(), null));
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof EsRejectedExecutionException) {
                listener.onResponse(AuthenticationResult.terminate("server is too busy to respond", e));
            } else {
                listener.onResponse(
                    AuthenticationResult.unsuccessful("apikey authentication for id " + credentials.getId() + " encountered a failure", e)
                );
            }
        }), client::get);
    }

    public List<RoleDescriptor> parseRoleDescriptors(
        final String apiKeyId,
        final Map<String, Object> roleDescriptorsMap,
        RoleReference.ApiKeyRoleType roleType
    ) {
        if (roleDescriptorsMap == null) {
            return null;
        }
        final List<RoleDescriptor> roleDescriptors = roleDescriptorsMap.entrySet().stream().map(entry -> {
            final String name = entry.getKey();
            @SuppressWarnings("unchecked")
            final Map<String, Object> rdMap = (Map<String, Object>) entry.getValue();
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.map(rdMap);
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(
                            NamedXContentRegistry.EMPTY,
                            new ApiKeyLoggingDeprecationHandler(deprecationLogger, apiKeyId),
                            BytesReference.bytes(builder).streamInput()
                        )
                ) {
                    return RoleDescriptor.parse(name, parser, false);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).toList();
        return roleType == RoleReference.ApiKeyRoleType.LIMITED_BY
            ? maybeReplaceSuperuserRoleDescriptor(apiKeyId, roleDescriptors)
            : roleDescriptors;
    }

    public List<RoleDescriptor> parseRoleDescriptorsBytes(
        final String apiKeyId,
        BytesReference bytesReference,
        RoleReference.ApiKeyRoleType roleType
    ) {
        if (bytesReference == null) {
            return Collections.emptyList();
        }

        List<RoleDescriptor> roleDescriptors = new ArrayList<>();
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                new ApiKeyLoggingDeprecationHandler(deprecationLogger, apiKeyId),
                bytesReference,
                XContentType.JSON
            )
        ) {
            parser.nextToken(); // skip outer start object
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                parser.nextToken(); // role name
                String roleName = parser.currentName();
                roleDescriptors.add(RoleDescriptor.parse(roleName, parser, false));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return roleType == RoleReference.ApiKeyRoleType.LIMITED_BY
            ? maybeReplaceSuperuserRoleDescriptor(apiKeyId, roleDescriptors)
            : roleDescriptors;
    }

    // package private for tests
    static final RoleDescriptor LEGACY_SUPERUSER_ROLE_DESCRIPTOR = new RoleDescriptor(
        "superuser",
        new String[] { "all" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(true).build() },
        new RoleDescriptor.ApplicationResourcePrivileges[] {
            RoleDescriptor.ApplicationResourcePrivileges.builder().application("*").privileges("*").resources("*").build() },
        null,
        new String[] { "*" },
        MetadataUtils.DEFAULT_RESERVED_METADATA,
        Collections.emptyMap()
    );

    // This method should only be called to replace the superuser role descriptor for the limited-by roles of an API Key.
    // We do not replace assigned roles because they are created explicitly by users.
    // Before #82049, it is possible to specify a role descriptor for API keys that is identical to the builtin superuser role
    // (including the _reserved metadata field).
    private static List<RoleDescriptor> maybeReplaceSuperuserRoleDescriptor(String apiKeyId, List<RoleDescriptor> roleDescriptors) {
        // Scan through all the roles because superuser can be one of the roles that a user has. Unlike building the Role object,
        // capturing role descriptors does not preempt for superuser.
        return roleDescriptors.stream().map(rd -> {
            // Since we are only replacing limited-by roles and all limited-by roles are looked up with role providers,
            // it is technically possible to just check the name of superuser and the _reserved metadata field.
            // But the gain is not much since role resolving is cached and comparing the whole role descriptor is still safer.
            if (rd.equals(LEGACY_SUPERUSER_ROLE_DESCRIPTOR)) {
                logger.debug("replacing superuser role for API key [{}]", apiKeyId);
                return ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR;
            }
            return rd;
        }).toList();
    }

    /**
     * Validates the ApiKey using the source map
     * @param docId the identifier of the document that was retrieved from the security index
     * @param apiKeyDoc the partially deserialized API key document
     * @param credentials the credentials provided by the user
     * @param listener the listener to notify after verification
     */
    void validateApiKeyCredentials(
        String docId,
        ApiKeyDoc apiKeyDoc,
        ApiKeyCredentials credentials,
        Clock clock,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        if ("api_key".equals(apiKeyDoc.docType) == false) {
            listener.onResponse(
                AuthenticationResult.unsuccessful("document [" + docId + "] is [" + apiKeyDoc.docType + "] not an api key", null)
            );
        } else if (apiKeyDoc.invalidated == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("api key document is missing invalidated field", null));
        } else if (apiKeyDoc.invalidated) {
            if (apiKeyAuthCache != null) {
                apiKeyAuthCache.invalidate(docId);
            }
            listener.onResponse(AuthenticationResult.unsuccessful("api key has been invalidated", null));
        } else {
            if (apiKeyDoc.hash == null) {
                throw new IllegalStateException("api key hash is missing");
            }

            if (apiKeyAuthCache != null) {
                final AtomicBoolean valueAlreadyInCache = new AtomicBoolean(true);
                final ListenableFuture<CachedApiKeyHashResult> listenableCacheEntry;
                try {
                    listenableCacheEntry = apiKeyAuthCache.computeIfAbsent(credentials.getId(), k -> {
                        valueAlreadyInCache.set(false);
                        return new ListenableFuture<>();
                    });
                } catch (ExecutionException e) {
                    listener.onFailure(e);
                    return;
                }

                if (valueAlreadyInCache.get()) {
                    listenableCacheEntry.addListener(ActionListener.wrap(result -> {
                        if (result.success) {
                            if (result.verify(credentials.getKey())) {
                                // move on
                                validateApiKeyExpiration(apiKeyDoc, credentials, clock, listener);
                            } else {
                                listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                            }
                        } else if (result.verify(credentials.getKey())) { // same key, pass the same result
                            listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                        } else {
                            apiKeyAuthCache.invalidate(credentials.getId(), listenableCacheEntry);
                            validateApiKeyCredentials(docId, apiKeyDoc, credentials, clock, listener);
                        }
                    }, listener::onFailure), threadPool.generic(), threadPool.getThreadContext());
                } else {
                    verifyKeyAgainstHash(apiKeyDoc.hash, credentials, ActionListener.wrap(verified -> {
                        listenableCacheEntry.onResponse(new CachedApiKeyHashResult(verified, credentials.getKey()));
                        if (verified) {
                            // move on
                            validateApiKeyExpiration(apiKeyDoc, credentials, clock, listener);
                        } else {
                            listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                        }
                    }, listener::onFailure));
                }
            } else {
                verifyKeyAgainstHash(apiKeyDoc.hash, credentials, ActionListener.wrap(verified -> {
                    if (verified) {
                        // move on
                        validateApiKeyExpiration(apiKeyDoc, credentials, clock, listener);
                    } else {
                        listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                    }
                }, listener::onFailure));
            }
        }
    }

    // pkg private for testing
    CachedApiKeyHashResult getFromCache(String id) {
        return apiKeyAuthCache == null ? null : FutureUtils.get(apiKeyAuthCache.get(id), 0L, TimeUnit.MILLISECONDS);
    }

    // pkg private for testing
    Cache<String, ListenableFuture<CachedApiKeyHashResult>> getApiKeyAuthCache() {
        return apiKeyAuthCache;
    }

    // pkg private for testing
    Cache<String, CachedApiKeyDoc> getDocCache() {
        return apiKeyDocCache == null ? null : apiKeyDocCache.docCache;
    }

    // pkg private for testing
    Cache<String, BytesReference> getRoleDescriptorsBytesCache() {
        return apiKeyDocCache == null ? null : apiKeyDocCache.roleDescriptorsBytesCache;
    }

    // package-private for testing
    static void validateApiKeyExpiration(
        ApiKeyDoc apiKeyDoc,
        ApiKeyCredentials credentials,
        Clock clock,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        if (apiKeyDoc.expirationTime == -1 || Instant.ofEpochMilli(apiKeyDoc.expirationTime).isAfter(clock.instant())) {
            final String principal = Objects.requireNonNull((String) apiKeyDoc.creator.get("principal"));
            final String fullName = (String) apiKeyDoc.creator.get("full_name");
            final String email = (String) apiKeyDoc.creator.get("email");
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) apiKeyDoc.creator.get("metadata");
            final User apiKeyUser = new User(principal, Strings.EMPTY_ARRAY, fullName, email, metadata, true);
            final Map<String, Object> authResultMetadata = new HashMap<>();
            authResultMetadata.put(AuthenticationField.API_KEY_CREATOR_REALM_NAME, apiKeyDoc.creator.get("realm"));
            authResultMetadata.put(AuthenticationField.API_KEY_CREATOR_REALM_TYPE, apiKeyDoc.creator.get("realm_type"));
            authResultMetadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, apiKeyDoc.roleDescriptorsBytes);
            authResultMetadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, apiKeyDoc.limitedByRoleDescriptorsBytes);
            authResultMetadata.put(AuthenticationField.API_KEY_ID_KEY, credentials.getId());
            authResultMetadata.put(AuthenticationField.API_KEY_NAME_KEY, apiKeyDoc.name);
            if (apiKeyDoc.metadataFlattened != null) {
                authResultMetadata.put(AuthenticationField.API_KEY_METADATA_KEY, apiKeyDoc.metadataFlattened);
            }
            listener.onResponse(AuthenticationResult.success(apiKeyUser, authResultMetadata));
        } else {
            listener.onResponse(AuthenticationResult.unsuccessful("api key is expired", null));
        }
    }

    /**
     * Gets the API Key from the <code>Authorization</code> header if the header begins with
     * <code>ApiKey </code>
     */
    ApiKeyCredentials getCredentialsFromHeader(ThreadContext threadContext) {
        if (false == isEnabled()) {
            return null;
        }
        final SecureString apiKeyString = Authenticator.extractCredentialFromAuthorizationHeader(threadContext, "ApiKey");
        if (apiKeyString != null) {
            final byte[] decodedApiKeyCredBytes = Base64.getDecoder().decode(CharArrays.toUtf8Bytes(apiKeyString.getChars()));
            char[] apiKeyCredChars = null;
            try {
                apiKeyCredChars = CharArrays.utf8BytesToChars(decodedApiKeyCredBytes);
                int colonIndex = -1;
                for (int i = 0; i < apiKeyCredChars.length; i++) {
                    if (apiKeyCredChars[i] == ':') {
                        colonIndex = i;
                        break;
                    }
                }

                if (colonIndex < 1) {
                    throw new IllegalArgumentException("invalid ApiKey value");
                }
                return new ApiKeyCredentials(
                    new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, colonIndex + 1, apiKeyCredChars.length))
                );
            } finally {
                if (apiKeyCredChars != null) {
                    Arrays.fill(apiKeyCredChars, (char) 0);
                }
            }
        }
        return null;
    }

    void computeHashForApiKey(SecureString apiKey, ActionListener<char[]> listener) {
        threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME).execute(ActionRunnable.supply(listener, () -> hasher.hash(apiKey)));
    }

    // Protected instance method so this can be mocked
    protected void verifyKeyAgainstHash(String apiKeyHash, ApiKeyCredentials credentials, ActionListener<Boolean> listener) {
        threadPool.executor(SECURITY_CRYPTO_THREAD_POOL_NAME).execute(ActionRunnable.supply(listener, () -> {
            Hasher hasher = Hasher.resolveFromHash(apiKeyHash.toCharArray());
            final char[] apiKeyHashChars = apiKeyHash.toCharArray();
            try {
                return hasher.verify(credentials.getKey(), apiKeyHashChars);
            } finally {
                Arrays.fill(apiKeyHashChars, (char) 0);
            }
        }));
    }

    private static Instant getApiKeyExpiration(Instant now, CreateApiKeyRequest request) {
        if (request.getExpiration() != null) {
            return now.plusSeconds(request.getExpiration().getSeconds());
        } else {
            return null;
        }
    }

    private boolean isEnabled() {
        return enabled;
    }

    public void ensureEnabled() {
        if (enabled == false) {
            throw new FeatureNotEnabledException(Feature.API_KEY_SERVICE, "api keys are not enabled");
        }
    }

    // public class for testing
    public static final class ApiKeyCredentials implements AuthenticationToken, Closeable {
        private final String id;
        private final SecureString key;

        public ApiKeyCredentials(String id, SecureString key) {
            this.id = id;
            this.key = key;
        }

        String getId() {
            return id;
        }

        SecureString getKey() {
            return key;
        }

        @Override
        public void close() {
            key.close();
        }

        @Override
        public String principal() {
            return id;
        }

        @Override
        public Object credentials() {
            return key;
        }

        @Override
        public void clearCredentials() {
            close();
        }
    }

    private static class ApiKeyLoggingDeprecationHandler implements DeprecationHandler {

        private final DeprecationLogger deprecationLogger;
        private final String apiKeyId;

        private ApiKeyLoggingDeprecationHandler(DeprecationLogger logger, String apiKeyId) {
            this.deprecationLogger = logger;
            this.apiKeyId = apiKeyId;
        }

        @Override
        public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {
            String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
            deprecationLogger.warn(
                DeprecationCategory.API,
                "api_key_field",
                "{}Deprecated field [{}] used in api key [{}], expected [{}] instead",
                prefix,
                oldName,
                apiKeyId,
                currentName
            );
        }

        @Override
        public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {
            String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
            deprecationLogger.warn(
                DeprecationCategory.API,
                "api_key_field",
                "{}Deprecated field [{}] used in api key [{}], replaced by [{}]",
                prefix,
                oldName,
                apiKeyId,
                replacedName
            );
        }

        @Override
        public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {
            String prefix = parserName == null ? "" : "[" + parserName + "][" + location.get() + "] ";
            deprecationLogger.warn(
                DeprecationCategory.API,
                "api_key_field",
                "{}Deprecated field [{}] used in api key [{}], which is unused and will be removed entirely",
                prefix,
                removedName,
                apiKeyId
            );
        }
    }

    /**
     * Invalidate API keys for given realm, user name, API key name and id.
     * @param realmNames realm names
     * @param username user name
     * @param apiKeyName API key name
     * @param apiKeyIds API key ids
     * @param invalidateListener listener for {@link InvalidateApiKeyResponse}
     */
    public void invalidateApiKeys(
        String[] realmNames,
        String username,
        String apiKeyName,
        String[] apiKeyIds,
        ActionListener<InvalidateApiKeyResponse> invalidateListener
    ) {
        ensureEnabled();
        if ((realmNames == null || realmNames.length == 0)
            && Strings.hasText(username) == false
            && Strings.hasText(apiKeyName) == false
            && (apiKeyIds == null || apiKeyIds.length == 0)) {
            logger.trace("none of the parameters [api key id, api key name, username, realm name] were specified for invalidation");
            invalidateListener.onFailure(
                new IllegalArgumentException("One of [api key id, api key name, username, realm name] must be specified")
            );
        } else {
            findApiKeysForUserRealmApiKeyIdAndNameCombination(
                realmNames,
                username,
                apiKeyName,
                apiKeyIds,
                true,
                false,
                ActionListener.wrap(apiKeys -> {
                    if (apiKeys.isEmpty()) {
                        logger.debug(
                            "No active api keys to invalidate for realms {}, username [{}], api key name [{}] and api key ids {}",
                            Arrays.toString(realmNames),
                            username,
                            apiKeyName,
                            Arrays.toString(apiKeyIds)
                        );
                        invalidateListener.onResponse(InvalidateApiKeyResponse.emptyResponse());
                    } else {
                        invalidateAllApiKeys(
                            apiKeys.stream().map(apiKey -> apiKey.getId()).collect(Collectors.toSet()),
                            invalidateListener
                        );
                    }
                }, invalidateListener::onFailure)
            );
        }
    }

    private void invalidateAllApiKeys(Collection<String> apiKeyIds, ActionListener<InvalidateApiKeyResponse> invalidateListener) {
        indexInvalidation(apiKeyIds, invalidateListener, null);
    }

    private void findApiKeys(
        final BoolQueryBuilder boolQuery,
        boolean filterOutInvalidatedKeys,
        boolean filterOutExpiredKeys,
        ActionListener<Collection<ApiKey>> listener
    ) {
        if (filterOutInvalidatedKeys) {
            boolQuery.filter(QueryBuilders.termQuery("api_key_invalidated", false));
        }
        if (filterOutExpiredKeys) {
            final BoolQueryBuilder expiredQuery = QueryBuilders.boolQuery();
            expiredQuery.should(QueryBuilders.rangeQuery("expiration_time").gt(Instant.now().toEpochMilli()));
            expiredQuery.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("expiration_time")));
            boolQuery.filter(expiredQuery);
        }
        final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                .setQuery(boolQuery)
                .setVersion(false)
                .setSize(1000)
                .setFetchSource(true)
                .request();
            securityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> ScrollHelper.fetchAllByEntity(
                    client,
                    request,
                    new ContextPreservingActionListener<>(supplier, listener),
                    ApiKeyService::convertSearchHitToApiKeyInfo
                )
            );
        }
    }

    public static QueryBuilder filterForRealmNames(String[] realmNames) {
        if (realmNames == null || realmNames.length == 0) {
            return null;
        }
        if (realmNames.length == 1) {
            return QueryBuilders.termQuery("creator.realm", realmNames[0]);
        } else {
            final BoolQueryBuilder realmsQuery = QueryBuilders.boolQuery();
            for (String realmName : realmNames) {
                realmsQuery.should(QueryBuilders.termQuery("creator.realm", realmName));
            }
            realmsQuery.minimumShouldMatch(1);
            return realmsQuery;
        }
    }

    private void findApiKeysForUserRealmApiKeyIdAndNameCombination(
        String[] realmNames,
        String userName,
        String apiKeyName,
        String[] apiKeyIds,
        boolean filterOutInvalidatedKeys,
        boolean filterOutExpiredKeys,
        ActionListener<Collection<ApiKey>> listener
    ) {
        final SecurityIndexManager frozenSecurityIndex = securityIndex.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", "api_key"));
            QueryBuilder realmsQuery = filterForRealmNames(realmNames);
            if (realmsQuery != null) {
                boolQuery.filter(realmsQuery);
            }
            if (Strings.hasText(userName)) {
                boolQuery.filter(QueryBuilders.termQuery("creator.principal", userName));
            }
            if (Strings.hasText(apiKeyName) && "*".equals(apiKeyName) == false) {
                if (apiKeyName.endsWith("*")) {
                    boolQuery.filter(QueryBuilders.prefixQuery("name", apiKeyName.substring(0, apiKeyName.length() - 1)));
                } else {
                    boolQuery.filter(QueryBuilders.termQuery("name", apiKeyName));
                }
            }
            if (apiKeyIds != null && apiKeyIds.length > 0) {
                boolQuery.filter(QueryBuilders.idsQuery().addIds(apiKeyIds));
            }

            findApiKeys(boolQuery, filterOutInvalidatedKeys, filterOutExpiredKeys, listener);
        }
    }

    /**
     * Performs the actual invalidation of a collection of api keys
     *
     * @param apiKeyIds       the api keys to invalidate
     * @param listener        the listener to notify upon completion
     * @param previousResult  if this not the initial attempt for invalidation, it contains the result of invalidating
     *                        api keys up to the point of the retry. This result is added to the result of the current attempt
     */
    private void indexInvalidation(
        Collection<String> apiKeyIds,
        ActionListener<InvalidateApiKeyResponse> listener,
        @Nullable InvalidateApiKeyResponse previousResult
    ) {
        maybeStartApiKeyRemover();
        if (apiKeyIds.isEmpty()) {
            listener.onFailure(new ElasticsearchSecurityException("No api key ids provided for invalidation"));
        } else {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (String apiKeyId : apiKeyIds) {
                UpdateRequest request = client.prepareUpdate(SECURITY_MAIN_ALIAS, apiKeyId)
                    .setDoc(Collections.singletonMap("api_key_invalidated", true))
                    .request();
                bulkRequestBuilder.add(request);
            }
            bulkRequestBuilder.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
            securityIndex.prepareIndexIfNeededThenExecute(
                ex -> listener.onFailure(traceLog("prepare security index", ex)),
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    bulkRequestBuilder.request(),
                    ActionListener.<BulkResponse>wrap(bulkResponse -> {
                        ArrayList<ElasticsearchException> failedRequestResponses = new ArrayList<>();
                        ArrayList<String> previouslyInvalidated = new ArrayList<>();
                        ArrayList<String> invalidated = new ArrayList<>();
                        if (null != previousResult) {
                            failedRequestResponses.addAll((previousResult.getErrors()));
                            previouslyInvalidated.addAll(previousResult.getPreviouslyInvalidatedApiKeys());
                            invalidated.addAll(previousResult.getInvalidatedApiKeys());
                        }
                        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                            if (bulkItemResponse.isFailed()) {
                                Throwable cause = bulkItemResponse.getFailure().getCause();
                                final String failedApiKeyId = bulkItemResponse.getFailure().getId();
                                traceLog("invalidate api key", failedApiKeyId, cause);
                                failedRequestResponses.add(new ElasticsearchException("Error invalidating api key", cause));
                            } else {
                                UpdateResponse updateResponse = bulkItemResponse.getResponse();
                                if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                                    logger.debug("Invalidated api key for doc [{}]", updateResponse.getId());
                                    invalidated.add(updateResponse.getId());
                                } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                                    previouslyInvalidated.add(updateResponse.getId());
                                }
                            }
                        }
                        InvalidateApiKeyResponse result = new InvalidateApiKeyResponse(
                            invalidated,
                            previouslyInvalidated,
                            failedRequestResponses
                        );
                        clearCache(result, listener);
                    }, e -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        traceLog("invalidate api keys", cause);
                        listener.onFailure(e);
                    }),
                    client::bulk
                )
            );
        }
    }

    private void clearCache(InvalidateApiKeyResponse result, ActionListener<InvalidateApiKeyResponse> listener) {
        final ClearSecurityCacheRequest clearApiKeyCacheRequest = new ClearSecurityCacheRequest().cacheName("api_key")
            .keys(result.getInvalidatedApiKeys().toArray(String[]::new));
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearSecurityCacheAction.INSTANCE, clearApiKeyCacheRequest, new ActionListener<>() {
            @Override
            public void onResponse(ClearSecurityCacheResponse nodes) {
                listener.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unable to clear API key cache", e);
                listener.onFailure(new ElasticsearchException("clearing the API key cache failed; please clear the caches manually", e));
            }
        });
    }

    /**
     * Logs an exception concerning a specific api key at TRACE level (if enabled)
     */
    private static <E extends Throwable> E traceLog(String action, String identifier, E exception) {
        if (logger.isTraceEnabled()) {
            if (exception instanceof final ElasticsearchException esEx) {
                final Object detail = esEx.getHeader("error_description");
                if (detail != null) {
                    logger.trace(() -> new ParameterizedMessage("Failure in [{}] for id [{}] - [{}]", action, identifier, detail), esEx);
                } else {
                    logger.trace(() -> new ParameterizedMessage("Failure in [{}] for id [{}]", action, identifier), esEx);
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
    private static <E extends Throwable> E traceLog(String action, E exception) {
        if (logger.isTraceEnabled()) {
            if (exception instanceof final ElasticsearchException esEx) {
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

    // pkg scoped for testing
    boolean isExpirationInProgress() {
        return expiredApiKeysRemover.isExpirationInProgress();
    }

    // pkg scoped for testing
    long lastTimeWhenApiKeysRemoverWasTriggered() {
        return lastExpirationRunMs;
    }

    private void maybeStartApiKeyRemover() {
        if (securityIndex.isAvailable()) {
            if (client.threadPool().relativeTimeInMillis() - lastExpirationRunMs > deleteInterval.getMillis()) {
                expiredApiKeysRemover.submit(client.threadPool());
                lastExpirationRunMs = client.threadPool().relativeTimeInMillis();
            }
        }
    }

    /**
     * Get API key information for given realm, user, API key name and id combination
     * @param realmNames realm names
     * @param username user name
     * @param apiKeyName API key name
     * @param apiKeyIds API key ids
     * @param listener listener for {@link GetApiKeyResponse}
     */
    public void getApiKeys(
        String[] realmNames,
        String username,
        String apiKeyName,
        String[] apiKeyIds,
        ActionListener<GetApiKeyResponse> listener
    ) {
        ensureEnabled();
        findApiKeysForUserRealmApiKeyIdAndNameCombination(
            realmNames,
            username,
            apiKeyName,
            apiKeyIds,
            false,
            false,
            ActionListener.wrap(apiKeyInfos -> {
                if (apiKeyInfos.isEmpty()) {
                    logger.debug(
                        "No active api keys found for realms {}, user [{}], api key name [{}] and api key ids {}",
                        Arrays.toString(realmNames),
                        username,
                        apiKeyName,
                        Arrays.toString(apiKeyIds)
                    );
                    listener.onResponse(GetApiKeyResponse.emptyResponse());
                } else {
                    listener.onResponse(new GetApiKeyResponse(apiKeyInfos));
                }
            }, listener::onFailure)
        );
    }

    public void queryApiKeys(SearchRequest searchRequest, ActionListener<QueryApiKeyResponse> listener) {
        ensureEnabled();

        final SecurityIndexManager frozenSecurityIndex = securityIndex.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(QueryApiKeyResponse.emptyResponse());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            securityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    SearchAction.INSTANCE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        final long total = searchResponse.getHits().getTotalHits().value;
                        if (total == 0) {
                            logger.debug("No api keys found for query [{}]", searchRequest.source().query());
                            listener.onResponse(QueryApiKeyResponse.emptyResponse());
                            return;
                        }
                        final List<QueryApiKeyResponse.Item> apiKeyItem = Arrays.stream(searchResponse.getHits().getHits())
                            .map(ApiKeyService::convertSearchHitToQueryItem)
                            .toList();
                        listener.onResponse(new QueryApiKeyResponse(total, apiKeyItem));
                    }, listener::onFailure)
                )
            );
        }
    }

    private static QueryApiKeyResponse.Item convertSearchHitToQueryItem(SearchHit hit) {
        return new QueryApiKeyResponse.Item(convertSearchHitToApiKeyInfo(hit), hit.getSortValues());
    }

    private static ApiKey convertSearchHitToApiKeyInfo(SearchHit hit) {
        Map<String, Object> source = hit.getSourceAsMap();
        String name = (String) source.get("name");
        String id = hit.getId();
        Long creation = (Long) source.get("creation_time");
        Long expiration = (Long) source.get("expiration_time");
        Boolean invalidated = (Boolean) source.get("api_key_invalidated");
        @SuppressWarnings("unchecked")
        String username = (String) ((Map<String, Object>) source.get("creator")).get("principal");
        @SuppressWarnings("unchecked")
        String realm = (String) ((Map<String, Object>) source.get("creator")).get("realm");
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) source.get("metadata_flattened");

        return new ApiKey(
            name,
            id,
            Instant.ofEpochMilli(creation),
            (expiration != null) ? Instant.ofEpochMilli(expiration) : null,
            invalidated,
            username,
            realm,
            metadata
        );
    }

    private RemovalListener<String, ListenableFuture<CachedApiKeyHashResult>> getAuthCacheRemovalListener(int maximumWeight) {
        return notification -> {
            if (RemovalReason.EVICTED == notification.getRemovalReason() && getApiKeyAuthCache().count() >= maximumWeight) {
                evictionCounter.increment();
                logger.trace(
                    "API key with ID [{}] was evicted from the authentication cache, " + "possibly due to cache size limit",
                    notification.getKey()
                );
                final long last = lastEvictionCheckedAt.get();
                final long now = System.nanoTime();
                if (now - last >= EVICTION_MONITOR_INTERVAL_NANOS && lastEvictionCheckedAt.compareAndSet(last, now)) {
                    final long sum = evictionCounter.sum();
                    evictionCounter.add(-sum); // reset by decrease
                    if (sum >= EVICTION_WARNING_THRESHOLD) {
                        logger.warn(
                            "Possible thrashing for API key authentication cache, "
                                + "[{}] eviction due to cache size within last [{}] seconds",
                            sum,
                            EVICTION_MONITOR_INTERVAL_SECONDS
                        );
                    }
                }
            }
        };
    }

    // package private for test
    LongAdder getEvictionCounter() {
        return evictionCounter;
    }

    // package private for test
    AtomicLong getLastEvictionCheckedAt() {
        return lastEvictionCheckedAt;
    }

    /**
     * Returns realm name of the owner user of an API key if the effective user is an API Key.
     * If the effective user is not an API key, it just returns the source realm name.
     *
     * @param authentication {@link Authentication}
     * @return realm name
     */
    public static String getCreatorRealmName(final Authentication authentication) {
        if (authentication.isApiKey()) {
            return (String) authentication.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME);
        } else {
            return authentication.getSourceRealm().getName();
        }
    }

    /** Returns the realm names that the username can access resources across.
     */
    public static String[] getOwnersRealmNames(Authentication authentication) {
        if (authentication.isApiKey()) {
            return new String[] { (String) authentication.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME) };
        } else {
            RealmDomain domain = authentication.getSourceRealm().getDomain();
            if (domain != null) {
                return domain.realms().stream().map(realmIdentifier -> realmIdentifier.getName()).toArray(String[]::new);
            } else {
                return new String[] { authentication.getSourceRealm().getName() };
            }
        }
    }

    /**
     * Returns realm type of the owner user of an API key if the effective user is an API Key.
     * If the effective user is not an API key, it just returns the source realm type.
     *
     * @param authentication {@link Authentication}
     * @return realm type
     */
    public static String getCreatorRealmType(final Authentication authentication) {
        if (authentication.isApiKey()) {
            return (String) authentication.getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_TYPE);
        } else {
            return authentication.getSourceRealm().getType();
        }
    }

    /**
     * If the authentication has type of api_key, returns the metadata associated to the
     * API key.
     * @param authentication {@link Authentication}
     * @return A map for the metadata or an empty map if no metadata is found.
     */
    public static Map<String, Object> getApiKeyMetadata(Authentication authentication) {
        if (false == authentication.isAuthenticatedAsApiKey()) {
            throw new IllegalArgumentException(
                "authentication realm must be ["
                    + AuthenticationField.API_KEY_REALM_TYPE
                    + "], got ["
                    + authentication.getAuthenticatedBy().getType()
                    + "]"
            );
        }
        final Object apiKeyMetadata = authentication.getMetadata().get(AuthenticationField.API_KEY_METADATA_KEY);
        if (apiKeyMetadata != null) {
            final Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
                (BytesReference) apiKeyMetadata,
                false,
                XContentType.JSON
            );
            return tuple.v2();
        } else {
            return Map.of();
        }
    }

    final class CachedApiKeyHashResult {
        final boolean success;
        final char[] hash;

        CachedApiKeyHashResult(boolean success, SecureString apiKey) {
            this.success = success;
            this.hash = cacheHasher.hash(apiKey);
        }

        boolean verify(SecureString password) {
            return hash != null && cacheHasher.verify(password, hash);
        }
    }

    public static final class ApiKeyDoc {

        private static final BytesReference NULL_BYTES = new BytesArray("null");
        static final InstantiatingObjectParser<ApiKeyDoc, Void> PARSER;
        static {
            InstantiatingObjectParser.Builder<ApiKeyDoc, Void> builder = InstantiatingObjectParser.builder(
                "api_key_doc",
                true,
                ApiKeyDoc.class
            );
            builder.declareString(constructorArg(), new ParseField("doc_type"));
            builder.declareLong(constructorArg(), new ParseField("creation_time"));
            builder.declareLongOrNull(constructorArg(), -1, new ParseField("expiration_time"));
            builder.declareBoolean(constructorArg(), new ParseField("api_key_invalidated"));
            builder.declareString(constructorArg(), new ParseField("api_key_hash"));
            builder.declareStringOrNull(optionalConstructorArg(), new ParseField("name"));
            builder.declareInt(constructorArg(), new ParseField("version"));
            ObjectParserHelper.declareRawObject(builder, constructorArg(), new ParseField("role_descriptors"));
            ObjectParserHelper.declareRawObject(builder, constructorArg(), new ParseField("limited_by_role_descriptors"));
            builder.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("creator"));
            ObjectParserHelper.declareRawObjectOrNull(builder, optionalConstructorArg(), new ParseField("metadata_flattened"));
            PARSER = builder.build();
        }

        final String docType;
        final long creationTime;
        final long expirationTime;
        final Boolean invalidated;
        final String hash;
        @Nullable
        final String name;
        final int version;
        final BytesReference roleDescriptorsBytes;
        final BytesReference limitedByRoleDescriptorsBytes;
        final Map<String, Object> creator;
        @Nullable
        final BytesReference metadataFlattened;

        public ApiKeyDoc(
            String docType,
            long creationTime,
            long expirationTime,
            Boolean invalidated,
            String hash,
            @Nullable String name,
            int version,
            BytesReference roleDescriptorsBytes,
            BytesReference limitedByRoleDescriptorsBytes,
            Map<String, Object> creator,
            @Nullable BytesReference metadataFlattened
        ) {

            this.docType = docType;
            this.creationTime = creationTime;
            this.expirationTime = expirationTime;
            this.invalidated = invalidated;
            this.hash = hash;
            this.name = name;
            this.version = version;
            this.roleDescriptorsBytes = roleDescriptorsBytes;
            this.limitedByRoleDescriptorsBytes = limitedByRoleDescriptorsBytes;
            this.creator = creator;
            this.metadataFlattened = NULL_BYTES.equals(metadataFlattened) ? null : metadataFlattened;
        }

        public CachedApiKeyDoc toCachedApiKeyDoc() {
            final MessageDigest digest = MessageDigests.sha256();
            final String roleDescriptorsHash = MessageDigests.toHexString(MessageDigests.digest(roleDescriptorsBytes, digest));
            digest.reset();
            final String limitedByRoleDescriptorsHash = MessageDigests.toHexString(
                MessageDigests.digest(limitedByRoleDescriptorsBytes, digest)
            );
            return new CachedApiKeyDoc(
                creationTime,
                expirationTime,
                invalidated,
                hash,
                name,
                version,
                creator,
                roleDescriptorsHash,
                limitedByRoleDescriptorsHash,
                metadataFlattened
            );
        }

        static ApiKeyDoc fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    /**
     * A cached version of the {@link ApiKeyDoc}. The main difference is that the role descriptors
     * are replaced by their hashes. The actual values are stored in a separate role descriptor cache,
     * so that duplicate role descriptors are cached only once (and therefore consume less memory).
     */
    public static final class CachedApiKeyDoc {
        final long creationTime;
        final long expirationTime;
        final Boolean invalidated;
        final String hash;
        final String name;
        final int version;
        final Map<String, Object> creator;
        final String roleDescriptorsHash;
        final String limitedByRoleDescriptorsHash;
        @Nullable
        final BytesReference metadataFlattened;

        public CachedApiKeyDoc(
            long creationTime,
            long expirationTime,
            Boolean invalidated,
            String hash,
            String name,
            int version,
            Map<String, Object> creator,
            String roleDescriptorsHash,
            String limitedByRoleDescriptorsHash,
            @Nullable BytesReference metadataFlattened
        ) {
            this.creationTime = creationTime;
            this.expirationTime = expirationTime;
            this.invalidated = invalidated;
            this.hash = hash;
            this.name = name;
            this.version = version;
            this.creator = creator;
            this.roleDescriptorsHash = roleDescriptorsHash;
            this.limitedByRoleDescriptorsHash = limitedByRoleDescriptorsHash;
            this.metadataFlattened = metadataFlattened;
        }

        public ApiKeyDoc toApiKeyDoc(BytesReference roleDescriptorsBytes, BytesReference limitedByRoleDescriptorsBytes) {
            return new ApiKeyDoc(
                "api_key",
                creationTime,
                expirationTime,
                invalidated,
                hash,
                name,
                version,
                roleDescriptorsBytes,
                limitedByRoleDescriptorsBytes,
                creator,
                metadataFlattened
            );
        }
    }

    private static final class ApiKeyDocCache {
        private final Cache<String, ApiKeyService.CachedApiKeyDoc> docCache;
        private final Cache<String, BytesReference> roleDescriptorsBytesCache;
        private final LockingAtomicCounter lockingAtomicCounter;

        ApiKeyDocCache(TimeValue ttl, int maximumWeight) {
            this.docCache = CacheBuilder.<String, ApiKeyService.CachedApiKeyDoc>builder()
                .setMaximumWeight(maximumWeight)
                .setExpireAfterWrite(ttl)
                .build();
            // We don't use the doc TTL because that TTL is very low to avoid the risk of
            // caching an invalidated API key. But role descriptors are immutable and may be shared between
            // multiple API keys, so we cache for longer and rely on the weight to manage the cache size.
            this.roleDescriptorsBytesCache = CacheBuilder.<String, BytesReference>builder()
                .setExpireAfterAccess(TimeValue.timeValueHours(1))
                .setMaximumWeight(maximumWeight * 2L)
                .build();
            this.lockingAtomicCounter = new LockingAtomicCounter();
        }

        public ApiKeyDoc get(String docId) {
            ApiKeyService.CachedApiKeyDoc existing = docCache.get(docId);
            if (existing != null) {
                final BytesReference roleDescriptorsBytes = roleDescriptorsBytesCache.get(existing.roleDescriptorsHash);
                final BytesReference limitedByRoleDescriptorsBytes = roleDescriptorsBytesCache.get(existing.limitedByRoleDescriptorsHash);
                if (roleDescriptorsBytes != null && limitedByRoleDescriptorsBytes != null) {
                    return existing.toApiKeyDoc(roleDescriptorsBytes, limitedByRoleDescriptorsBytes);
                }
            }
            return null;
        }

        public long getInvalidationCount() {
            return lockingAtomicCounter.get();
        }

        public void putIfNoInvalidationSince(String docId, ApiKeyDoc apiKeyDoc, long invalidationCount) {
            final CachedApiKeyDoc cachedApiKeyDoc = apiKeyDoc.toCachedApiKeyDoc();
            lockingAtomicCounter.compareAndRun(invalidationCount, () -> {
                docCache.put(docId, cachedApiKeyDoc);
                try {
                    roleDescriptorsBytesCache.computeIfAbsent(cachedApiKeyDoc.roleDescriptorsHash, k -> apiKeyDoc.roleDescriptorsBytes);
                    roleDescriptorsBytesCache.computeIfAbsent(
                        cachedApiKeyDoc.limitedByRoleDescriptorsHash,
                        k -> apiKeyDoc.limitedByRoleDescriptorsBytes
                    );
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public void invalidate(Collection<String> docIds) {
            lockingAtomicCounter.increment();
            logger.debug("Invalidating API key doc cache with ids: [{}]", Strings.collectionToCommaDelimitedString(docIds));
            docIds.forEach(docCache::invalidate);
        }

        public void invalidateAll() {
            lockingAtomicCounter.increment();
            logger.debug("Invalidating all API key doc cache and descriptor cache");
            docCache.invalidateAll();
            roleDescriptorsBytesCache.invalidateAll();
        }
    }
}
