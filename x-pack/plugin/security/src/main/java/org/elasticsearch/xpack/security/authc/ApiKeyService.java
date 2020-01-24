/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ApiKey;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import javax.crypto.SecretKeyFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;

public class ApiKeyService {

    private static final Logger logger = LogManager.getLogger(ApiKeyService.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    public static final String API_KEY_ID_KEY = "_security_api_key_id";
    public static final String API_KEY_REALM_NAME = "_es_api_key";
    public static final String API_KEY_REALM_TYPE = "_es_api_key";
    public static final String API_KEY_CREATOR_REALM = "_security_api_key_creator_realm";
    static final String API_KEY_ROLE_DESCRIPTORS_KEY = "_security_api_key_role_descriptors";
    static final String API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY = "_security_api_key_limited_by_role_descriptors";


    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = new Setting<>(
        "xpack.security.authc.api_key.hashing.algorithm", "pbkdf2", Function.identity(), v -> {
        if (Hasher.getAvailableAlgoStoredHash().contains(v.toLowerCase(Locale.ROOT)) == false) {
            throw new IllegalArgumentException("Invalid algorithm: " + v + ". Valid values for password hashing are " +
                Hasher.getAvailableAlgoStoredHash().toString());
        } else if (v.regionMatches(true, 0, "pbkdf2", 0, "pbkdf2".length())) {
            try {
                SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException(
                    "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the " +
                        "PBKDF2 algorithms for the [xpack.security.authc.api_key.hashing.algorithm] setting.", e);
            }
        }
    }, Setting.Property.NodeScope);
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting("xpack.security.authc.api_key.delete.timeout",
            TimeValue.MINUS_ONE, Property.NodeScope);
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting("xpack.security.authc.api_key.delete.interval",
            TimeValue.timeValueHours(24L), Property.NodeScope);
    public static final Setting<String> CACHE_HASH_ALGO_SETTING = Setting.simpleString("xpack.security.authc.api_key.cache.hash_algo",
        "ssha256", Setting.Property.NodeScope);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("xpack.security.authc.api_key.cache.ttl",
        TimeValue.timeValueHours(24L), Property.NodeScope);
    public static final Setting<Integer> CACHE_MAX_KEYS_SETTING = Setting.intSetting("xpack.security.authc.api_key.cache.max_keys",
        10000, Property.NodeScope);

    private final Clock clock;
    private final Client client;
    private final XPackLicenseState licenseState;
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

    private volatile long lastExpirationRunMs;

    public ApiKeyService(Settings settings, Clock clock, Client client, XPackLicenseState licenseState, SecurityIndexManager securityIndex,
                         ClusterService clusterService, ThreadPool threadPool) {
        this.clock = clock;
        this.client = client;
        this.licenseState = licenseState;
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
        if (ttl.getNanos() > 0) {
            this.apiKeyAuthCache = CacheBuilder.<String, ListenableFuture<CachedApiKeyHashResult>>builder()
                .setExpireAfterWrite(ttl)
                .setMaximumWeight(CACHE_MAX_KEYS_SETTING.get(settings))
                .build();
        } else {
            this.apiKeyAuthCache = null;
        }
    }

    /**
     * Asynchronously creates a new API key based off of the request and authentication
     * @param authentication the authentication that this api key should be based off of
     * @param request the request to create the api key included any permission restrictions
     * @param userRoles the user's actual roles that we always enforce
     * @param listener the listener that will be used to notify of completion
     */
    public void createApiKey(Authentication authentication, CreateApiKeyRequest request, Set<RoleDescriptor> userRoles,
                             ActionListener<CreateApiKeyResponse> listener) {
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(new IllegalArgumentException("authentication must be provided"));
        } else {
            createApiKeyAndIndexIt(authentication, request, userRoles, listener);
        }
    }

    private void createApiKeyAndIndexIt(Authentication authentication, CreateApiKeyRequest request, Set<RoleDescriptor> roleDescriptorSet,
                                        ActionListener<CreateApiKeyResponse> listener) {
        final Instant created = clock.instant();
        final Instant expiration = getApiKeyExpiration(created, request);
        final SecureString apiKey = UUIDs.randomBase64UUIDSecureString();
        final Version version = clusterService.state().nodes().getMinNodeVersion();

        try (XContentBuilder builder = newDocument(apiKey, request.getName(), authentication, roleDescriptorSet, created, expiration,
            request.getRoleDescriptors(), version)) {
            final IndexRequest indexRequest =
                client.prepareIndex(SECURITY_MAIN_ALIAS)
                    .setSource(builder)
                    .setRefreshPolicy(request.getRefreshPolicy())
                    .request();
            securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () ->
            executeAsyncWithOrigin(client, SECURITY_ORIGIN, IndexAction.INSTANCE, indexRequest,
                    ActionListener.wrap(
                            indexResponse -> listener.onResponse(
                                    new CreateApiKeyResponse(request.getName(), indexResponse.getId(), apiKey, expiration)),
                            listener::onFailure)));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * package protected for testing
     */
    XContentBuilder newDocument(SecureString apiKey, String name, Authentication authentication, Set<RoleDescriptor> userRoles,
                                        Instant created, Instant expiration, List<RoleDescriptor> keyRoles,
                                        Version version) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
            .field("doc_type", "api_key")
            .field("creation_time", created.toEpochMilli())
            .field("expiration_time", expiration == null ? null : expiration.toEpochMilli())
            .field("api_key_invalidated", false);

        byte[] utf8Bytes = null;
        final char[] keyHash = hasher.hash(apiKey);
        try {
            utf8Bytes = CharArrays.toUtf8Bytes(keyHash);
            builder.field("api_key_hash").utf8Value(utf8Bytes, 0, utf8Bytes.length);
        } finally {
            if (utf8Bytes != null) {
                Arrays.fill(utf8Bytes, (byte) 0);
            }
            Arrays.fill(keyHash, (char) 0);
        }


        // Save role_descriptors
        builder.startObject("role_descriptors");
        if (keyRoles != null && keyRoles.isEmpty() == false) {
            for (RoleDescriptor descriptor : keyRoles) {
                builder.field(descriptor.getName(),
                    (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
            }
        }
        builder.endObject();

        // Save limited_by_role_descriptors
        builder.startObject("limited_by_role_descriptors");
        for (RoleDescriptor descriptor : userRoles) {
            builder.field(descriptor.getName(),
                (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
        }
        builder.endObject();

        builder.field("name", name)
            .field("version", version.id)
            .startObject("creator")
            .field("principal", authentication.getUser().principal())
            .field("metadata", authentication.getUser().metadata())
            .field("realm", authentication.getLookedUpBy() == null ?
                authentication.getAuthenticatedBy().getName() : authentication.getLookedUpBy().getName())
            .endObject()
            .endObject();

        return builder;
    }

    /**
     * Checks for the presence of a {@code Authorization} header with a value that starts with
     * {@code ApiKey }. If found this will attempt to authenticate the key.
     */
    void authenticateWithApiKeyIfPresent(ThreadContext ctx, ActionListener<AuthenticationResult> listener) {
        if (isEnabled()) {
            final ApiKeyCredentials credentials;
            try {
                credentials = getCredentialsFromHeader(ctx);
            } catch (IllegalArgumentException iae) {
                listener.onResponse(AuthenticationResult.unsuccessful(iae.getMessage(), iae));
                return;
            }

            if (credentials != null) {
                loadApiKeyAndValidateCredentials(ctx, credentials, ActionListener.wrap(
                    response -> {
                        credentials.close();
                        listener.onResponse(response);
                    },
                    e -> {
                        credentials.close();
                        listener.onFailure(e);
                    }
                ));
            } else {
                listener.onResponse(AuthenticationResult.notHandled());
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    private void loadApiKeyAndValidateCredentials(ThreadContext ctx, ApiKeyCredentials credentials,
                                                  ActionListener<AuthenticationResult> listener) {
        final String docId = credentials.getId();
        final GetRequest getRequest = client
            .prepareGet(SECURITY_MAIN_ALIAS, docId)
            .setFetchSource(true)
            .request();
        executeAsyncWithOrigin(ctx, SECURITY_ORIGIN, getRequest, ActionListener.<GetResponse>wrap(response -> {
                if (response.isExists()) {
                    final Map<String, Object> source = response.getSource();
                    validateApiKeyCredentials(docId, source, credentials, clock, listener);
                } else {
                    listener.onResponse(
                        AuthenticationResult.unsuccessful("unable to find apikey with id " + credentials.getId(), null));
                }
            },
            e -> listener.onResponse(AuthenticationResult.unsuccessful(
                "apikey authentication for id " + credentials.getId() + " encountered a failure", e))),
            client::get);
    }

    /**
     * The current request has been authenticated by an API key and this method enables the
     * retrieval of role descriptors that are associated with the api key
     */
    public void getRoleForApiKey(Authentication authentication, ActionListener<ApiKeyRoleDescriptors> listener) {
        if (authentication.getAuthenticationType() != Authentication.AuthenticationType.API_KEY) {
            throw new IllegalStateException("authentication type must be api key but is " + authentication.getAuthenticationType());
        }

        final Map<String, Object> metadata = authentication.getMetadata();
        final String apiKeyId = (String) metadata.get(API_KEY_ID_KEY);

        final Map<String, Object> roleDescriptors = (Map<String, Object>) metadata.get(API_KEY_ROLE_DESCRIPTORS_KEY);
        final Map<String, Object> authnRoleDescriptors = (Map<String, Object>) metadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY);

        if (roleDescriptors == null && authnRoleDescriptors == null) {
            listener.onFailure(new ElasticsearchSecurityException("no role descriptors found for API key"));
        } else if (roleDescriptors == null || roleDescriptors.isEmpty()) {
            final List<RoleDescriptor> authnRoleDescriptorsList = parseRoleDescriptors(apiKeyId, authnRoleDescriptors);
            listener.onResponse(new ApiKeyRoleDescriptors(apiKeyId, authnRoleDescriptorsList, null));
        } else {
            final List<RoleDescriptor> roleDescriptorList = parseRoleDescriptors(apiKeyId, roleDescriptors);
            final List<RoleDescriptor> authnRoleDescriptorsList = parseRoleDescriptors(apiKeyId, authnRoleDescriptors);
            listener.onResponse(new ApiKeyRoleDescriptors(apiKeyId, roleDescriptorList, authnRoleDescriptorsList));
        }
    }

    public static class ApiKeyRoleDescriptors {

        private final String apiKeyId;
        private final List<RoleDescriptor> roleDescriptors;
        private final List<RoleDescriptor> limitedByRoleDescriptors;

        public ApiKeyRoleDescriptors(String apiKeyId, List<RoleDescriptor> roleDescriptors, List<RoleDescriptor> limitedByDescriptors) {
            this.apiKeyId = apiKeyId;
            this.roleDescriptors = roleDescriptors;
            this.limitedByRoleDescriptors = limitedByDescriptors;
        }

        public String getApiKeyId() {
            return apiKeyId;
        }

        public List<RoleDescriptor> getRoleDescriptors() {
            return roleDescriptors;
        }

        public List<RoleDescriptor> getLimitedByRoleDescriptors() {
            return limitedByRoleDescriptors;
        }
    }

    private List<RoleDescriptor> parseRoleDescriptors(final String apiKeyId, final Map<String, Object> roleDescriptors) {
        if (roleDescriptors == null) {
            return null;
        }
        return roleDescriptors.entrySet().stream()
            .map(entry -> {
                final String name = entry.getKey();
                final Map<String, Object> rdMap = (Map<String, Object>) entry.getValue();
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    builder.map(rdMap);
                    try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                        new ApiKeyLoggingDeprecationHandler(deprecationLogger, apiKeyId),
                        BytesReference.bytes(builder).streamInput())) {
                        return RoleDescriptor.parse(name, parser, false);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
    }

    /**
     * Validates the ApiKey using the source map
     * @param docId the identifier of the document that was retrieved from the security index
     * @param source the source map from a get of the ApiKey document
     * @param credentials the credentials provided by the user
     * @param listener the listener to notify after verification
     */
    void validateApiKeyCredentials(String docId, Map<String, Object> source, ApiKeyCredentials credentials, Clock clock,
                                   ActionListener<AuthenticationResult> listener) {
        final String docType = (String) source.get("doc_type");
        final Boolean invalidated = (Boolean) source.get("api_key_invalidated");
        if ("api_key".equals(docType) == false) {
            listener.onResponse(
                AuthenticationResult.unsuccessful("document [" + docId + "] is [" + docType + "] not an api key", null));
        } else if (invalidated == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("api key document is missing invalidated field", null));
        } else if (invalidated) {
            listener.onResponse(AuthenticationResult.unsuccessful("api key has been invalidated", null));
        } else {
            final String apiKeyHash = (String) source.get("api_key_hash");
            if (apiKeyHash == null) {
                throw new IllegalStateException("api key hash is missing");
            }

            if (apiKeyAuthCache != null) {
                final AtomicBoolean valueAlreadyInCache = new AtomicBoolean(true);
                final ListenableFuture<CachedApiKeyHashResult> listenableCacheEntry;
                try {
                    listenableCacheEntry = apiKeyAuthCache.computeIfAbsent(credentials.getId(),
                        k -> {
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
                                    validateApiKeyExpiration(source, credentials, clock, listener);
                                } else {
                                    listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                                }
                            } else if (result.verify(credentials.getKey())) { // same key, pass the same result
                                listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                            } else {
                                apiKeyAuthCache.invalidate(credentials.getId(), listenableCacheEntry);
                                validateApiKeyCredentials(docId, source, credentials, clock, listener);
                            }
                        }, listener::onFailure),
                        threadPool.generic(), threadPool.getThreadContext());
                } else {
                    final boolean verified = verifyKeyAgainstHash(apiKeyHash, credentials);
                    listenableCacheEntry.onResponse(new CachedApiKeyHashResult(verified, credentials.getKey()));
                    if (verified) {
                        // move on
                        validateApiKeyExpiration(source, credentials, clock, listener);
                    } else {
                        listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                    }
                }
            } else {
                final boolean verified = verifyKeyAgainstHash(apiKeyHash, credentials);
                if (verified) {
                    // move on
                    validateApiKeyExpiration(source, credentials, clock, listener);
                } else {
                    listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
                }
            }
        }
    }

    // pkg private for testing
    CachedApiKeyHashResult getFromCache(String id) {
        return apiKeyAuthCache == null ? null : FutureUtils.get(apiKeyAuthCache.get(id), 0L, TimeUnit.MILLISECONDS);
    }

    private void validateApiKeyExpiration(Map<String, Object> source, ApiKeyCredentials credentials, Clock clock,
                                  ActionListener<AuthenticationResult> listener) {
        final Long expirationEpochMilli = (Long) source.get("expiration_time");
        if (expirationEpochMilli == null || Instant.ofEpochMilli(expirationEpochMilli).isAfter(clock.instant())) {
            final Map<String, Object> creator = Objects.requireNonNull((Map<String, Object>) source.get("creator"));
            final String principal = Objects.requireNonNull((String) creator.get("principal"));
            Map<String, Object> metadata = (Map<String, Object>) creator.get("metadata");
            final Map<String, Object> roleDescriptors = (Map<String, Object>) source.get("role_descriptors");
            final Map<String, Object> limitedByRoleDescriptors = (Map<String, Object>) source.get("limited_by_role_descriptors");
            final String[] roleNames = (roleDescriptors != null) ? roleDescriptors.keySet().toArray(Strings.EMPTY_ARRAY)
                : limitedByRoleDescriptors.keySet().toArray(Strings.EMPTY_ARRAY);
            final User apiKeyUser = new User(principal, roleNames, null, null, metadata, true);
            final Map<String, Object> authResultMetadata = new HashMap<>();
            authResultMetadata.put(API_KEY_CREATOR_REALM, creator.get("realm"));
            authResultMetadata.put(API_KEY_ROLE_DESCRIPTORS_KEY, roleDescriptors);
            authResultMetadata.put(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, limitedByRoleDescriptors);
            authResultMetadata.put(API_KEY_ID_KEY, credentials.getId());
            listener.onResponse(AuthenticationResult.success(apiKeyUser, authResultMetadata));
        } else {
            listener.onResponse(AuthenticationResult.unsuccessful("api key is expired", null));
        }
    }

    /**
     * Gets the API Key from the <code>Authorization</code> header if the header begins with
     * <code>ApiKey </code>
     */
    static ApiKeyCredentials getCredentialsFromHeader(ThreadContext threadContext) {
        String header = threadContext.getHeader("Authorization");
        if (Strings.hasText(header) && header.regionMatches(true, 0, "ApiKey ", 0, "ApiKey ".length())
            && header.length() > "ApiKey ".length()) {
            final byte[] decodedApiKeyCredBytes = Base64.getDecoder().decode(header.substring("ApiKey ".length()));
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
                return new ApiKeyCredentials(new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, colonIndex + 1, apiKeyCredChars.length)));
            } finally {
                if (apiKeyCredChars != null) {
                    Arrays.fill(apiKeyCredChars, (char) 0);
                }
            }
        }
        return null;
    }

    // Protected instance method so this can be mocked
    protected boolean verifyKeyAgainstHash(String apiKeyHash, ApiKeyCredentials credentials) {
        final char[] apiKeyHashChars = apiKeyHash.toCharArray();
        try {
            Hasher hasher = Hasher.resolveFromHash(apiKeyHash.toCharArray());
            return hasher.verify(credentials.getKey(), apiKeyHashChars);
        } finally {
            Arrays.fill(apiKeyHashChars, (char) 0);
        }
    }

    private Instant getApiKeyExpiration(Instant now, CreateApiKeyRequest request) {
        if (request.getExpiration() != null) {
            return now.plusSeconds(request.getExpiration().getSeconds());
        } else {
            return null;
        }
    }

    private boolean isEnabled() {
        return enabled && licenseState.isApiKeyServiceAllowed();
    }

    private void ensureEnabled() {
        if (licenseState.isApiKeyServiceAllowed() == false) {
            throw LicenseUtils.newComplianceException("api keys");
        }
        if (enabled == false) {
            throw new IllegalStateException("api keys are not enabled");
        }
    }

    // package private class for testing
    static final class ApiKeyCredentials implements Closeable {
        private final String id;
        private final SecureString key;

        ApiKeyCredentials(String id, SecureString key) {
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
    }

    private static class ApiKeyLoggingDeprecationHandler implements DeprecationHandler {

        private final DeprecationLogger deprecationLogger;
        private final String apiKeyId;

        private ApiKeyLoggingDeprecationHandler(DeprecationLogger logger, String apiKeyId) {
            this.deprecationLogger = logger;
            this.apiKeyId = apiKeyId;
        }

        @Override
        public void usedDeprecatedName(String usedName, String modernName) {
            deprecationLogger.deprecated("Deprecated field [{}] used in api key [{}], expected [{}] instead",
                usedName, apiKeyId, modernName);
        }

        @Override
        public void usedDeprecatedField(String usedName, String replacedWith) {
            deprecationLogger.deprecated("Deprecated field [{}] used in api key [{}], replaced by [{}]",
                usedName, apiKeyId, replacedWith);
        }
    }

    /**
     * Invalidate API keys for given realm, user name, API key name and id.
     * @param realmName realm name
     * @param username user name
     * @param apiKeyName API key name
     * @param apiKeyId API key id
     * @param invalidateListener listener for {@link InvalidateApiKeyResponse}
     */
    public void invalidateApiKeys(String realmName, String username, String apiKeyName, String apiKeyId,
                                  ActionListener<InvalidateApiKeyResponse> invalidateListener) {
        ensureEnabled();
        if (Strings.hasText(realmName) == false && Strings.hasText(username) == false && Strings.hasText(apiKeyName) == false
            && Strings.hasText(apiKeyId) == false) {
            logger.trace("none of the parameters [api key id, api key name, username, realm name] were specified for invalidation");
            invalidateListener
                .onFailure(new IllegalArgumentException("One of [api key id, api key name, username, realm name] must be specified"));
        } else {
            findApiKeysForUserRealmApiKeyIdAndNameCombination(realmName, username, apiKeyName, apiKeyId, true, false,
                ActionListener.wrap(apiKeys -> {
                    if (apiKeys.isEmpty()) {
                        logger.debug(
                            "No active api keys to invalidate for realm [{}], username [{}], api key name [{}] and api key id [{}]",
                            realmName, username, apiKeyName, apiKeyId);
                        invalidateListener.onResponse(InvalidateApiKeyResponse.emptyResponse());
                    } else {
                        invalidateAllApiKeys(apiKeys.stream().map(apiKey -> apiKey.getId()).collect(Collectors.toSet()),
                            invalidateListener);
                    }
                }, invalidateListener::onFailure));
        }
    }

    private void invalidateAllApiKeys(Collection<String> apiKeyIds, ActionListener<InvalidateApiKeyResponse> invalidateListener) {
        indexInvalidation(apiKeyIds, invalidateListener, null);
    }

    private void findApiKeys(final BoolQueryBuilder boolQuery, boolean filterOutInvalidatedKeys, boolean filterOutExpiredKeys,
                             ActionListener<Collection<ApiKey>> listener) {
        if (filterOutInvalidatedKeys) {
            boolQuery.filter(QueryBuilders.termQuery("api_key_invalidated", false));
        }
        if (filterOutExpiredKeys) {
            final BoolQueryBuilder expiredQuery = QueryBuilders.boolQuery();
            expiredQuery.should(QueryBuilders.rangeQuery("expiration_time").lte(Instant.now().toEpochMilli()));
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
            securityIndex.checkIndexVersionThenExecute(listener::onFailure,
                    () -> ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(supplier, listener),
                            (SearchHit hit) -> {
                                Map<String, Object> source = hit.getSourceAsMap();
                                String name = (String) source.get("name");
                                String id = hit.getId();
                                Long creation = (Long) source.get("creation_time");
                                Long expiration = (Long) source.get("expiration_time");
                                Boolean invalidated = (Boolean) source.get("api_key_invalidated");
                                String username = (String) ((Map<String, Object>) source.get("creator")).get("principal");
                                String realm = (String) ((Map<String, Object>) source.get("creator")).get("realm");
                                return new ApiKey(name, id, Instant.ofEpochMilli(creation),
                                        (expiration != null) ? Instant.ofEpochMilli(expiration) : null, invalidated, username, realm);
                            }));
        }
    }

    private void findApiKeysForUserRealmApiKeyIdAndNameCombination(String realmName, String userName, String apiKeyName, String apiKeyId,
                                                                   boolean filterOutInvalidatedKeys, boolean filterOutExpiredKeys,
                                                                   ActionListener<Collection<ApiKey>> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndex.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", "api_key"));
            if (Strings.hasText(realmName)) {
                boolQuery.filter(QueryBuilders.termQuery("creator.realm", realmName));
            }
            if (Strings.hasText(userName)) {
                boolQuery.filter(QueryBuilders.termQuery("creator.principal", userName));
            }
            if (Strings.hasText(apiKeyName)) {
                boolQuery.filter(QueryBuilders.termQuery("name", apiKeyName));
            }
            if (Strings.hasText(apiKeyId)) {
                boolQuery.filter(QueryBuilders.termQuery("_id", apiKeyId));
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
    private void indexInvalidation(Collection<String> apiKeyIds, ActionListener<InvalidateApiKeyResponse> listener,
                                   @Nullable InvalidateApiKeyResponse previousResult) {
        maybeStartApiKeyRemover();
        if (apiKeyIds.isEmpty()) {
            listener.onFailure(new ElasticsearchSecurityException("No api key ids provided for invalidation"));
        } else {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (String apiKeyId : apiKeyIds) {
                UpdateRequest request = client
                    .prepareUpdate(SECURITY_MAIN_ALIAS, apiKeyId)
                    .setDoc(Collections.singletonMap("api_key_invalidated", true))
                    .request();
                bulkRequestBuilder.add(request);
            }
            bulkRequestBuilder.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
            securityIndex.prepareIndexIfNeededThenExecute(ex -> listener.onFailure(traceLog("prepare security index", ex)),
                () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, bulkRequestBuilder.request(),
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
                        InvalidateApiKeyResponse result = new InvalidateApiKeyResponse(invalidated, previouslyInvalidated,
                            failedRequestResponses);
                        listener.onResponse(result);
                    }, e -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        traceLog("invalidate api keys", cause);
                        listener.onFailure(e);
                    }), client::bulk));
        }
    }

    /**
     * Logs an exception concerning a specific api key at TRACE level (if enabled)
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
     * @param realmName realm name
     * @param username user name
     * @param apiKeyName API key name
     * @param apiKeyId API key id
     * @param listener listener for {@link GetApiKeyResponse}
     */
    public void getApiKeys(String realmName, String username, String apiKeyName, String apiKeyId,
                           ActionListener<GetApiKeyResponse> listener) {
        ensureEnabled();
        findApiKeysForUserRealmApiKeyIdAndNameCombination(realmName, username, apiKeyName, apiKeyId, false, false,
            ActionListener.wrap(apiKeyInfos -> {
                if (apiKeyInfos.isEmpty()) {
                    logger.debug("No active api keys found for realm [{}], user [{}], api key name [{}] and api key id [{}]",
                        realmName, username, apiKeyName, apiKeyId);
                    listener.onResponse(GetApiKeyResponse.emptyResponse());
                } else {
                    listener.onResponse(new GetApiKeyResponse(apiKeyInfos));
                }
            }, listener::onFailure));
    }

    /**
     * Returns realm name for the authenticated user.
     * If the user is authenticated by realm type {@value API_KEY_REALM_TYPE}
     * then it will return the realm name of user who created this API key.
     * @param authentication {@link Authentication}
     * @return realm name
     */
    public static String getCreatorRealmName(final Authentication authentication) {
        if (authentication.getAuthenticatedBy().getType().equals(API_KEY_REALM_TYPE)) {
            return (String) authentication.getMetadata().get(API_KEY_CREATOR_REALM);
        } else {
            return authentication.getAuthenticatedBy().getName();
        }
    }

    final class CachedApiKeyHashResult {
        final boolean success;
        final char[] hash;

        CachedApiKeyHashResult(boolean success, SecureString apiKey) {
            this.success = success;
            this.hash = cacheHasher.hash(apiKey);
        }

        private boolean verify(SecureString password) {
            return hash != null && cacheHasher.verify(password, hash);
        }
    }
}
