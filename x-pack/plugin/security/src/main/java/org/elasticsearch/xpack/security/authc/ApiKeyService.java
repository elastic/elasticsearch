/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
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
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.action.apikey.AbstractCreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.BaseBulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BaseUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.metric.SecurityCacheMetrics;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException.Feature;
import org.elasticsearch.xpack.security.support.LockingAtomicCounter;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.TransportVersions.ADD_MANAGE_ROLES_PRIVILEGE;
import static org.elasticsearch.TransportVersions.ROLE_REMOTE_CLUSTER_PRIVS;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.WORKFLOWS_RESTRICTION_VERSION;
import static org.elasticsearch.xpack.security.Security.SECURITY_CRYPTO_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class ApiKeyService implements Closeable {

    private static final Logger logger = LogManager.getLogger(ApiKeyService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ApiKeyService.class);

    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = XPackSettings.defaultStoredHashAlgorithmSetting(
        "xpack.security.authc.api_key.hashing.algorithm",
        (s) -> Hasher.PBKDF2.name()
    );
    public static final Setting<TimeValue> DELETE_TIMEOUT = Setting.timeSetting(
        "xpack.security.authc.api_key.delete.timeout",
        TimeValue.MINUS_ONE,
        Property.NodeScope
    );
    public static final Setting<TimeValue> DELETE_INTERVAL = Setting.timeSetting(
        "xpack.security.authc.api_key.delete.interval",
        TimeValue.timeValueHours(24L),
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<TimeValue> DELETE_RETENTION_PERIOD = Setting.positiveTimeSetting(
        "xpack.security.authc.api_key.delete.retention_period",
        TimeValue.timeValueDays(7),
        Property.NodeScope,
        Property.Dynamic
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

    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder().allowRestriction(true).build();

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Hasher hasher;
    private final boolean enabled;
    private final Settings settings;
    private final InactiveApiKeysRemover inactiveApiKeysRemover;
    private final Cache<String, ListenableFuture<CachedApiKeyHashResult>> apiKeyAuthCache;
    private final Hasher cacheHasher;
    private final ThreadPool threadPool;
    private final ApiKeyDocCache apiKeyDocCache;

    // The API key secret is a Base64 encoded v4 UUID without padding. The UUID is 128 bits, i.e. 16 byte,
    // which requires 22 digits of Base64 characters for encoding without padding.
    // See also UUIDs.randomBase64UUIDSecureString
    private static final int API_KEY_SECRET_LENGTH = 22;
    private static final long EVICTION_MONITOR_INTERVAL_SECONDS = 300L; // 5 minutes
    private static final long EVICTION_MONITOR_INTERVAL_NANOS = EVICTION_MONITOR_INTERVAL_SECONDS * 1_000_000_000L;
    private static final long EVICTION_WARNING_THRESHOLD = 15L * EVICTION_MONITOR_INTERVAL_SECONDS; // 15 eviction per sec = 4500 in 5 min
    private final AtomicLong lastEvictionCheckedAt = new AtomicLong(0);
    private final LongAdder evictionCounter = new LongAdder();

    private final List<AutoCloseable> cacheMetrics;

    @SuppressWarnings("this-escape")
    public ApiKeyService(
        Settings settings,
        Clock clock,
        Client client,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        CacheInvalidatorRegistry cacheInvalidatorRegistry,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings);
        this.hasher = Hasher.resolve(PASSWORD_HASHING_ALGORITHM.get(settings));
        this.settings = settings;
        this.inactiveApiKeysRemover = new InactiveApiKeysRemover(settings, client, clusterService);
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
            cacheInvalidatorRegistry.registerCacheInvalidator("api_key_doc", new CacheInvalidatorRegistry.CacheInvalidator() {
                @Override
                public void invalidate(Collection<String> keys) {
                    if (apiKeyDocCache != null) {
                        apiKeyDocCache.invalidate(keys);
                    }
                }

                @Override
                public void invalidateAll() {
                    if (apiKeyDocCache != null) {
                        apiKeyDocCache.invalidateAll();
                    }
                }
            });
        } else {
            this.apiKeyAuthCache = null;
            this.apiKeyDocCache = null;
        }

        if (enabled) {
            final List<AutoCloseable> cacheMetrics = new ArrayList<>();
            if (this.apiKeyAuthCache != null) {
                cacheMetrics.addAll(
                    SecurityCacheMetrics.registerAsyncCacheMetrics(
                        meterRegistry,
                        this.apiKeyAuthCache,
                        SecurityCacheMetrics.CacheType.API_KEY_AUTH_CACHE
                    )
                );
            }
            if (this.apiKeyDocCache != null) {
                cacheMetrics.addAll(
                    SecurityCacheMetrics.registerAsyncCacheMetrics(
                        meterRegistry,
                        this.apiKeyDocCache.docCache,
                        SecurityCacheMetrics.CacheType.API_KEY_DOCS_CACHE
                    )
                );
                cacheMetrics.addAll(
                    SecurityCacheMetrics.registerAsyncCacheMetrics(
                        meterRegistry,
                        this.apiKeyDocCache.roleDescriptorsBytesCache,
                        SecurityCacheMetrics.CacheType.API_KEY_ROLE_DESCRIPTORS_CACHE
                    )
                );
            }
            this.cacheMetrics = List.copyOf(cacheMetrics);
        } else {
            this.cacheMetrics = List.of();
        }

    }

    /**
     * Asynchronously creates a new API key based off of the request and authentication
     * @param authentication the authentication that this api key should be based off of
     * @param request the request to create the api key included any permission restrictions
     * @param userRoleDescriptors the user's actual roles that we always enforce
     * @param listener the listener that will be used to notify of completion
     */
    public void createApiKey(
        Authentication authentication,
        AbstractCreateApiKeyRequest request,
        Set<RoleDescriptor> userRoleDescriptors,
        ActionListener<CreateApiKeyResponse> listener
    ) {
        assert request.getType() != ApiKey.Type.CROSS_CLUSTER || false == authentication.isApiKey()
            : "cannot create derived cross-cluster API keys (name=["
                + request.getName()
                + "], type=["
                + request.getType()
                + "], auth=["
                + authentication
                + "])";
        assert request.getType() != ApiKey.Type.CROSS_CLUSTER || userRoleDescriptors.isEmpty()
            : "owner user role descriptor must be empty for cross-cluster API keys (name=["
                + request.getName()
                + "], type=["
                + request.getType()
                + "], roles=["
                + userRoleDescriptors
                + "])";
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(new IllegalArgumentException("authentication must be provided"));
        } else {
            final TransportVersion transportVersion = getMinTransportVersion();
            if (validateRoleDescriptorsForMixedCluster(listener, request.getRoleDescriptors(), transportVersion) == false) {
                return;
            }

            if (transportVersion.before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)
                && request.getType() == ApiKey.Type.CROSS_CLUSTER) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "all nodes must have version ["
                            + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                            + "] or higher to support creating cross cluster API keys"
                    )
                );
                return;
            }
            final IllegalArgumentException workflowsValidationException = validateWorkflowsRestrictionConstraints(
                transportVersion,
                request.getRoleDescriptors(),
                userRoleDescriptors
            );
            if (workflowsValidationException != null) {
                listener.onFailure(workflowsValidationException);
                return;
            }

            Set<RoleDescriptor> filteredRoleDescriptors = filterRoleDescriptorsForMixedCluster(
                userRoleDescriptors,
                transportVersion,
                request.getId()
            );

            createApiKeyAndIndexIt(authentication, request, filteredRoleDescriptors, listener);
        }
    }

    private Set<RoleDescriptor> filterRoleDescriptorsForMixedCluster(
        final Set<RoleDescriptor> userRoleDescriptors,
        final TransportVersion transportVersion,
        final String... apiKeyIds
    ) {
        final Set<RoleDescriptor> userRolesWithoutDescription = removeUserRoleDescriptorDescriptions(userRoleDescriptors);
        return maybeRemoveRemotePrivileges(userRolesWithoutDescription, transportVersion, apiKeyIds);
    }

    private boolean validateRoleDescriptorsForMixedCluster(
        final ActionListener<?> listener,
        final List<RoleDescriptor> roleDescriptors,
        final TransportVersion transportVersion
    ) {
        if (transportVersion.before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY) && hasRemoteIndices(roleDescriptors)) {
            // API keys with roles which define remote indices privileges is not allowed in a mixed cluster.
            listener.onFailure(
                new IllegalArgumentException(
                    "all nodes must have version ["
                        + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                        + "] or higher to support remote indices privileges for API keys"
                )
            );
            return false;
        }
        if (transportVersion.before(ROLE_REMOTE_CLUSTER_PRIVS) && hasRemoteCluster(roleDescriptors)) {
            // API keys with roles which define remote cluster privileges is not allowed in a mixed cluster.
            listener.onFailure(
                new IllegalArgumentException(
                    "all nodes must have version ["
                        + ROLE_REMOTE_CLUSTER_PRIVS
                        + "] or higher to support remote cluster privileges for API keys"
                )
            );
            return false;
        }
        if (transportVersion.before(ADD_MANAGE_ROLES_PRIVILEGE) && hasGlobalManageRolesPrivilege(roleDescriptors)) {
            listener.onFailure(
                new IllegalArgumentException(
                    "all nodes must have version ["
                        + ADD_MANAGE_ROLES_PRIVILEGE
                        + "] or higher to support the manage roles privilege for API keys"
                )
            );
            return false;
        }
        return true;
    }

    /**
     * This method removes description from the given user's (limited-by) role descriptors.
     * The description field is not supported for API key role descriptors hence storing limited-by roles with descriptions
     * would be inconsistent and require handling backwards compatibility.
     * Hence why we have to remove them before create/update of API key roles.
     */
    static Set<RoleDescriptor> removeUserRoleDescriptorDescriptions(Set<RoleDescriptor> userRoleDescriptors) {
        return userRoleDescriptors.stream().map(roleDescriptor -> {
            if (roleDescriptor.hasDescription()) {
                return new RoleDescriptor(
                    roleDescriptor.getName(),
                    roleDescriptor.getClusterPrivileges(),
                    roleDescriptor.getIndicesPrivileges(),
                    roleDescriptor.getApplicationPrivileges(),
                    roleDescriptor.getConditionalClusterPrivileges(),
                    roleDescriptor.getRunAs(),
                    roleDescriptor.getMetadata(),
                    roleDescriptor.getTransientMetadata(),
                    roleDescriptor.getRemoteIndicesPrivileges(),
                    roleDescriptor.getRemoteClusterPermissions(),
                    roleDescriptor.getRestriction(),
                    null
                );
            }
            return roleDescriptor;
        }).collect(Collectors.toSet());
    }

    private TransportVersion getMinTransportVersion() {
        return clusterService.state().getMinTransportVersion();
    }

    private static boolean hasRemoteIndices(Collection<RoleDescriptor> roleDescriptors) {
        return roleDescriptors != null && roleDescriptors.stream().anyMatch(RoleDescriptor::hasRemoteIndicesPrivileges);
    }

    private static boolean hasRemoteCluster(Collection<RoleDescriptor> roleDescriptors) {
        return roleDescriptors != null && roleDescriptors.stream().anyMatch(RoleDescriptor::hasRemoteClusterPermissions);
    }

    private static boolean hasGlobalManageRolesPrivilege(Collection<RoleDescriptor> roleDescriptors) {
        return roleDescriptors != null
            && roleDescriptors.stream()
                .flatMap(roleDescriptor -> Arrays.stream(roleDescriptor.getConditionalClusterPrivileges()))
                .anyMatch(privilege -> privilege instanceof ConfigurableClusterPrivileges.ManageRolesPrivilege);
    }

    private static IllegalArgumentException validateWorkflowsRestrictionConstraints(
        TransportVersion transportVersion,
        List<RoleDescriptor> requestRoleDescriptors,
        Set<RoleDescriptor> userRoleDescriptors
    ) {
        if (getNumberOfRoleDescriptorsWithRestriction(userRoleDescriptors) > 0L) {
            return new IllegalArgumentException("owner user role descriptors must not include restriction");
        }
        final long numberOfRoleDescriptorsWithRestriction = getNumberOfRoleDescriptorsWithRestriction(requestRoleDescriptors);
        if (numberOfRoleDescriptorsWithRestriction > 0L) {
            // creating/updating API keys with restrictions is not allowed in a mixed cluster.
            if (transportVersion.before(WORKFLOWS_RESTRICTION_VERSION)) {
                return new IllegalArgumentException(
                    "all nodes must have version ["
                        + WORKFLOWS_RESTRICTION_VERSION.toReleaseVersion()
                        + "] or higher to support restrictions for API keys"
                );
            }
            // It's only allowed to create/update API keys with a single role descriptor that is restricted.
            if (numberOfRoleDescriptorsWithRestriction != 1L) {
                return new IllegalArgumentException("more than one role descriptor with restriction is not supported");
            }
            // Combining roles with and without restriction is not allowed either.
            if (numberOfRoleDescriptorsWithRestriction != requestRoleDescriptors.size()) {
                return new IllegalArgumentException("combining role descriptors with and without restriction is not supported");
            }
        }
        return null;
    }

    private static long getNumberOfRoleDescriptorsWithRestriction(Collection<RoleDescriptor> roleDescriptors) {
        if (roleDescriptors == null || roleDescriptors.isEmpty()) {
            return 0L;
        }
        return roleDescriptors.stream().filter(RoleDescriptor::hasRestriction).count();
    }

    private void createApiKeyAndIndexIt(
        Authentication authentication,
        AbstractCreateApiKeyRequest request,
        Set<RoleDescriptor> userRoleDescriptors,
        ActionListener<CreateApiKeyResponse> listener
    ) {
        final Instant created = clock.instant();
        final Instant expiration = getApiKeyExpiration(created, request.getExpiration());
        final SecureString apiKey = UUIDs.randomBase64UUIDSecureString();
        assert ApiKey.Type.CROSS_CLUSTER != request.getType() || API_KEY_SECRET_LENGTH == apiKey.length()
            : "Invalid API key (name=[" + request.getName() + "], type=[" + request.getType() + "], length=[" + apiKey.length() + "])";

        computeHashForApiKey(apiKey, listener.delegateFailure((l, apiKeyHashChars) -> {
            try (
                XContentBuilder builder = newDocument(
                    apiKeyHashChars,
                    request.getName(),
                    authentication,
                    userRoleDescriptors,
                    created,
                    expiration,
                    request.getRoleDescriptors(),
                    request.getType(),
                    ApiKey.CURRENT_API_KEY_VERSION,
                    request.getMetadata()
                )
            ) {
                final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                bulkRequestBuilder.add(
                    client.prepareIndex(SECURITY_MAIN_ALIAS)
                        .setSource(builder)
                        .setId(request.getId())
                        .setOpType(DocWriteRequest.OpType.CREATE)
                        .request()
                );
                bulkRequestBuilder.setRefreshPolicy(request.getRefreshPolicy());
                final BulkRequest bulkRequest = bulkRequestBuilder.request();

                securityIndex.prepareIndexIfNeededThenExecute(
                    listener::onFailure,
                    () -> executeAsyncWithOrigin(
                        client,
                        SECURITY_ORIGIN,
                        TransportBulkAction.TYPE,
                        bulkRequest,
                        TransportBulkAction.<IndexResponse>unwrappingSingleItemBulkResponse(ActionListener.wrap(indexResponse -> {
                            assert request.getId().equals(indexResponse.getId())
                                : "Mismatched API key (request=["
                                    + request.getId()
                                    + "](name=["
                                    + request.getName()
                                    + "]) index=["
                                    + indexResponse.getId()
                                    + "])";
                            assert indexResponse.getResult() == DocWriteResponse.Result.CREATED
                                : "Index response was [" + indexResponse.getResult() + "]";
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

    public void updateApiKeys(
        final Authentication authentication,
        final BaseBulkUpdateApiKeyRequest request,
        final Set<RoleDescriptor> userRoleDescriptors,
        final ActionListener<BulkUpdateApiKeyResponse> listener
    ) {
        assert request.getType() != ApiKey.Type.CROSS_CLUSTER || userRoleDescriptors.isEmpty()
            : "owner user role descriptor must be empty for cross-cluster API keys (ids=["
                + (request.getIds().size() <= 10
                    ? request.getIds()
                    : (request.getIds().size() + " including " + request.getIds().subList(0, 10)))
                + "], type=["
                + request.getType()
                + "], roles=["
                + userRoleDescriptors
                + "])";
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(new IllegalArgumentException("authentication must be provided"));
            return;
        } else if (authentication.isApiKey()) {
            listener.onFailure(
                new IllegalArgumentException("authentication via API key not supported: only the owner user can update an API key")
            );
            return;
        }

        final TransportVersion transportVersion = getMinTransportVersion();

        if (validateRoleDescriptorsForMixedCluster(listener, request.getRoleDescriptors(), transportVersion) == false) {
            return;
        }

        final Exception workflowsValidationException = validateWorkflowsRestrictionConstraints(
            transportVersion,
            request.getRoleDescriptors(),
            userRoleDescriptors
        );
        if (workflowsValidationException != null) {
            listener.onFailure(workflowsValidationException);
            return;
        }

        final String[] apiKeyIds = request.getIds().toArray(String[]::new);

        if (logger.isDebugEnabled()) {
            logger.debug("Updating [{}] API keys", buildDelimitedStringWithLimit(10, apiKeyIds));
        }

        Set<RoleDescriptor> filteredRoleDescriptors = filterRoleDescriptorsForMixedCluster(
            userRoleDescriptors,
            transportVersion,
            apiKeyIds
        );

        findVersionedApiKeyDocsForSubject(
            authentication,
            apiKeyIds,
            ActionListener.wrap(
                versionedDocs -> updateApiKeys(authentication, request, filteredRoleDescriptors, versionedDocs, listener),
                ex -> listener.onFailure(traceLog("bulk update", ex))
            )
        );
    }

    private void updateApiKeys(
        final Authentication authentication,
        final BaseBulkUpdateApiKeyRequest request,
        final Set<RoleDescriptor> userRoleDescriptors,
        final Collection<VersionedApiKeyDoc> targetVersionedDocs,
        final ActionListener<BulkUpdateApiKeyResponse> listener
    ) {
        logger.trace("Found [{}] API keys of [{}] requested for update", targetVersionedDocs.size(), request.getIds().size());
        assert targetVersionedDocs.size() <= request.getIds().size()
            : "more docs were found for update than were requested. found ["
                + targetVersionedDocs.size()
                + "] requested ["
                + request.getIds().size()
                + "]";

        final BulkUpdateApiKeyResponse.Builder responseBuilder = BulkUpdateApiKeyResponse.builder();
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (VersionedApiKeyDoc versionedDoc : targetVersionedDocs) {
            final String apiKeyId = versionedDoc.id();
            try {
                validateForUpdate(apiKeyId, request.getType(), authentication, versionedDoc.doc());
                final IndexRequest indexRequest = maybeBuildIndexRequest(versionedDoc, authentication, request, userRoleDescriptors);
                final boolean isNoop = indexRequest == null;
                if (isNoop) {
                    logger.debug("Detected noop update request for API key [{}]. Skipping index request", apiKeyId);
                    responseBuilder.noop(apiKeyId);
                } else {
                    bulkRequestBuilder.add(indexRequest);
                }
            } catch (Exception ex) {
                responseBuilder.error(apiKeyId, traceLog("prepare index request for update", ex));
            }
        }
        addErrorsForNotFoundApiKeys(responseBuilder, targetVersionedDocs, request.getIds());
        if (bulkRequestBuilder.numberOfActions() == 0) {
            logger.trace("No bulk request execution necessary for API key update");
            listener.onResponse(responseBuilder.build());
            return;
        }

        logger.trace("Executing bulk request to update [{}] API keys", bulkRequestBuilder.numberOfActions());
        bulkRequestBuilder.setRefreshPolicy(defaultCreateDocRefreshPolicy(settings));
        securityIndex.prepareIndexIfNeededThenExecute(
            ex -> listener.onFailure(traceLog("prepare security index before update", ex)),
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                bulkRequestBuilder.request(),
                ActionListener.<BulkResponse>wrap(
                    bulkResponse -> buildResponseAndClearCache(bulkResponse, responseBuilder, listener),
                    ex -> listener.onFailure(traceLog("execute bulk request for update", ex))
                ),
                client::bulk
            )
        );
    }

    // package-private for testing
    void validateForUpdate(
        final String apiKeyId,
        final ApiKey.Type expectedType,
        final Authentication authentication,
        final ApiKeyDoc apiKeyDoc
    ) {
        assert authentication.getEffectiveSubject().getUser().principal().equals(apiKeyDoc.creator.get("principal"))
            : "Authenticated user should be owner (authentication=["
                + authentication
                + "], owner=["
                + apiKeyDoc.creator
                + "], id=["
                + apiKeyId
                + "])";

        if (apiKeyDoc.invalidated) {
            throw new IllegalArgumentException("cannot update invalidated API key [" + apiKeyId + "]");
        }

        boolean expired = apiKeyDoc.expirationTime != -1 && clock.instant().isAfter(Instant.ofEpochMilli(apiKeyDoc.expirationTime));
        if (expired) {
            throw new IllegalArgumentException("cannot update expired API key [" + apiKeyId + "]");
        }

        if (Strings.isNullOrEmpty(apiKeyDoc.name)) {
            throw new IllegalArgumentException("cannot update legacy API key [" + apiKeyId + "] without name");
        }

        if (expectedType != apiKeyDoc.type) {
            throw new IllegalArgumentException(
                "cannot update API key of type [" + apiKeyDoc.type.value() + "] while expected type is [" + expectedType.value() + "]"
            );
        }
    }

    /**
     * This method removes remote indices and cluster privileges from the given role descriptors
     * when we are in a mixed cluster in which some of the nodes do not support remote indices/clusters.
     * Storing these roles would cause parsing issues on old nodes
     * (i.e. nodes running with transport version before
     * {@link org.elasticsearch.transport.RemoteClusterPortSettings#TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY}).
     */
    static Set<RoleDescriptor> maybeRemoveRemotePrivileges(
        final Set<RoleDescriptor> userRoleDescriptors,
        final TransportVersion transportVersion,
        final String... apiKeyIds
    ) {
        if (transportVersion.before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)
            || transportVersion.before(ROLE_REMOTE_CLUSTER_PRIVS)) {
            final Set<RoleDescriptor> affectedRoles = new HashSet<>();
            final Set<RoleDescriptor> result = userRoleDescriptors.stream().map(roleDescriptor -> {
                if (roleDescriptor.hasRemoteIndicesPrivileges() || roleDescriptor.hasRemoteClusterPermissions()) {
                    affectedRoles.add(roleDescriptor);
                    return new RoleDescriptor(
                        roleDescriptor.getName(),
                        roleDescriptor.getClusterPrivileges(),
                        roleDescriptor.getIndicesPrivileges(),
                        roleDescriptor.getApplicationPrivileges(),
                        roleDescriptor.getConditionalClusterPrivileges(),
                        roleDescriptor.getRunAs(),
                        roleDescriptor.getMetadata(),
                        roleDescriptor.getTransientMetadata(),
                        roleDescriptor.hasRemoteIndicesPrivileges()
                            && transportVersion.before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)
                                ? null
                                : roleDescriptor.getRemoteIndicesPrivileges(),
                        roleDescriptor.hasRemoteClusterPermissions() && transportVersion.before(ROLE_REMOTE_CLUSTER_PRIVS)
                            ? null
                            : roleDescriptor.getRemoteClusterPermissions(),
                        roleDescriptor.getRestriction(),
                        roleDescriptor.getDescription()
                    );
                }
                return roleDescriptor;
            }).collect(Collectors.toSet());

            if (false == affectedRoles.isEmpty()) {
                List<String> affectedRolesNames = affectedRoles.stream().map(RoleDescriptor::getName).sorted().collect(Collectors.toList());
                if (affectedRoles.stream().anyMatch(RoleDescriptor::hasRemoteIndicesPrivileges)
                    && transportVersion.before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)) {
                    logger.info(
                        "removed remote indices privileges from role(s) {} for API key(s) [{}]",
                        affectedRolesNames,
                        buildDelimitedStringWithLimit(10, apiKeyIds)
                    );
                    HeaderWarning.addWarning(
                        "Removed API key's remote indices privileges from role(s) "
                            + affectedRolesNames
                            + ". Remote indices are not supported by all nodes in the cluster. "
                    );
                }
                if (affectedRoles.stream().anyMatch(RoleDescriptor::hasRemoteClusterPermissions)
                    && transportVersion.before(ROLE_REMOTE_CLUSTER_PRIVS)) {
                    logger.info(
                        "removed remote cluster privileges from role(s) {} for API key(s) [{}]",
                        affectedRolesNames,
                        buildDelimitedStringWithLimit(10, apiKeyIds)
                    );
                    HeaderWarning.addWarning(
                        "Removed API key's remote cluster privileges from role(s) "
                            + affectedRolesNames
                            + ". Remote cluster privileges are not supported by all nodes in the cluster."
                    );
                }
            }
            return result;
        }
        return userRoleDescriptors;
    }

    /**
     * Builds a comma delimited string from the given string values (e.g. value1, value2...).
     * The number of values included can be controlled with the {@code limit}. The limit must be a positive number.
     * Note: package-private for testing
     */
    static String buildDelimitedStringWithLimit(final int limit, final String... values) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive number");
        }
        if (values == null || values.length <= 0) {
            return "";
        }
        final int total = values.length;
        final int omitted = Math.max(0, total - limit);
        final int valuesToAppend = Math.min(limit, total);
        final int capacityForOmittedInfoText = 5; // The number of additional info strings we append when omitting.
        final int capacity = valuesToAppend + (omitted > 0 ? capacityForOmittedInfoText : 0);
        final StringBuilder sb = new StringBuilder(capacity);

        int counter = 0;
        while (counter < valuesToAppend) {
            sb.append(values[counter]);
            counter += 1;
            if (counter < valuesToAppend) {
                sb.append(", ");
            }
        }

        if (omitted > 0) {
            sb.append("... (").append(total).append(" in total, ").append(omitted).append(" omitted)");
        }

        return sb.toString();
    }

    /**
     * package-private for testing
     */
    static XContentBuilder newDocument(
        char[] apiKeyHashChars,
        String name,
        Authentication authentication,
        Set<RoleDescriptor> userRoleDescriptors,
        Instant created,
        Instant expiration,
        List<RoleDescriptor> keyRoleDescriptors,
        ApiKey.Type type,
        ApiKey.Version version,
        @Nullable Map<String, Object> metadata
    ) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
            .field("doc_type", "api_key")
            .field("type", type.value())
            .field("creation_time", created.toEpochMilli())
            .field("expiration_time", expiration == null ? null : expiration.toEpochMilli())
            .field("api_key_invalidated", false);

        addApiKeyHash(builder, apiKeyHashChars);
        addRoleDescriptors(builder, keyRoleDescriptors);
        addLimitedByRoleDescriptors(builder, userRoleDescriptors);

        builder.field("name", name).field("version", version.version()).field("metadata_flattened", metadata);
        addCreator(builder, authentication);

        return builder.endObject();
    }

    // package private for testing

    /**
     * @return `null` if the update is a noop, i.e., if no changes to `currentApiKeyDoc` are required
     */
    @Nullable
    static XContentBuilder maybeBuildUpdatedDocument(
        final String apiKeyId,
        final ApiKeyDoc currentApiKeyDoc,
        final ApiKey.Version targetDocVersion,
        final Authentication authentication,
        final BaseUpdateApiKeyRequest request,
        final Set<RoleDescriptor> userRoleDescriptors,
        final Clock clock
    ) throws IOException {
        assert currentApiKeyDoc.type == request.getType()
            : "API Key doc does not match request type (key-id=["
                + apiKeyId
                + "], doc=["
                + currentApiKeyDoc.type
                + "], request=["
                + request.getType()
                + "])";
        if (isNoop(apiKeyId, currentApiKeyDoc, targetDocVersion, authentication, request, userRoleDescriptors)) {
            return null;
        }

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
            .field("doc_type", "api_key")
            .field("type", currentApiKeyDoc.type.value())
            .field("creation_time", currentApiKeyDoc.creationTime)
            .field("api_key_invalidated", false);

        if (request.getExpiration() != null) {
            builder.field("expiration_time", getApiKeyExpiration(clock.instant(), request.getExpiration()).toEpochMilli());
        } else {
            builder.field("expiration_time", currentApiKeyDoc.expirationTime == -1 ? null : currentApiKeyDoc.expirationTime);
        }

        addApiKeyHash(builder, currentApiKeyDoc.hash.toCharArray());

        final List<RoleDescriptor> keyRoles = request.getRoleDescriptors();

        if (keyRoles != null) {
            logger.trace(() -> format("Building API key doc with updated role descriptors [%s]", keyRoles));
            addRoleDescriptors(builder, keyRoles);
        } else {
            assert currentApiKeyDoc.roleDescriptorsBytes != null : "Role descriptors for [" + apiKeyId + "] are null";
            builder.rawField("role_descriptors", currentApiKeyDoc.roleDescriptorsBytes.streamInput(), XContentType.JSON);
        }

        addLimitedByRoleDescriptors(builder, userRoleDescriptors);

        builder.field("name", currentApiKeyDoc.name).field("version", targetDocVersion.version());

        assert currentApiKeyDoc.metadataFlattened == null
            || MetadataUtils.containsReservedMetadata(
                XContentHelper.convertToMap(currentApiKeyDoc.metadataFlattened, false, XContentType.JSON).v2()
            ) == false : "API key doc [" + apiKeyId + "] to be updated contains reserved metadata";
        final Map<String, Object> metadata = request.getMetadata();
        if (metadata != null) {
            logger.trace(() -> format("Building API key doc with updated metadata [%s]", metadata));
            builder.field("metadata_flattened", metadata);
        } else {
            builder.rawField(
                "metadata_flattened",
                currentApiKeyDoc.metadataFlattened == null
                    ? ApiKeyDoc.NULL_BYTES.streamInput()
                    : currentApiKeyDoc.metadataFlattened.streamInput(),
                XContentType.JSON
            );
        }

        addCreator(builder, authentication);

        return builder.endObject();
    }

    private static boolean isNoop(
        final String apiKeyId,
        final ApiKeyDoc apiKeyDoc,
        final ApiKey.Version targetDocVersion,
        final Authentication authentication,
        final BaseUpdateApiKeyRequest request,
        final Set<RoleDescriptor> userRoleDescriptors
    ) throws IOException {
        if (apiKeyDoc.version != targetDocVersion.version()) {
            return false;
        }

        if (request.getExpiration() != null) {
            // Since expiration is relative current time, it's not likely that it matches the stored value to the ms, so assume update
            return false;
        }

        final Map<String, Object> currentCreator = apiKeyDoc.creator;
        final var user = authentication.getEffectiveSubject().getUser();
        final var sourceRealm = authentication.getEffectiveSubject().getRealm();
        if (false == (Objects.equals(user.principal(), currentCreator.get("principal"))
            && Objects.equals(user.fullName(), currentCreator.get("full_name"))
            && Objects.equals(user.email(), currentCreator.get("email"))
            && Objects.equals(user.metadata(), currentCreator.get("metadata"))
            && Objects.equals(sourceRealm.getName(), currentCreator.get("realm"))
            && Objects.equals(sourceRealm.getType(), currentCreator.get("realm_type")))) {
            return false;
        }
        if (sourceRealm.getDomain() != null) {
            if (currentCreator.get("realm_domain") == null) {
                return false;
            }
            @SuppressWarnings("unchecked")
            var m = (Map<String, Object>) currentCreator.get("realm_domain");
            final RealmDomain currentRealmDomain;
            try (var parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, m)) {
                currentRealmDomain = RealmDomain.fromXContent(parser);
            }
            if (sourceRealm.getDomain().equals(currentRealmDomain) == false) {
                return false;
            }
        } else {
            if (currentCreator.get("realm_domain") != null) {
                return false;
            }
        }

        final Map<String, Object> newMetadata = request.getMetadata();
        if (newMetadata != null) {
            if (apiKeyDoc.metadataFlattened == null) {
                return false;
            }
            final Map<String, Object> currentMetadata = XContentHelper.convertToMap(apiKeyDoc.metadataFlattened, false, XContentType.JSON)
                .v2();
            if (newMetadata.equals(currentMetadata) == false) {
                return false;
            }
        }

        final List<RoleDescriptor> newRoleDescriptors = request.getRoleDescriptors();

        if (newRoleDescriptors != null) {
            final List<RoleDescriptor> currentRoleDescriptors = parseRoleDescriptorsBytes(apiKeyId, apiKeyDoc.roleDescriptorsBytes, false);
            if (false == (newRoleDescriptors.size() == currentRoleDescriptors.size()
                && Set.copyOf(newRoleDescriptors).containsAll(currentRoleDescriptors))) {
                return false;
            }

            if (newRoleDescriptors.size() == currentRoleDescriptors.size()) {
                for (int i = 0; i < currentRoleDescriptors.size(); i++) {
                    // if remote cluster permissions are not equal, then it is not a noop
                    if (currentRoleDescriptors.get(i)
                        .getRemoteClusterPermissions()
                        .equals(newRoleDescriptors.get(i).getRemoteClusterPermissions()) == false) {
                        return false;
                    }
                }
            }
        }

        assert userRoleDescriptors != null : "API Key [" + apiKeyId + "] has null role descriptors";
        final List<RoleDescriptor> currentLimitedByRoleDescriptors = parseRoleDescriptorsBytes(
            apiKeyId,
            apiKeyDoc.limitedByRoleDescriptorsBytes,
            // We want the 7.x `LEGACY_SUPERUSER_ROLE_DESCRIPTOR` role descriptor to be returned here to auto-update
            // `LEGACY_SUPERUSER_ROLE_DESCRIPTOR` to `ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR`, when we update a 7.x API key.
            false
        );
        return (userRoleDescriptors.size() == currentLimitedByRoleDescriptors.size()
            && userRoleDescriptors.containsAll(currentLimitedByRoleDescriptors));
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
                        XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
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
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY.withDeprecationHandler(
                            new ApiKeyLoggingDeprecationHandler(deprecationLogger, apiKeyId)
                        ),
                        BytesReference.bytes(builder),
                        XContentType.JSON
                    )
                ) {
                    return ROLE_DESCRIPTOR_PARSER.parse(name, parser);
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
        return parseRoleDescriptorsBytes(apiKeyId, bytesReference, roleType == RoleReference.ApiKeyRoleType.LIMITED_BY);
    }

    private static List<RoleDescriptor> parseRoleDescriptorsBytes(
        final String apiKeyId,
        BytesReference bytesReference,
        final boolean replaceLegacySuperuserRoleDescriptor
    ) {
        if (bytesReference == null) {
            return Collections.emptyList();
        }

        List<RoleDescriptor> roleDescriptors = new ArrayList<>();
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY.withDeprecationHandler(new ApiKeyLoggingDeprecationHandler(deprecationLogger, apiKeyId)),
                bytesReference,
                XContentType.JSON
            )
        ) {
            parser.nextToken(); // skip outer start object
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                parser.nextToken(); // role name
                String roleName = parser.currentName();
                roleDescriptors.add(ROLE_DESCRIPTOR_PARSER.parse(roleName, parser));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return replaceLegacySuperuserRoleDescriptor ? maybeReplaceSuperuserRoleDescriptor(apiKeyId, roleDescriptors) : roleDescriptors;
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
            listener.onResponse(AuthenticationResult.unsuccessful("api key [" + credentials.getId() + "] has been invalidated", null));
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
                                validateApiKeyTypeAndExpiration(apiKeyDoc, credentials, clock, listener);
                            } else {
                                listener.onResponse(
                                    AuthenticationResult.unsuccessful("invalid credentials for API key [" + credentials.getId() + "]", null)
                                );
                            }
                        } else if (result.verify(credentials.getKey())) { // same key, pass the same result
                            listener.onResponse(
                                AuthenticationResult.unsuccessful("invalid credentials for API key [" + credentials.getId() + "]", null)
                            );
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
                            validateApiKeyTypeAndExpiration(apiKeyDoc, credentials, clock, listener);
                        } else {
                            listener.onResponse(
                                AuthenticationResult.unsuccessful("invalid credentials for API key [" + credentials.getId() + "]", null)
                            );
                        }
                    }, exception -> {
                        // Crypto threadpool queue is full, invalidate this cache entry and make sure nothing is going to wait on it
                        logger.warn(
                            Strings.format(
                                "rejecting possibly valid API key authentication because the [%s] threadpool is full",
                                SECURITY_CRYPTO_THREAD_POOL_NAME
                            )
                        );
                        apiKeyAuthCache.invalidate(credentials.getId(), listenableCacheEntry);
                        listenableCacheEntry.onFailure(exception);
                        listener.onFailure(exception);
                    }));
                }
            } else {
                verifyKeyAgainstHash(apiKeyDoc.hash, credentials, ActionListener.wrap(verified -> {
                    if (verified) {
                        // move on
                        validateApiKeyTypeAndExpiration(apiKeyDoc, credentials, clock, listener);
                    } else {
                        listener.onResponse(
                            AuthenticationResult.unsuccessful("invalid credentials for API key [" + credentials.getId() + "]", null)
                        );
                    }
                }, listener::onFailure));
            }
        }
    }

    // pkg private for testing
    CachedApiKeyHashResult getFromCache(String id) {
        return apiKeyAuthCache == null ? null : apiKeyAuthCache.get(id).result();
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
    static void validateApiKeyTypeAndExpiration(
        ApiKeyDoc apiKeyDoc,
        ApiKeyCredentials credentials,
        Clock clock,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        if (apiKeyDoc.type != credentials.expectedType) {
            listener.onResponse(
                AuthenticationResult.terminate(
                    Strings.format(
                        "authentication expected API key type of [%s], but API key [%s] has type [%s]",
                        credentials.expectedType.value(),
                        credentials.getId(),
                        apiKeyDoc.type.value()
                    )
                )
            );
            return;
        }

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
            authResultMetadata.put(AuthenticationField.API_KEY_TYPE_KEY, apiKeyDoc.type.value());
            if (apiKeyDoc.metadataFlattened != null) {
                authResultMetadata.put(AuthenticationField.API_KEY_METADATA_KEY, apiKeyDoc.metadataFlattened);
            }
            listener.onResponse(AuthenticationResult.success(apiKeyUser, authResultMetadata));
        } else {
            listener.onResponse(AuthenticationResult.unsuccessful("api key is expired", null));
        }
    }

    ApiKeyCredentials parseCredentialsFromApiKeyString(SecureString apiKeyString) {
        if (false == isEnabled()) {
            return null;
        }
        return parseApiKey(apiKeyString, ApiKey.Type.REST);
    }

    static ApiKeyCredentials getCredentialsFromHeader(final String header, ApiKey.Type expectedType) {
        return parseApiKey(Authenticator.extractCredentialFromHeaderValue(header, "ApiKey"), expectedType);
    }

    public static String withApiKeyPrefix(final String encodedApiKey) {
        return "ApiKey " + encodedApiKey;
    }

    private static ApiKeyCredentials parseApiKey(SecureString apiKeyString, ApiKey.Type expectedType) {
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
                final int secretStartPos = colonIndex + 1;
                if (ApiKey.Type.CROSS_CLUSTER == expectedType && API_KEY_SECRET_LENGTH != apiKeyCredChars.length - secretStartPos) {
                    throw new IllegalArgumentException("invalid cross-cluster API key value");
                }
                return new ApiKeyCredentials(
                    new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, secretStartPos, apiKeyCredChars.length)),
                    expectedType
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

    private static Instant getApiKeyExpiration(Instant now, @Nullable TimeValue expiration) {
        if (expiration != null) {
            return now.plusSeconds(expiration.getSeconds());
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

    public void crossClusterApiKeyUsageStats(ActionListener<Map<String, Object>> listener) {
        if (false == isEnabled()) {
            listener.onResponse(Map.of());
            return;
        }
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(Map.of("total", 0, "ccs", 0, "ccr", 0, "ccs_ccr", 0));
        } else if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("doc_type", "api_key"))
                .filter(QueryBuilders.termQuery("type", ApiKey.Type.CROSS_CLUSTER.value()));
            findApiKeys(boolQuery, true, true, this::convertSearchHitToApiKeyInfo, ActionListener.wrap(apiKeyInfos -> {
                int ccsKeys = 0, ccrKeys = 0, ccsCcrKeys = 0;
                for (ApiKey apiKeyInfo : apiKeyInfos) {
                    assert apiKeyInfo.getType() == ApiKey.Type.CROSS_CLUSTER
                        : "Incorrect API Key type for [" + apiKeyInfo + "] should be [" + ApiKey.Type.CROSS_CLUSTER + "]";
                    assert apiKeyInfo.getRoleDescriptors().size() == 1
                        : "API Key ["
                            + apiKeyInfo
                            + "] has ["
                            + apiKeyInfo.getRoleDescriptors().size()
                            + "] role descriptors, but should be 1";

                    final List<String> clusterPrivileges = Arrays.asList(
                        apiKeyInfo.getRoleDescriptors().iterator().next().getClusterPrivileges()
                    );

                    if (clusterPrivileges.contains("cross_cluster_search")
                        && clusterPrivileges.contains("cross_cluster_replication") == false) {
                        ccsKeys += 1;
                    } else if (clusterPrivileges.contains("cross_cluster_replication")
                        && clusterPrivileges.contains("cross_cluster_search") == false) {
                            ccrKeys += 1;
                        } else if (clusterPrivileges.contains("cross_cluster_search")
                            && clusterPrivileges.contains("cross_cluster_replication")) {
                                ccsCcrKeys += 1;
                            } else {
                                final String message = "invalid cluster privileges "
                                    + clusterPrivileges
                                    + " for cross-cluster API key ["
                                    + apiKeyInfo.getId()
                                    + "]";
                                assert false : message;
                                listener.onFailure(new IllegalStateException(message));
                            }
                }
                listener.onResponse(Map.of("total", apiKeyInfos.size(), "ccs", ccsKeys, "ccr", ccrKeys, "ccs_ccr", ccsCcrKeys));
            }, listener::onFailure));
        }
    }

    @Override
    public void close() {
        cacheMetrics.forEach(metric -> {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        });
    }

    // public class for testing
    public static final class ApiKeyCredentials implements AuthenticationToken, Closeable {
        private final String id;
        private final SecureString key;
        private final ApiKey.Type expectedType;

        public ApiKeyCredentials(String id, SecureString key, ApiKey.Type expectedType) {
            this.id = id;
            this.key = key;
            this.expectedType = expectedType;
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

        public ApiKey.Type getExpectedType() {
            return expectedType;
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
     * @return `null` if the update is a noop, i.e., if no changes to `currentApiKeyDoc` are required
     */
    @Nullable
    private IndexRequest maybeBuildIndexRequest(
        final VersionedApiKeyDoc currentVersionedDoc,
        final Authentication authentication,
        final BaseUpdateApiKeyRequest request,
        final Set<RoleDescriptor> userRoleDescriptors
    ) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace(
                "Building index request for update of API key doc [{}] with seqNo [{}] and primaryTerm [{}]",
                currentVersionedDoc.id(),
                currentVersionedDoc.seqNo(),
                currentVersionedDoc.primaryTerm()
            );
        }
        final var targetDocVersion = ApiKey.CURRENT_API_KEY_VERSION;
        final var currentDocVersion = new ApiKey.Version(currentVersionedDoc.doc().version);
        assert currentDocVersion.onOrBefore(targetDocVersion)
            : "API key ["
                + currentVersionedDoc.id()
                + "] has version ["
                + currentDocVersion
                + " which is greater than current version ["
                + ApiKey.CURRENT_API_KEY_VERSION
                + "]";
        if (logger.isDebugEnabled() && currentDocVersion.before(targetDocVersion)) {
            logger.debug(
                "API key update for [{}] will update version from [{}] to [{}]",
                currentVersionedDoc.id(),
                currentDocVersion,
                targetDocVersion
            );
        }
        final XContentBuilder builder = maybeBuildUpdatedDocument(
            currentVersionedDoc.id(),
            currentVersionedDoc.doc(),
            targetDocVersion,
            authentication,
            request,
            userRoleDescriptors,
            clock
        );
        final boolean isNoop = builder == null;
        return isNoop
            ? null
            : client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(currentVersionedDoc.id())
                .setSource(builder)
                .setIfSeqNo(currentVersionedDoc.seqNo())
                .setIfPrimaryTerm(currentVersionedDoc.primaryTerm())
                .setOpType(DocWriteRequest.OpType.INDEX)
                .request();
    }

    private static void addErrorsForNotFoundApiKeys(
        final BulkUpdateApiKeyResponse.Builder responseBuilder,
        final Collection<VersionedApiKeyDoc> foundDocs,
        final List<String> requestedIds
    ) {
        // Short-circuiting by size is safe: `foundDocs` only contains unique IDs of those requested. Same size here necessarily implies
        // same content
        if (foundDocs.size() == requestedIds.size()) {
            return;
        }
        final Set<String> foundIds = foundDocs.stream().map(VersionedApiKeyDoc::id).collect(Collectors.toUnmodifiableSet());
        for (String id : requestedIds) {
            if (foundIds.contains(id) == false) {
                responseBuilder.error(id, new ResourceNotFoundException("no API key owned by requesting user found for ID [" + id + "]"));
            }
        }
    }

    /**
     * Invalidate API keys for given realm, user name, API key name and id.
     * @param realmNames realm names
     * @param username username
     * @param apiKeyName API key name
     * @param apiKeyIds API key ids
     * @param includeCrossClusterApiKeys whether to include cross-cluster api keys in the invalidation; if false any cross-cluster api keys
     *                                   will be skipped. skipped API keys will be included in the error details of the response
     * @param invalidateListener listener for {@link InvalidateApiKeyResponse}
     */
    public void invalidateApiKeys(
        String[] realmNames,
        String username,
        String apiKeyName,
        String[] apiKeyIds,
        boolean includeCrossClusterApiKeys,
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
                this::convertSearchHitToApiKeyInfo,
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
                        indexInvalidation(apiKeys, includeCrossClusterApiKeys, invalidateListener);
                    }
                }, invalidateListener::onFailure)
            );
        }
    }

    private <T> void findApiKeys(
        final BoolQueryBuilder boolQuery,
        boolean filterOutInvalidatedKeys,
        boolean filterOutExpiredKeys,
        final Function<SearchHit, T> hitParser,
        final ActionListener<Collection<T>> listener
    ) {
        if (filterOutInvalidatedKeys) {
            boolQuery.filter(QueryBuilders.termQuery("api_key_invalidated", false));
        }
        if (filterOutExpiredKeys) {
            final BoolQueryBuilder expiredQuery = QueryBuilders.boolQuery();
            expiredQuery.should(QueryBuilders.rangeQuery("expiration_time").gt(clock.instant().toEpochMilli()));
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
                () -> ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(supplier, listener), hitParser)
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

    private void findVersionedApiKeyDocsForSubject(
        final Authentication authentication,
        final String[] apiKeyIds,
        final ActionListener<Collection<VersionedApiKeyDoc>> listener
    ) {
        assert authentication.isApiKey() == false : "Authentication [" + authentication + "] is an API key, but should not be";
        findApiKeysForUserRealmApiKeyIdAndNameCombination(
            getOwnersRealmNames(authentication),
            authentication.getEffectiveSubject().getUser().principal(),
            null,
            apiKeyIds,
            false,
            false,
            ApiKeyService::convertSearchHitToVersionedApiKeyDoc,
            listener
        );
    }

    private <T> void findApiKeysForUserRealmApiKeyIdAndNameCombination(
        String[] realmNames,
        String userName,
        String apiKeyName,
        String[] apiKeyIds,
        boolean filterOutInvalidatedKeys,
        boolean filterOutExpiredKeys,
        Function<SearchHit, T> hitParser,
        ActionListener<Collection<T>> listener
    ) {
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
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

            findApiKeys(boolQuery, filterOutInvalidatedKeys, filterOutExpiredKeys, hitParser, listener);
        }
    }

    /**
     * Performs the actual invalidation of a collection of api keys
     *
     * @param apiKeys the api keys to invalidate
     * @param includeCrossClusterApiKeys whether to include cross-cluster api keys in the invalidation; if false any cross-cluster api keys
     *                                   will be skipped. skipped API keys will be included in the error details of the response
     * @param listener  the listener to notify upon completion
     */
    private void indexInvalidation(
        Collection<ApiKey> apiKeys,
        boolean includeCrossClusterApiKeys,
        ActionListener<InvalidateApiKeyResponse> listener
    ) {
        maybeStartApiKeyRemover();
        if (apiKeys.isEmpty()) {
            listener.onFailure(new ElasticsearchSecurityException("No api key ids provided for invalidation"));
        } else {
            final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            final long invalidationTime = clock.instant().toEpochMilli();
            final Set<String> apiKeyIdsToInvalidate = new HashSet<>();
            final Set<String> crossClusterApiKeyIdsToSkip = new HashSet<>();
            final ArrayList<ElasticsearchException> failedRequestResponses = new ArrayList<>();
            for (ApiKey apiKey : apiKeys) {
                final String apiKeyId = apiKey.getId();
                if (apiKeyIdsToInvalidate.contains(apiKeyId) || crossClusterApiKeyIdsToSkip.contains(apiKeyId)) {
                    continue;
                }
                if (false == includeCrossClusterApiKeys && ApiKey.Type.CROSS_CLUSTER.equals(apiKey.getType())) {
                    logger.debug("Skipping invalidation of cross cluster API key [{}]", apiKey);
                    failedRequestResponses.add(cannotInvalidateCrossClusterApiKeyException(apiKeyId));
                    crossClusterApiKeyIdsToSkip.add(apiKeyId);
                } else {
                    final UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(SECURITY_MAIN_ALIAS, apiKeyId)
                        .setDoc(Map.of("api_key_invalidated", true, "invalidation_time", invalidationTime));
                    bulkRequestBuilder.add(updateRequestBuilder);
                    apiKeyIdsToInvalidate.add(apiKeyId);
                }
            }

            // noinspection ConstantValue
            assert false == apiKeyIdsToInvalidate.isEmpty() || false == crossClusterApiKeyIdsToSkip.isEmpty()
                : "There are no API keys but that should never happen, original=["
                    + (apiKeys.size() > 10 ? ("size=" + apiKeys.size() + " including " + apiKeys.iterator().next()) : apiKeys)
                    + "], to-invalidate=["
                    + apiKeyIdsToInvalidate
                    + "], to-skip=["
                    + crossClusterApiKeyIdsToSkip
                    + "]";

            if (apiKeyIdsToInvalidate.isEmpty()) {
                listener.onResponse(new InvalidateApiKeyResponse(Collections.emptyList(), Collections.emptyList(), failedRequestResponses));
                return;
            }
            assert bulkRequestBuilder.numberOfActions() > 0
                : "Bulk request has ["
                    + bulkRequestBuilder.numberOfActions()
                    + "] actions, but there are ["
                    + apiKeyIdsToInvalidate.size()
                    + "] api keys to invalidate";

            bulkRequestBuilder.setRefreshPolicy(defaultCreateDocRefreshPolicy(settings));
            securityIndex.prepareIndexIfNeededThenExecute(
                ex -> listener.onFailure(traceLog("prepare security index", ex)),
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    bulkRequestBuilder.request(),
                    ActionListener.<BulkResponse>wrap(bulkResponse -> {
                        ArrayList<String> previouslyInvalidated = new ArrayList<>();
                        ArrayList<String> invalidated = new ArrayList<>();
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

    private ElasticsearchException cannotInvalidateCrossClusterApiKeyException(String apiKeyId) {
        return new ElasticsearchSecurityException(
            "Cannot invalidate cross-cluster API key ["
                + apiKeyId
                + "]. This requires ["
                + ClusterPrivilegeResolver.MANAGE_SECURITY.name()
                + "] cluster privilege or higher"
        );
    }

    private void buildResponseAndClearCache(
        final BulkResponse bulkResponse,
        final BulkUpdateApiKeyResponse.Builder responseBuilder,
        final ActionListener<BulkUpdateApiKeyResponse> listener
    ) {
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            final String apiKeyId = bulkItemResponse.getId();
            if (bulkItemResponse.isFailed()) {
                responseBuilder.error(
                    apiKeyId,
                    new ElasticsearchException("bulk request execution failure", bulkItemResponse.getFailure().getCause())
                );
            } else {
                // Since we made an index request against an existing document, we can't get a NOOP or CREATED here
                assert bulkItemResponse.getResponse().getResult() == DocWriteResponse.Result.UPDATED
                    : "Bulk Item ["
                        + bulkItemResponse.getId()
                        + "] is ["
                        + bulkItemResponse.getResponse().getResult()
                        + "] but should be ["
                        + DocWriteResponse.Result.UPDATED
                        + "]";
                responseBuilder.updated(apiKeyId);
            }
        }
        clearApiKeyDocCache(responseBuilder.build(), listener);
    }

    private static void addLimitedByRoleDescriptors(final XContentBuilder builder, final Set<RoleDescriptor> limitedByRoleDescriptors)
        throws IOException {
        assert limitedByRoleDescriptors != null;
        builder.startObject("limited_by_role_descriptors");
        for (RoleDescriptor descriptor : limitedByRoleDescriptors) {
            builder.field(descriptor.getName(), (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
        }
        builder.endObject();
    }

    private static void addApiKeyHash(final XContentBuilder builder, final char[] apiKeyHashChars) throws IOException {
        byte[] utf8Bytes = null;
        try {
            utf8Bytes = CharArrays.toUtf8Bytes(apiKeyHashChars);
            builder.field("api_key_hash").utf8Value(utf8Bytes, 0, utf8Bytes.length);
        } finally {
            if (utf8Bytes != null) {
                Arrays.fill(utf8Bytes, (byte) 0);
            }
        }
    }

    private static void addCreator(final XContentBuilder builder, final Authentication authentication) throws IOException {
        final var user = authentication.getEffectiveSubject().getUser();
        final var sourceRealm = authentication.getEffectiveSubject().getRealm();
        builder.startObject("creator")
            .field("principal", user.principal())
            .field("full_name", user.fullName())
            .field("email", user.email())
            .field("metadata", user.metadata())
            .field("realm", sourceRealm.getName())
            .field("realm_type", sourceRealm.getType());
        if (sourceRealm.getDomain() != null) {
            builder.field("realm_domain", sourceRealm.getDomain());
        }
        builder.endObject();
    }

    private static void addRoleDescriptors(final XContentBuilder builder, final List<RoleDescriptor> keyRoles) throws IOException {
        builder.startObject("role_descriptors");
        if (keyRoles != null && keyRoles.isEmpty() == false) {
            for (RoleDescriptor descriptor : keyRoles) {
                builder.field(descriptor.getName(), (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
            }
        }
        builder.endObject();
    }

    private void clearCache(InvalidateApiKeyResponse result, ActionListener<InvalidateApiKeyResponse> listener) {
        executeClearCacheRequest(
            result,
            listener,
            new ClearSecurityCacheRequest().cacheName("api_key").keys(result.getInvalidatedApiKeys().toArray(String[]::new))
        );
    }

    private void clearApiKeyDocCache(final BulkUpdateApiKeyResponse result, final ActionListener<BulkUpdateApiKeyResponse> listener) {
        executeClearCacheRequest(
            result,
            listener,
            new ClearSecurityCacheRequest().cacheName("api_key_doc").keys(result.getUpdated().toArray(String[]::new))
        );
    }

    private <T> void executeClearCacheRequest(T result, ActionListener<T> listener, ClearSecurityCacheRequest clearApiKeyCacheRequest) {
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearSecurityCacheAction.INSTANCE, clearApiKeyCacheRequest, new ActionListener<>() {
            @Override
            public void onResponse(ClearSecurityCacheResponse nodes) {
                listener.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> format("unable to clear API key cache [%s]", clearApiKeyCacheRequest.cacheName()), e);
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
                    logger.trace(() -> format("Failure in [%s] for id [%s] - [%s]", action, identifier, detail), esEx);
                } else {
                    logger.trace(() -> format("Failure in [%s] for id [%s]", action, identifier), esEx);
                }
            } else {
                logger.trace(() -> format("Failure in [%s] for id [%s]", action, identifier), exception);
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
                    logger.trace(() -> format("Failure in [%s] - [%s]", action, detail), esEx);
                } else {
                    logger.trace(() -> "Failure in [" + action + "]", esEx);
                }
            } else {
                logger.trace(() -> "Failure in [" + action + "]", exception);
            }
        }
        return exception;
    }

    // pkg scoped for testing
    boolean isExpirationInProgress() {
        return inactiveApiKeysRemover.isExpirationInProgress();
    }

    // pkg scoped for testing
    long lastTimeWhenApiKeysRemoverWasTriggered() {
        return inactiveApiKeysRemover.getLastRunTimestamp();
    }

    private void maybeStartApiKeyRemover() {
        if (securityIndex.isAvailable(PRIMARY_SHARDS)) {
            inactiveApiKeysRemover.maybeSubmit(client.threadPool());
        }
    }

    /**
     * Get API key information for given realm, user, API key name and id combination
     * @param realmNames realm names
     * @param username user name
     * @param apiKeyName API key name
     * @param apiKeyIds API key ids
     * @param withLimitedBy whether to parse and return the limited by role descriptors
     * @param listener receives the requested collection of {@link ApiKey}s
     */
    public void getApiKeys(
        String[] realmNames,
        String username,
        String apiKeyName,
        String[] apiKeyIds,
        boolean withLimitedBy,
        boolean activeOnly,
        ActionListener<Collection<ApiKey>> listener
    ) {
        ensureEnabled();
        findApiKeysForUserRealmApiKeyIdAndNameCombination(
            realmNames,
            username,
            apiKeyName,
            apiKeyIds,
            activeOnly,
            activeOnly,
            hit -> convertSearchHitToApiKeyInfo(hit, withLimitedBy),
            ActionListener.wrap(apiKeyInfos -> {
                if (apiKeyInfos.isEmpty() && logger.isDebugEnabled()) {
                    logger.debug(
                        "No API keys found for realms {}, user [{}], API key name [{}], API key IDs {}, and active_only flag [{}]",
                        Arrays.toString(realmNames),
                        username,
                        apiKeyName,
                        Arrays.toString(apiKeyIds),
                        activeOnly
                    );
                }
                listener.onResponse(apiKeyInfos);
            }, listener::onFailure)
        );
    }

    public record QueryApiKeysResult(
        long total,
        Collection<ApiKey> apiKeyInfos,
        Collection<Object[]> sortValues,
        @Nullable InternalAggregations aggregations
    ) {
        static final QueryApiKeysResult EMPTY = new QueryApiKeysResult(0, List.of(), List.of(), null);
    }

    public void queryApiKeys(SearchRequest searchRequest, boolean withLimitedBy, ActionListener<QueryApiKeysResult> listener) {
        ensureEnabled();
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(QueryApiKeysResult.EMPTY);
        } else if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            securityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportSearchAction.TYPE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        long total = searchResponse.getHits().getTotalHits().value;
                        if (total == 0) {
                            logger.debug("No api keys found for query [{}]", searchRequest.source().query());
                            listener.onResponse(QueryApiKeysResult.EMPTY);
                            return;
                        }
                        SearchHit[] hits = searchResponse.getHits().getHits();
                        List<ApiKey> apiKeyInfos = Arrays.stream(hits)
                            .map(hit -> convertSearchHitToApiKeyInfo(hit, withLimitedBy))
                            .toList();
                        List<Object[]> sortValues = Arrays.stream(hits).map(SearchHit::getSortValues).toList();
                        listener.onResponse(new QueryApiKeysResult(total, apiKeyInfos, sortValues, searchResponse.getAggregations()));
                    }, listener::onFailure)
                )
            );
        }
    }

    private ApiKey convertSearchHitToApiKeyInfo(SearchHit hit) {
        return convertSearchHitToApiKeyInfo(hit, false);
    }

    private ApiKey convertSearchHitToApiKeyInfo(SearchHit hit, boolean withLimitedBy) {
        final ApiKeyDoc apiKeyDoc = convertSearchHitToVersionedApiKeyDoc(hit).doc;
        final String apiKeyId = hit.getId();
        final Map<String, Object> metadata = apiKeyDoc.metadataFlattened != null
            ? XContentHelper.convertToMap(apiKeyDoc.metadataFlattened, false, XContentType.JSON).v2()
            : Map.of();

        final List<RoleDescriptor> roleDescriptors = parseRoleDescriptorsBytes(
            apiKeyId,
            apiKeyDoc.roleDescriptorsBytes,
            RoleReference.ApiKeyRoleType.ASSIGNED
        );

        final List<RoleDescriptor> limitedByRoleDescriptors = (withLimitedBy && apiKeyDoc.type != ApiKey.Type.CROSS_CLUSTER)
            ? parseRoleDescriptorsBytes(apiKeyId, apiKeyDoc.limitedByRoleDescriptorsBytes, RoleReference.ApiKeyRoleType.LIMITED_BY)
            : null;

        return new ApiKey(
            apiKeyDoc.name,
            apiKeyId,
            apiKeyDoc.type,
            Instant.ofEpochMilli(apiKeyDoc.creationTime),
            apiKeyDoc.expirationTime != -1 ? Instant.ofEpochMilli(apiKeyDoc.expirationTime) : null,
            apiKeyDoc.invalidated,
            apiKeyDoc.invalidation != -1 ? Instant.ofEpochMilli(apiKeyDoc.invalidation) : null,
            (String) apiKeyDoc.creator.get("principal"),
            (String) apiKeyDoc.creator.get("realm"),
            (String) apiKeyDoc.creator.get("realm_type"),
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors
        );
    }

    private static VersionedApiKeyDoc convertSearchHitToVersionedApiKeyDoc(SearchHit hit) {
        try (
            XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, hit.getSourceRef(), XContentType.JSON)
        ) {
            return new VersionedApiKeyDoc(ApiKeyDoc.fromXContent(parser), hit.getId(), hit.getSeqNo(), hit.getPrimaryTerm());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private record VersionedApiKeyDoc(ApiKeyDoc doc, String id, long seqNo, long primaryTerm) {}

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
        if (authentication.isApiKey() || authentication.isCrossClusterAccess()) {
            return (String) authentication.getEffectiveSubject().getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME);
        } else {
            // TODO we should use the effective subject realm here but need to handle the failed lookup scenario, in which the realm may be
            // `null`. Since this method is used in audit logging, this requires some care.
            if (authentication.isFailedRunAs()) {
                return authentication.getAuthenticatingSubject().getRealm().getName();
            } else {
                return authentication.getEffectiveSubject().getRealm().getName();
            }
        }
    }

    /**
     * Returns the realm names that the username can access resources across.
     */
    public static String[] getOwnersRealmNames(final Authentication authentication) {
        if (authentication.isApiKey()) {
            return new String[] {
                (String) authentication.getEffectiveSubject().getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_NAME) };
        } else {
            final Authentication.RealmRef effectiveSubjectRealm = authentication.getEffectiveSubject().getRealm();
            // The effective subject realm can only be `null` when run-as lookup fails. The owner is always the effective subject, so there
            // is no owner information to return here
            if (effectiveSubjectRealm == null) {
                final var message =
                    "Cannot determine owner realms without an effective subject realm for non-API key authentication object ["
                        + authentication
                        + "]";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            final RealmDomain domain = effectiveSubjectRealm.getDomain();
            if (domain != null) {
                return domain.realms().stream().map(RealmConfig.RealmIdentifier::getName).toArray(String[]::new);
            } else {
                return new String[] { effectiveSubjectRealm.getName() };
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
            return (String) authentication.getEffectiveSubject().getMetadata().get(AuthenticationField.API_KEY_CREATOR_REALM_TYPE);
        } else {
            // TODO we should use the effective subject realm here but need to handle the failed lookup scenario, in which the realm may be
            // `null`. Since this method is used in audit logging, this requires some care.
            if (authentication.isFailedRunAs()) {
                return authentication.getAuthenticatingSubject().getRealm().getType();
            } else {
                return authentication.getEffectiveSubject().getRealm().getType();
            }
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
                    + authentication.getEffectiveSubject().getRealm().getType()
                    + "]"
            );
        }
        final Object apiKeyMetadata = authentication.getEffectiveSubject().getMetadata().get(AuthenticationField.API_KEY_METADATA_KEY);
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
            builder.declareField(
                optionalConstructorArg(),
                ApiKey.Type::fromXContent,
                new ParseField("type"),
                ObjectParser.ValueType.STRING
            );
            builder.declareLong(constructorArg(), new ParseField("creation_time"));
            builder.declareLongOrNull(constructorArg(), -1, new ParseField("expiration_time"));
            builder.declareBoolean(constructorArg(), new ParseField("api_key_invalidated"));
            builder.declareLong(optionalConstructorArg(), new ParseField("invalidation_time"));
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
        final ApiKey.Type type;
        final long creationTime;
        final long expirationTime;
        final Boolean invalidated;
        final long invalidation;
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
            ApiKey.Type type,
            long creationTime,
            long expirationTime,
            Boolean invalidated,
            @Nullable Long invalidation,
            String hash,
            @Nullable String name,
            int version,
            BytesReference roleDescriptorsBytes,
            BytesReference limitedByRoleDescriptorsBytes,
            Map<String, Object> creator,
            @Nullable BytesReference metadataFlattened
        ) {
            this.docType = docType;
            if (type == null) {
                logger.trace("API key document with [null] type defaults to [rest] type");
                this.type = ApiKey.Type.REST;
            } else {
                this.type = type;
            }
            this.creationTime = creationTime;
            this.expirationTime = expirationTime;
            this.invalidated = invalidated;
            this.invalidation = (invalidation == null) ? -1 : invalidation;
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
                type,
                creationTime,
                expirationTime,
                invalidated,
                invalidation,
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
        final ApiKey.Type type;
        final long creationTime;
        final long expirationTime;
        final Boolean invalidated;
        final long invalidation;
        final String hash;
        final String name;
        final int version;
        final Map<String, Object> creator;
        final String roleDescriptorsHash;
        final String limitedByRoleDescriptorsHash;
        @Nullable
        final BytesReference metadataFlattened;

        public CachedApiKeyDoc(
            ApiKey.Type type,
            long creationTime,
            long expirationTime,
            Boolean invalidated,
            long invalidation,
            String hash,
            String name,
            int version,
            Map<String, Object> creator,
            String roleDescriptorsHash,
            String limitedByRoleDescriptorsHash,
            @Nullable BytesReference metadataFlattened
        ) {
            this.type = type;
            this.creationTime = creationTime;
            this.expirationTime = expirationTime;
            this.invalidated = invalidated;
            this.invalidation = invalidation;
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
                type,
                creationTime,
                expirationTime,
                invalidated,
                invalidation,
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

    /**
     * API Key documents are refreshed after creation, such that the API Key docs are visible in searches after the create-API-key
     * endpoint returns.
     * In stateful deployments, the automatic refresh interval is short (hard-coded to 1 sec), so the {@code RefreshPolicy#WAIT_UNTIL}
     * is an acceptable tradeoff for the superior doc creation throughput compared to {@code RefreshPolicy#IMMEDIATE}.
     * But in stateless the automatic refresh interval is too long (at least 10 sec), which translates to long create-API-key endpoint
     * latency, so in this case we opt for {@code RefreshPolicy#IMMEDIATE} and acknowledge the lower maximum doc creation throughput.
     */
    public static RefreshPolicy defaultCreateDocRefreshPolicy(Settings settings) {
        return DiscoveryNode.isStateless(settings) ? RefreshPolicy.IMMEDIATE : RefreshPolicy.WAIT_UNTIL;
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
