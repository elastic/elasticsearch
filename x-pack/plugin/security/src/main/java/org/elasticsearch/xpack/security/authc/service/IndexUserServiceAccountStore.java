/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.service.PutServiceAccountRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

public class IndexUserServiceAccountStore implements CacheInvalidatorRegistry.CacheInvalidator {

    public static final String CACHE_NAME = "index_user_service_account";
    static final String USER_SERVICE_ACCOUNT_DOC_TYPE = "user_service_account";

    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.authc.user_service_account.cache.ttl",
        TimeValue.timeValueMinutes(20),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> CACHE_MAX_ENTRIES_SETTING = Setting.intSetting(
        "xpack.security.authc.user_service_account.cache.max_entries",
        10_000,
        Setting.Property.NodeScope
    );

    private final Settings settings;
    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final Cache<String, RoleDescriptor> cache;

    @SuppressWarnings("this-escape")
    public IndexUserServiceAccountStore(
        Settings settings,
        Clock clock,
        Client client,
        SecurityIndexManager securityIndex,
        CacheInvalidatorRegistry cacheInvalidatorRegistry
    ) {
        this.settings = settings;
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            this.cache = CacheBuilder.<String, RoleDescriptor>builder()
                .setExpireAfterWrite(ttl)
                .setMaximumWeight(CACHE_MAX_ENTRIES_SETTING.get(settings))
                .build();
        } else {
            this.cache = null;
        }
        cacheInvalidatorRegistry.registerCacheInvalidator(CACHE_NAME, this);
    }

    public void putAccount(Authentication authentication, PutServiceAccountRequest request, ActionListener<Boolean> listener) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        try (XContentBuilder builder = newDocument(authentication, accountId, request.getRoleDescriptor())) {
            final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(docIdForPrincipal(accountId.asPrincipal()))
                .setSource(builder)
                .setOpType(OpType.INDEX)
                .setRefreshPolicy(request.getRefreshPolicy())
                .request();
            final BulkRequest bulkRequest = toSingleItemBulkRequest(indexRequest);

            securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
                executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportBulkAction.TYPE,
                    bulkRequest,
                    TransportBulkAction.<IndexResponse>unwrappingSingleItemBulkResponse(ActionListener.wrap(response -> {
                        clearCacheForPrincipal(
                            accountId.asPrincipal(),
                            ActionListener.wrap(ignored -> { listener.onResponse(true); }, listener::onFailure)
                        );
                    }, listener::onFailure))
                );
            });
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * Asynchronously look up a user-defined service account by its principal (e.g. {@code myorg/payments}).
     * Resolves to {@code null} if no such user-defined account exists.
     */
    public void getAccount(String principal, ActionListener<RoleDescriptor> listener) {
        if (cache != null) {
            final RoleDescriptor cached = cache.get(principal);
            if (cached != null) {
                listener.onResponse(cached);
                return;
            }
        }

        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(null);
            return;
        }
        if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
            return;
        }

        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final GetRequest getRequest = client.prepareGet(SECURITY_MAIN_ALIAS, docIdForPrincipal(principal))
                .setFetchSource(true)
                .request();
            executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                TransportGetAction.TYPE,
                getRequest,
                ActionListener.<GetResponse>wrap(response -> {
                    if (response.isExists() == false) {
                        listener.onResponse(null);
                        return;
                    }
                    final RoleDescriptor parsed = parseRoleDescriptorFromSource(principal, response.getSourceAsBytesRef());
                    if (parsed != null && cache != null) {
                        cache.put(principal, parsed);
                    }
                    listener.onResponse(parsed);
                }, listener::onFailure)
            );
        });
    }

    /**
     * Asynchronously list all user-defined service accounts. Used by the GET API when the caller
     * opts in to including user-defined accounts.
     */
    public void findAllAccounts(ActionListener<Collection<UserServiceAccountInfo>> listener) {
        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(List.of());
            return;
        }
        if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
            return;
        }
        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final Supplier<ThreadContext.StoredContext> contextSupplier = client.threadPool()
                .getThreadContext()
                .newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                final BoolQueryBuilder query = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("doc_type", USER_SERVICE_ACCOUNT_DOC_TYPE));
                final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                    .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                    .setQuery(query)
                    .setSize(1000)
                    .setFetchSource(true)
                    .request();
                request.indicesOptions().ignoreUnavailable();

                ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(contextSupplier, listener), hit -> {
                    final String principal = (String) hit.getSourceAsMap().get("username");
                    if (principal == null) {
                        return null;
                    }
                    final RoleDescriptor parsed = parseRoleDescriptorFromSource(principal, hit.getSourceRef());
                    return parsed == null ? null : new UserServiceAccountInfo(principal, parsed);
                });
            }
        });
    }

    private static String docIdForPrincipal(String principal) {
        return USER_SERVICE_ACCOUNT_DOC_TYPE + "-" + principal;
    }

    private XContentBuilder newDocument(Authentication authentication, ServiceAccountId accountId, RoleDescriptor roleDescriptor)
        throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
            .field("doc_type", USER_SERVICE_ACCOUNT_DOC_TYPE)
            .field("username", accountId.asPrincipal())
            .field("creation_time", clock.instant().toEpochMilli())
            .field("enabled", true);
        {
            final Subject effectiveSubject = authentication.getEffectiveSubject();
            builder.startObject("creator")
                .field("principal", effectiveSubject.getUser().principal())
                .field("full_name", effectiveSubject.getUser().fullName())
                .field("email", effectiveSubject.getUser().email())
                .field("metadata", effectiveSubject.getUser().metadata())
                .field("realm", effectiveSubject.getRealm().getName())
                .field("realm_type", effectiveSubject.getRealm().getType());
            if (effectiveSubject.getRealm().getDomain() != null) {
                builder.field("realm_domain", effectiveSubject.getRealm().getDomain());
            }
            builder.endObject();
        }
        // Store the role descriptor under the existing `role_descriptors` mapping (object, enabled:false)
        // so that it is persisted as an opaque JSON blob keyed by the account principal.
        builder.startObject("role_descriptors");
        builder.field(accountId.asPrincipal());
        roleDescriptor.toXContent(builder, org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS, false);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static RoleDescriptor parseRoleDescriptorFromSource(String principal, BytesReference source) {
        if (source == null) {
            return null;
        }
        try {
            final Map<String, Object> sourceMap = XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
            final Map<String, Object> roleDescriptors = (Map<String, Object>) sourceMap.get("role_descriptors");
            if (roleDescriptors == null) {
                return null;
            }
            final Object descriptor = roleDescriptors.get(principal);
            if (descriptor == null) {
                return null;
            }
            try (
                XContentBuilder b = XContentFactory.jsonBuilder().map(Collections.singletonMap(principal, descriptor));
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG, BytesReference.bytes(b).streamInput())
            ) {
                parser.nextToken(); // START_OBJECT
                parser.nextToken(); // FIELD_NAME
                parser.nextToken(); // START_OBJECT (descriptor)
                return RoleDescriptor.parserBuilder().build().parse(principal, parser, false);
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to parse user service account role descriptor for [" + principal + "]", e);
        }
    }

    private void clearCacheForPrincipal(String principal, ActionListener<Void> listener) {
        if (cache != null) {
            cache.invalidate(principal);
        }
        // Clear cluster-wide so other nodes drop their cached entry.
        final ClearSecurityCacheRequest clearRequest = new ClearSecurityCacheRequest().cacheName(CACHE_NAME).keys(principal);
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearSecurityCacheAction.INSTANCE,
            clearRequest,
            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
        );
    }

    @Override
    public void invalidate(Collection<String> keys) {
        if (cache != null) {
            keys.forEach(cache::invalidate);
        }
    }

    @Override
    public void invalidateAll() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    /**
     * Holds the principal and resolved {@link RoleDescriptor} for a user-defined service account
     * returned by {@link #findAllAccounts(ActionListener)}.
     */
    public record UserServiceAccountInfo(String principal, RoleDescriptor roleDescriptor) {}
}
