/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.DELETED;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * This store reads + writes {@link ExpressionRoleMapping role mappings} in an Elasticsearch
 * {@link SecuritySystemIndices#SECURITY_MAIN_ALIAS index}.
 * <br>
 * The store is responsible for all read and write operations as well as
 * {@link #resolveRoles(UserData, ActionListener) resolving roles}.
 * <p>
 * No caching is done by this class, it is handled at a higher level and no polling for changes
 * is done by this class. Modification operations make a best effort attempt to clear the cache
 * on all nodes for the user that was modified.
 */
public class NativeRoleMappingStore implements UserRoleMapper {

    private static final Logger logger = LogManager.getLogger(NativeRoleMappingStore.class);
    static final String DOC_TYPE_FIELD = "doc_type";
    static final String DOC_TYPE_ROLE_MAPPING = "role-mapping";

    private static final String ID_PREFIX = DOC_TYPE_ROLE_MAPPING + "_";

    public static final Setting<Boolean> LAST_LOAD_CACHE_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authz.store.role_mappings.last_load_cache.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    private final Settings settings;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ScriptService scriptService;
    private final List<String> realmsToRefresh = new CopyOnWriteArrayList<>();
    private final boolean lastLoadCacheEnabled;
    private final AtomicReference<List<ExpressionRoleMapping>> lastLoadRef = new AtomicReference<>(null);

    public NativeRoleMappingStore(Settings settings, Client client, SecurityIndexManager securityIndex, ScriptService scriptService) {
        this.settings = settings;
        this.client = client;
        this.securityIndex = securityIndex;
        this.scriptService = scriptService;
        this.lastLoadCacheEnabled = LAST_LOAD_CACHE_ENABLED_SETTING.get(settings);
    }

    private static String getNameFromId(String id) {
        assert id.startsWith(ID_PREFIX);
        return id.substring(ID_PREFIX.length());
    }

    // package-private for testing
    static String getIdForName(String name) {
        return ID_PREFIX + name;
    }

    /**
     * Loads all mappings from the index.
     * <em>package private</em> for unit testing
     */
    protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
        if (securityIndex.isIndexUpToDate() == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Security index is not on the current version - the native realm will not be operational until "
                        + "the upgrade API is run on the security index"
                )
            );
            return;
        }
        final QueryBuilder query = QueryBuilders.termQuery(DOC_TYPE_FIELD, DOC_TYPE_ROLE_MAPPING);
        final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                .setQuery(query)
                .setSize(1000)
                .setFetchSource(true)
                .request();
            request.indicesOptions().ignoreUnavailable();
            ScrollHelper.fetchAllByEntity(
                client,
                request,
                new ContextPreservingActionListener<>(supplier, ActionListener.wrap((Collection<ExpressionRoleMapping> mappings) -> {
                    final List<ExpressionRoleMapping> mappingList = mappings.stream().filter(Objects::nonNull).toList();
                    logger.debug("successfully loaded [{}] role-mapping(s) from [{}]", mappingList.size(), securityIndex.aliasName());
                    if (lastLoadCacheEnabled) {
                        logger.debug("caching loaded role-mapping(s)");
                        lastLoadRef.set(List.copyOf(mappingList));
                    }
                    listener.onResponse(mappingList);
                }, ex -> {
                    logger.error(
                        () -> format("failed to load role mappings from index [%s] skipping all mappings.", SECURITY_MAIN_ALIAS),
                        ex
                    );
                    listener.onResponse(Collections.emptyList());
                })),
                doc -> buildMapping(getNameFromId(doc.getId()), doc.getSourceRef())
            );
        }
    }

    protected static ExpressionRoleMapping buildMapping(String id, BytesReference source) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                source,
                XContentType.JSON
            )
        ) {
            return ExpressionRoleMapping.parse(id, parser);
        } catch (Exception e) {
            logger.warn(() -> "Role mapping [" + id + "] cannot be parsed and will be skipped", e);
            return null;
        }
    }

    /**
     * Stores (create or update) a single mapping in the index
     */
    public void putRoleMapping(PutRoleMappingRequest request, ActionListener<Boolean> listener) {
        // Validate all templates before storing the role mapping
        for (TemplateRoleName templateRoleName : request.getRoleTemplates()) {
            templateRoleName.validate(scriptService);
        }
        modifyMapping(request.getName(), this::innerPutMapping, request, listener);
    }

    /**
     * Deletes a named mapping from the index
     */
    public void deleteRoleMapping(DeleteRoleMappingRequest request, ActionListener<Boolean> listener) {
        modifyMapping(request.getName(), this::innerDeleteMapping, request, listener);
    }

    private <Request, Result> void modifyMapping(
        String name,
        CheckedBiConsumer<Request, ActionListener<Result>, Exception> inner,
        Request request,
        ActionListener<Result> listener
    ) {
        if (securityIndex.isIndexUpToDate() == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Security index is not on the current version - the native realm will not be operational until "
                        + "the upgrade API is run on the security index"
                )
            );
        } else {
            try {
                logger.trace("Modifying role mapping [{}] for [{}]", name, request.getClass().getSimpleName());
                inner.accept(request, ActionListener.wrap(r -> refreshRealms(listener, r), listener::onFailure));
            } catch (Exception e) {
                logger.error(() -> "failed to modify role-mapping [" + name + "]", e);
                listener.onFailure(e);
            }
        }
    }

    private void innerPutMapping(PutRoleMappingRequest request, ActionListener<Boolean> listener) {
        final ExpressionRoleMapping mapping = request.getMapping();
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final XContentBuilder xContentBuilder;
            try {
                xContentBuilder = mapping.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS, true);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareIndex(SECURITY_MAIN_ALIAS)
                    .setId(getIdForName(mapping.getName()))
                    .setSource(xContentBuilder)
                    .setRefreshPolicy(request.getRefreshPolicy())
                    .setWaitForActiveShards(ActiveShardCount.NONE)
                    .request(),
                new ActionListener<DocWriteResponse>() {
                    @Override
                    public void onResponse(DocWriteResponse indexResponse) {
                        boolean created = indexResponse.getResult() == CREATED;
                        listener.onResponse(created);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(() -> "failed to put role-mapping [" + mapping.getName() + "]", e);
                        listener.onFailure(e);
                    }
                },
                client::index
            );
        });
    }

    private void innerDeleteMapping(DeleteRoleMappingRequest request, ActionListener<Boolean> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(false);
        } else if (securityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    client.prepareDelete(SECURITY_MAIN_ALIAS, getIdForName(request.getName()))
                        .setRefreshPolicy(request.getRefreshPolicy())
                        .setWaitForActiveShards(ActiveShardCount.NONE)
                        .request(),
                    new ActionListener<DeleteResponse>() {

                        @Override
                        public void onResponse(DeleteResponse deleteResponse) {
                            boolean deleted = deleteResponse.getResult() == DELETED;
                            listener.onResponse(deleted);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(() -> "failed to delete role-mapping [" + request.getName() + "]", e);
                            listener.onFailure(e);

                        }
                    },
                    client::delete
                );
            });
        }
    }

    /**
     * Retrieves one or more mappings from the index.
     * If <code>names</code> is <code>null</code> or {@link Set#isEmpty empty}, then this retrieves all mappings.
     * Otherwise it retrieves the specified mappings by name.
     */
    public void getRoleMappings(Set<String> names, ActionListener<List<ExpressionRoleMapping>> listener) {
        if (names == null || names.isEmpty()) {
            getMappings(listener);
        } else {
            getMappings(listener.safeMap(mappings -> mappings.stream().filter(m -> names.contains(m.getName())).toList()));
        }
    }

    private void getMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.debug("The security index does not exist - no role mappings can be loaded");
            listener.onResponse(Collections.emptyList());
            return;
        }
        final List<ExpressionRoleMapping> lastLoad = lastLoadRef.get();
        if (frozenSecurityIndex.indexIsClosed()) {
            if (lastLoad != null) {
                assert lastLoadCacheEnabled;
                logger.debug("The security index exists but is closed - returning previously cached role mappings");
                listener.onResponse(lastLoad);
            } else {
                logger.debug("The security index exists but is closed - no role mappings can be loaded");
                listener.onResponse(Collections.emptyList());
            }
        } else if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            final ElasticsearchException unavailableReason = frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS);
            if (lastLoad != null) {
                assert lastLoadCacheEnabled;
                logger.debug(
                    "The security index exists but is not available - returning previously cached role mappings",
                    unavailableReason
                );
                listener.onResponse(lastLoad);
            } else {
                logger.debug("The security index exists but is not available - no role mappings can be loaded");
                listener.onFailure(unavailableReason);
            }
        } else {
            loadMappings(listener);
        }
    }

    // package-private for testing
    @Nullable
    List<ExpressionRoleMapping> getLastLoad() {
        return lastLoadRef.get();
    }

    /**
     * Provides usage statistics for this store.
     * The resulting map contains the keys
     * <ul>
     * <li><code>size</code> - The total number of mappings stored in the index</li>
     * <li><code>enabled</code> - The number of mappings that are
     * {@link ExpressionRoleMapping#isEnabled() enabled}</li>
     * </ul>
     */
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        if (securityIndex.indexIsClosed() || securityIndex.isAvailable(SEARCH_SHARDS) == false) {
            reportStats(listener, Collections.emptyList());
        } else {
            getMappings(ActionListener.wrap(mappings -> reportStats(listener, mappings), listener::onFailure));
        }
    }

    private static void reportStats(ActionListener<Map<String, Object>> listener, List<ExpressionRoleMapping> mappings) {
        Map<String, Object> usageStats = new HashMap<>();
        usageStats.put("size", mappings.size());
        usageStats.put("enabled", mappings.stream().filter(ExpressionRoleMapping::isEnabled).count());
        listener.onResponse(usageStats);
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState)
            || isIndexDeleted(previousState, currentState)
            || Objects.equals(previousState.indexUUID, currentState.indexUUID) == false
            || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            refreshRealms(ActionListener.noop(), null);
        }
    }

    private <Result> void refreshRealms(ActionListener<Result> listener, Result result) {
        if (realmsToRefresh.isEmpty()) {
            listener.onResponse(result);
            return;
        }

        final String[] realmNames = this.realmsToRefresh.toArray(Strings.EMPTY_ARRAY);
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearRealmCacheAction.INSTANCE,
            new ClearRealmCacheRequest().realms(realmNames),
            ActionListener.wrap(response -> {
                logger.debug(() -> format("Cleared cached in realms [%s] due to role mapping change", Arrays.toString(realmNames)));
                listener.onResponse(result);
            }, ex -> {
                logger.warn(() -> "Failed to clear cache for realms [" + Arrays.toString(realmNames) + "]", ex);
                listener.onFailure(ex);
            })
        );
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        getRoleMappings(null, ActionListener.wrap(mappings -> {
            final ExpressionModel model = user.asModel();
            final Set<String> roles = mappings.stream()
                .filter(ExpressionRoleMapping::isEnabled)
                .filter(m -> m.getExpression().match(model))
                .flatMap(m -> {
                    final Set<String> roleNames = m.getRoleNames(scriptService, model);
                    logger.trace("Applying role-mapping [{}] to user-model [{}] produced role-names [{}]", m.getName(), model, roleNames);
                    return roleNames.stream();
                })
                .collect(Collectors.toSet());
            logger.debug("Mapping user [{}] to roles [{}]", user, roles);
            listener.onResponse(roles);
        }, listener::onFailure));
    }

    /**
     * Indicates that the provided realm should have its cache cleared if this store is updated
     * (that is, {@link #putRoleMapping(PutRoleMappingRequest, ActionListener)} or
     * {@link #deleteRoleMapping(DeleteRoleMappingRequest, ActionListener)} are called).
     * @see ClearRealmCacheAction
     */
    @Override
    public void refreshRealmOnChange(CachingRealm realm) {
        realmsToRefresh.add(realm.name());
    }
}
