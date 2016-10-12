/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.IndicesPermission.Group;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.client.SecurityClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.SecurityTemplateService.securityIndexMappingAndTemplateUpToDate;

/**
 * NativeRolesStore is a {@code RolesStore} that, instead of reading from a
 * file, reads from an Elasticsearch index instead. Unlike the file-based roles
 * store, ESNativeRolesStore can be used to add a role to the store by inserting
 * the document into the administrative index.
 *
 * No caching is done by this class, it is handled at a higher level
 */
public class NativeRolesStore extends AbstractComponent implements RolesStore, ClusterStateListener {

    public static final Setting<Integer> SCROLL_SIZE_SETTING =
            Setting.intSetting(setting("authz.store.roles.index.scroll.size"), 1000, Property.NodeScope);

    public static final Setting<TimeValue> SCROLL_KEEP_ALIVE_SETTING =
            Setting.timeSetting(setting("authz.store.roles.index.scroll.keep_alive"), TimeValue.timeValueSeconds(10L), Property.NodeScope);

    private static final Setting<Integer> CACHE_SIZE_SETTING =
            Setting.intSetting(setting("authz.store.roles.index.cache.max_size"), 10000, Property.NodeScope);
    private static final Setting<TimeValue> CACHE_TTL_SETTING =
            Setting.timeSetting(setting("authz.store.roles.index.cache.ttl"), TimeValue.timeValueMinutes(20), Property.NodeScope);

    public enum State {
        INITIALIZED,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }

    public static final String ROLE_DOC_TYPE = "role";

    private final InternalClient client;
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final boolean isTribeNode;
    private final Cache<String, RoleAndVersion> roleCache;
    // the lock is used in an odd manner; when iterating over the cache we cannot have modifiers other than deletes using
    // the iterator but when not iterating we can modify the cache without external locking. When making normal modifications to the cache
    // the read lock is obtained so that we can allow concurrent modifications; however when we need to iterate over the keys or values of
    // the cache the write lock must obtained to prevent any modifications
    private final ReleasableLock readLock;
    private final ReleasableLock writeLock;

    {
        final ReadWriteLock iterationLock = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(iterationLock.readLock());
        writeLock = new ReleasableLock(iterationLock.writeLock());
    }

    private SecurityClient securityClient;
    private int scrollSize;
    private TimeValue scrollKeepAlive;

    private volatile boolean securityIndexExists = false;

    public NativeRolesStore(Settings settings, InternalClient client) {
        super(settings);
        this.client = client;
        this.roleCache = CacheBuilder.<String, RoleAndVersion>builder()
                .setMaximumWeight(CACHE_SIZE_SETTING.get(settings))
                .setExpireAfterWrite(CACHE_TTL_SETTING.get(settings).getMillis())
                .build();
        this.isTribeNode = settings.getGroups("tribe", true).isEmpty() == false;
    }

    public boolean canStart(ClusterState clusterState, boolean master) {
        if (state() != NativeRolesStore.State.INITIALIZED) {
            return false;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we
            // think may not have the security index but it may not have
            // been restored from the cluster state on disk yet
            logger.debug("native roles store waiting until gateway has recovered from disk");
            return false;
        }

        if (isTribeNode) {
            return true;
        }

        if (securityIndexMappingAndTemplateUpToDate(clusterState, logger) == false) {
            return false;
        }

        IndexMetaData metaData = clusterState.metaData().index(SecurityTemplateService.SECURITY_INDEX_NAME);
        if (metaData == null) {
            logger.debug("security index [{}] does not exist, so service can start", SecurityTemplateService.SECURITY_INDEX_NAME);
            return true;
        }

        if (clusterState.routingTable().index(SecurityTemplateService.SECURITY_INDEX_NAME).allPrimaryShardsActive()) {
            logger.debug("security index [{}] all primary shards started, so service can start",
                    SecurityTemplateService.SECURITY_INDEX_NAME);
            securityIndexExists = true;
            return true;
        }
        return false;
    }

    public void start() {
        try {
            if (state.compareAndSet(State.INITIALIZED, State.STARTING)) {
                this.securityClient = new SecurityClient(client);
                this.scrollSize = SCROLL_SIZE_SETTING.get(settings);
                this.scrollKeepAlive = SCROLL_KEEP_ALIVE_SETTING.get(settings);
                state.set(State.STARTED);
            }
        } catch (Exception e) {
            logger.error("failed to start ESNativeRolesStore", e);
            state.set(State.FAILED);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            state.set(State.STOPPED);
        }
    }

    /**
     * Retrieve a list of roles, if rolesToGet is null or empty, fetch all roles
     */
    public void getRoleDescriptors(String[] names, final ActionListener<List<RoleDescriptor>> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to get roles before service was started");
            listener.onFailure(new IllegalStateException("roles cannot be retrieved as native role service has not been started"));
            return;
        }
        try {
            final List<RoleDescriptor> roles = new ArrayList<>();
            QueryBuilder query;
            if (names == null || names.length == 0) {
                query = QueryBuilders.matchAllQuery();
            } else {
                query = QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery(ROLE_DOC_TYPE).addIds(names));
            }
            SearchRequest request = client.prepareSearch(SecurityTemplateService.SECURITY_INDEX_NAME)
                    .setTypes(ROLE_DOC_TYPE)
                    .setScroll(scrollKeepAlive)
                    .setQuery(query)
                    .setSize(scrollSize)
                    .setFetchSource(true)
                    .request();
            request.indicesOptions().ignoreUnavailable();

            // This function is MADNESS! But it works, don't think about it too hard...
            client.search(request, new ActionListener<SearchResponse>() {

                private SearchResponse lastResponse = null;

                @Override
                public void onResponse(SearchResponse resp) {
                    lastResponse = resp;
                    boolean hasHits = resp.getHits().getHits().length > 0;
                    if (hasHits) {
                        for (SearchHit hit : resp.getHits().getHits()) {
                            RoleDescriptor rd = transformRole(hit.getId(), hit.getSourceRef(), logger);
                            if (rd != null) {
                                roles.add(rd);
                            }
                        }
                        SearchScrollRequest scrollRequest = client.prepareSearchScroll(resp.getScrollId())
                                .setScroll(scrollKeepAlive).request();
                        client.searchScroll(scrollRequest, this);
                    } else {
                        if (resp.getScrollId() != null) {
                            clearScollRequest(resp.getScrollId());
                        }
                        // Finally, return the list of users
                        listener.onResponse(Collections.unmodifiableList(roles));
                    }
                }

                @Override
                public void onFailure(Exception t) {
                    // attempt to clear the scroll request
                    if (lastResponse != null && lastResponse.getScrollId() != null) {
                        clearScollRequest(lastResponse.getScrollId());
                    }

                    if (t instanceof IndexNotFoundException) {
                        logger.trace("could not retrieve roles because security index does not exist");
                        // since this is expected to happen at times, we just call the listener with an empty list
                        listener.onResponse(Collections.<RoleDescriptor>emptyList());
                    } else {
                        listener.onFailure(t);
                    }
                }
            });
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to retrieve roles {}", Arrays.toString(names)), e);
            listener.onFailure(e);
        }
    }

    public void getRoleDescriptor(final String role, final ActionListener<RoleDescriptor> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to get role [{}] before service was started", role);
            listener.onResponse(null);
        }
        RoleAndVersion roleAndVersion = getRoleAndVersion(role);
        listener.onResponse(roleAndVersion == null ? null : roleAndVersion.getRoleDescriptor());
    }

    public void deleteRole(final DeleteRoleRequest deleteRoleRequest, final ActionListener<Boolean> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to delete role [{}] before service was started", deleteRoleRequest.name());
            listener.onResponse(false);
        } else if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("roles may not be deleted using a tribe node"));
            return;
        }

        try {
            DeleteRequest request = client.prepareDelete(SecurityTemplateService.SECURITY_INDEX_NAME,
                    ROLE_DOC_TYPE, deleteRoleRequest.name()).request();
            request.setRefreshPolicy(deleteRoleRequest.getRefreshPolicy());
            client.delete(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    clearRoleCache(deleteRoleRequest.name(), listener, deleteResponse.getResult() == DocWriteResponse.Result.DELETED);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to delete role from the index", e);
                    listener.onFailure(e);
                }
            });
        } catch (IndexNotFoundException e) {
            logger.trace("security index does not exist", e);
            listener.onResponse(false);
        } catch (Exception e) {
            logger.error("unable to remove role", e);
            listener.onFailure(e);
        }
    }

    public void putRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to put role [{}] before service was started", request.name());
            listener.onResponse(false);
        } else if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("roles may not be created or modified using a tribe node"));
            return;
        }

        try {
            client.prepareIndex(SecurityTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role.getName())
                    .setSource(role.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .setRefreshPolicy(request.getRefreshPolicy())
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                            if (created) {
                                listener.onResponse(true);
                                return;
                            }
                            clearRoleCache(role.getName(), listener, created);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to put role [{}]", request.name()), e);
                            listener.onFailure(e);
                        }
                    });
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to put role [{}]", request.name()), e);
            listener.onFailure(e);
        }

    }

    @Override
    public Role role(String roleName) {
        if (state() != State.STARTED) {
            return null;
        }
        RoleAndVersion roleAndVersion = getRoleAndVersion(roleName);
        return roleAndVersion == null ? null : roleAndVersion.getRole();
    }

    @Override
    public Map<String, Object> usageStats() {
        if (state() != State.STARTED) {
            return Collections.emptyMap();
        }

        boolean dls = false;
        boolean fls = false;
        Map<String, Object> usageStats = new HashMap<>();
        if (securityIndexExists == false) {
            usageStats.put("size", 0L);
            usageStats.put("fls", fls);
            usageStats.put("dls", dls);
            return usageStats;
        }

        long count = roleCache.count();
        try (final ReleasableLock ignored = writeLock.acquire()) {
            for (RoleAndVersion rv : roleCache.values()) {
                Role role = rv.getRole();
                for (Group group : role.indices()) {
                    fls = fls || group.getFieldPermissions().hasFieldLevelSecurity();
                    dls = dls || group.hasQuery();
                }
                if (fls && dls) {
                    break;
                }
            }
        }

        // slow path - query for necessary information
        if (fls == false || dls == false) {
            MultiSearchRequestBuilder builder = client.prepareMultiSearch()
                    .add(client.prepareSearch(SecurityTemplateService.SECURITY_INDEX_NAME)
                            .setTypes(ROLE_DOC_TYPE)
                            .setQuery(QueryBuilders.matchAllQuery())
                            .setSize(0));

            if (fls == false) {
                builder.add(client.prepareSearch(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .setTypes(ROLE_DOC_TYPE)
                        .setQuery(QueryBuilders.boolQuery()
                                .should(existsQuery("indices.field_security.grant"))
                                .should(existsQuery("indices.field_security.except"))
                                // for backwardscompat with 2.x
                                .should(existsQuery("indices.fields")))
                        .setSize(0)
                        .setTerminateAfter(1));
            }

            if (dls == false) {
                builder.add(client.prepareSearch(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .setTypes(ROLE_DOC_TYPE)
                        .setQuery(existsQuery("indices.query"))
                        .setSize(0)
                        .setTerminateAfter(1));
            }

            MultiSearchResponse multiSearchResponse = builder.get();
            int pos = 0;
            Item[] responses = multiSearchResponse.getResponses();
            if (responses[pos].isFailure() == false) {
                count = responses[pos].getResponse().getHits().getTotalHits();
            }

            if (fls == false) {
                if (responses[++pos].isFailure() == false) {
                    fls = responses[pos].getResponse().getHits().getTotalHits() > 0L;
                }
            }

            if (dls == false) {
                if (responses[++pos].isFailure() == false) {
                    dls = responses[pos].getResponse().getHits().getTotalHits() > 0L;
                }
            }
        }

        usageStats.put("size", count);
        usageStats.put("fls", fls);
        usageStats.put("dls", dls);
        return usageStats;
    }

    private RoleAndVersion getRoleAndVersion(final String roleId) {
        if (securityIndexExists == false) {
            return null;
        }

        RoleAndVersion roleAndVersion = null;
        final AtomicReference<GetResponse> getRef = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            roleAndVersion = roleCache.computeIfAbsent(roleId, (key) -> {
                logger.debug("attempting to load role [{}] from index", key);
                executeGetRoleRequest(roleId, new LatchedActionListener<>(new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse role) {
                        getRef.set(role);
                    }

                    @Override
                    public void onFailure(Exception t) {
                        if (t instanceof IndexNotFoundException) {
                            logger.trace(
                                    (Supplier<?>) () -> new ParameterizedMessage(
                                            "failed to retrieve role [{}] since security index does not exist", roleId), t);
                        } else {
                            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to retrieve role [{}]", roleId), t);
                        }
                    }
                }, latch));

                try {
                    latch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("timed out retrieving role [{}]", roleId);
                }

                GetResponse response = getRef.get();
                if (response == null) {
                    return null;
                }

                RoleDescriptor descriptor = transformRole(response);
                if (descriptor == null) {
                    return null;
                }
                logger.debug("loaded role [{}] from index with version [{}]", key, response.getVersion());
                try (final ReleasableLock ignored = readLock.acquire()) {
                    return new RoleAndVersion(descriptor, response.getVersion());
                }
            });
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NullPointerException) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("role [{}] was not found", roleId), e);
            } else {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to load role [{}]", roleId), e);
            }
        }

        return roleAndVersion;
    }

    private void executeGetRoleRequest(String role, ActionListener<GetResponse> listener) {
        try {
            GetRequest request = client.prepareGet(SecurityTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role).request();
            client.get(request, listener);
        } catch (IndexNotFoundException e) {
            logger.trace(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "unable to retrieve role [{}] since security index does not exist", role), e);
            listener.onResponse(new GetResponse(
                    new GetResult(SecurityTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role, -1, false, null, null)));
        } catch (Exception e) {
            logger.error("unable to retrieve role", e);
            listener.onFailure(e);
        }
    }

    private void clearScollRequest(final String scrollId) {
        ClearScrollRequest clearScrollRequest = client.prepareClearScroll().addScrollId(scrollId).request();
        client.clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse response) {
                // cool, it cleared, we don't really care though...
            }

            @Override
            public void onFailure(Exception t) {
                // Not really much to do here except for warn about it...
                logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage("failed to clear scroll [{}] after retrieving roles", scrollId), t);
            }
        });
    }

    // FIXME hack for testing
    public void reset() {
        final State state = state();
        if (state != State.STOPPED && state != State.FAILED) {
            throw new IllegalStateException("can only reset if stopped!!!");
        }
        invalidateAll();
        this.securityIndexExists = false;
        this.state.set(State.INITIALIZED);
    }

    public void invalidateAll() {
        logger.debug("invalidating all roles in cache");
        try (final ReleasableLock ignored = readLock.acquire()) {
            roleCache.invalidateAll();
        }
    }

    public void invalidate(String role) {
        logger.debug("invalidating role [{}] in cache", role);
        try (final ReleasableLock ignored = readLock.acquire()) {
            roleCache.invalidate(role);
        }
    }

    private <Response> void clearRoleCache(final String role, ActionListener<Response> listener, Response response) {
        ClearRolesCacheRequest request = new ClearRolesCacheRequest().names(role);
        securityClient.clearRolesCache(request, new ActionListener<ClearRolesCacheResponse>() {
            @Override
            public void onResponse(ClearRolesCacheResponse nodes) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to clear cache for role [{}]", role), e);
                ElasticsearchException exception = new ElasticsearchException("clearing the cache for [" + role
                        + "] failed. please clear the role cache manually", e);
                listener.onFailure(exception);
            }
        });
    }

    // TODO abstract this code rather than duplicating...
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final boolean exists = event.state().metaData().indices().get(SecurityTemplateService.SECURITY_INDEX_NAME) != null;
        // make sure all the primaries are active
        if (exists && event.state().routingTable().index(SecurityTemplateService.SECURITY_INDEX_NAME).allPrimaryShardsActive()) {
            logger.debug(
                    "security index [{}] all primary shards started, so polling can start", SecurityTemplateService.SECURITY_INDEX_NAME);
            securityIndexExists = true;
        } else {
            // always set the value - it may have changed...
            securityIndexExists = false;
        }
    }

    public State state() {
        return state.get();
    }

    @Nullable
    private RoleDescriptor transformRole(GetResponse response) {
        if (response.isExists() == false) {
            return null;
        }
        return transformRole(response.getId(), response.getSourceAsBytesRef(), logger);
    }

    @Nullable
    static RoleDescriptor transformRole(String name, BytesReference sourceBytes, Logger logger) {
        try {
            // we pass true as last parameter because we do not want to reject permissions if the field permissions
            // are given in 2.x syntax
            return RoleDescriptor.parse(name, sourceBytes, true);
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("error in the format of data for role [{}]", name), e);
            return null;
        }
    }

    private static class RoleAndVersion {

        private final RoleDescriptor roleDescriptor;
        private final Role role;
        private final long version;

        RoleAndVersion(RoleDescriptor roleDescriptor, long version) {
            this.roleDescriptor = roleDescriptor;
            this.role = Role.builder(roleDescriptor).build();
            this.version = version;
        }

        RoleDescriptor getRoleDescriptor() {
            return roleDescriptor;
        }

        Role getRole() {
            return role;
        }

        long getVersion() {
            return version;
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(SCROLL_SIZE_SETTING);
        settings.add(SCROLL_KEEP_ALIVE_SETTING);
        settings.add(CACHE_SIZE_SETTING);
        settings.add(CACHE_TTL_SETTING);
    }
}
