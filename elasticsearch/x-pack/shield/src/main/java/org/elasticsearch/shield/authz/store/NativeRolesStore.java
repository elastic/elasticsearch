/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.shield.ShieldTemplateService;
import org.elasticsearch.shield.action.role.ClearRolesCacheRequest;
import org.elasticsearch.shield.action.role.ClearRolesCacheResponse;
import org.elasticsearch.shield.action.role.DeleteRoleRequest;
import org.elasticsearch.shield.action.role.PutRoleRequest;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.client.SecurityClient;
import org.elasticsearch.shield.support.SelfReschedulingRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.shield.Security.setting;

/**
 * ESNativeRolesStore is a {@code RolesStore} that, instead of reading from a
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

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING =
            Setting.timeSetting(setting("authz.store.roles.index.reload.interval"), TimeValue.timeValueSeconds(30L), Property.NodeScope);

    public enum State {
        INITIALIZED,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }

    public static final String ROLE_DOC_TYPE = "role";

    private final Provider<InternalClient> clientProvider;
    private final ThreadPool threadPool;
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final ConcurrentHashMap<String, RoleAndVersion> roleCache = new ConcurrentHashMap<>();

    private Client client;
    private SecurityClient securityClient;
    private int scrollSize;
    private TimeValue scrollKeepAlive;
    private SelfReschedulingRunnable rolesPoller;

    private volatile boolean shieldIndexExists = false;

    @Inject
    public NativeRolesStore(Settings settings, Provider<InternalClient> clientProvider, ThreadPool threadPool) {
        super(settings);
        this.clientProvider = clientProvider;
        this.threadPool = threadPool;
    }

    public boolean canStart(ClusterState clusterState, boolean master) {
        if (state() != NativeRolesStore.State.INITIALIZED) {
            return false;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we
            // think may not have the .shield index but they it may not have
            // been restored from the cluster state on disk yet
            logger.debug("native roles store waiting until gateway has recovered from disk");
            return false;
        }

        if (clusterState.metaData().templates().get(ShieldTemplateService.SECURITY_TEMPLATE_NAME) == null) {
            logger.debug("native roles template [{}] does not exist, so service cannot start",
                    ShieldTemplateService.SECURITY_TEMPLATE_NAME);
            return false;
        }
        // Okay to start...
        return true;
    }

    public void start() {
        try {
            if (state.compareAndSet(State.INITIALIZED, State.STARTING)) {
                this.client = clientProvider.get();
                this.securityClient = new SecurityClient(client);
                this.scrollSize = SCROLL_SIZE_SETTING.get(settings);
                this.scrollKeepAlive = SCROLL_KEEP_ALIVE_SETTING.get(settings);
                TimeValue pollInterval = POLL_INTERVAL_SETTING.get(settings);
                RolesStorePoller poller = new RolesStorePoller();
                try {
                    poller.doRun();
                } catch (Exception e) {
                    logger.warn("failed to perform initial poll of roles index [{}]. scheduling again in [{}]", e,
                            ShieldTemplateService.SECURITY_INDEX_NAME, pollInterval);
                }
                rolesPoller = new SelfReschedulingRunnable(poller, threadPool, pollInterval, Names.GENERIC, logger);
                rolesPoller.start();
                state.set(State.STARTED);
            }
        } catch (Exception e) {
            logger.error("failed to start ESNativeRolesStore", e);
            state.set(State.FAILED);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                rolesPoller.stop();
            } finally {
                state.set(State.STOPPED);
            }
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
            SearchRequest request = client.prepareSearch(ShieldTemplateService.SECURITY_INDEX_NAME)
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
                            RoleDescriptor rd = transformRole(hit.getId(), hit.getSourceRef());
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
                public void onFailure(Throwable t) {
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
            logger.error("unable to retrieve roles {}", e, Arrays.toString(names));
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
        }
        try {
            DeleteRequest request = client.prepareDelete(ShieldTemplateService.SECURITY_INDEX_NAME,
                    ROLE_DOC_TYPE, deleteRoleRequest.name()).request();
            request.refresh(deleteRoleRequest.refresh());
            client.delete(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    clearRoleCache(deleteRoleRequest.name(), listener, deleteResponse.isFound());
                }

                @Override
                public void onFailure(Throwable e) {
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
        }
        try {
            client.prepareIndex(ShieldTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role.getName())
                    .setSource(role.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                    .setRefresh(request.refresh())
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            if (indexResponse.isCreated()) {
                                listener.onResponse(indexResponse.isCreated());
                                return;
                            }
                            clearRoleCache(role.getName(), listener, indexResponse.isCreated());
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("failed to put role [{}]", e, request.name());
                            listener.onFailure(e);
                        }
                    });
        } catch (Exception e) {
            logger.error("unable to put role [{}]", e, request.name());
            listener.onFailure(e);
        }

    }

    @Override
    public Role role(String roleName) {
        RoleAndVersion roleAndVersion = getRoleAndVersion(roleName);
        return roleAndVersion == null ? null : roleAndVersion.getRole();
    }

    private RoleAndVersion getRoleAndVersion(final String roleId) {
        RoleAndVersion roleAndVersion = null;
        final AtomicReference<GetResponse> getRef = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            roleAndVersion = roleCache.computeIfAbsent(roleId, new Function<String, RoleAndVersion>() {
                @Override
                public RoleAndVersion apply(String key) {
                    logger.debug("attempting to load role [{}] from index", key);
                    executeGetRoleRequest(roleId, new LatchedActionListener<>(new ActionListener<GetResponse>() {
                        @Override
                        public void onResponse(GetResponse role) {
                            getRef.set(role);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            if (t instanceof IndexNotFoundException) {
                                logger.trace("failed to retrieve role [{}] since security index does not exist", t, roleId);
                            } else {
                                logger.error("failed to retrieve role [{}]", t, roleId);
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
                    return new RoleAndVersion(descriptor, response.getVersion());
                }
            });
        } catch (RuntimeException e) {
            logger.error("could not get or load value from cache for role [{}]", e, roleId);
        }

        return roleAndVersion;
    }

    private void executeGetRoleRequest(String role, ActionListener<GetResponse> listener) {
        try {
            GetRequest request = client.prepareGet(ShieldTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role).request();
            client.get(request, listener);
        } catch (IndexNotFoundException e) {
            logger.trace("unable to retrieve role [{}] since security index does not exist", e, role);
            listener.onResponse(new GetResponse(
                    new GetResult(ShieldTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role, -1, false, null, null)));
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
            public void onFailure(Throwable t) {
                // Not really much to do here except for warn about it...
                logger.warn("failed to clear scroll [{}] after retrieving roles", t, scrollId);
            }
        });
    }

    // FIXME hack for testing
    public void reset() {
        final State state = state();
        if (state != State.STOPPED && state != State.FAILED) {
            throw new IllegalStateException("can only reset if stopped!!!");
        }
        this.roleCache.clear();
        this.client = null;
        this.shieldIndexExists = false;
        this.state.set(State.INITIALIZED);
    }

    public void invalidateAll() {
        logger.debug("invalidating all roles in cache");
        roleCache.clear();
    }

    public void invalidate(String role) {
        logger.debug("invalidating role [{}] in cache", role);
        roleCache.remove(role);
    }

    private <Response> void clearRoleCache(final String role, ActionListener<Response> listener, Response response) {
        ClearRolesCacheRequest request = new ClearRolesCacheRequest().names(role);
        securityClient.clearRolesCache(request, new ActionListener<ClearRolesCacheResponse>() {
            @Override
            public void onResponse(ClearRolesCacheResponse nodes) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("unable to clear cache for role [{}]", e, role);
                ElasticsearchException exception = new ElasticsearchException("clearing the cache for [" + role
                        + "] failed. please clear the role cache manually", e);
                listener.onFailure(exception);
            }
        });
    }

    // TODO abstract this code rather than duplicating...
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final boolean exists = event.state().metaData().indices().get(ShieldTemplateService.SECURITY_INDEX_NAME) != null;
        // make sure all the primaries are active
        if (exists && event.state().routingTable().index(ShieldTemplateService.SECURITY_INDEX_NAME).allPrimaryShardsActive()) {
            logger.debug("security index [{}] all primary shards started, so polling can start",
                    ShieldTemplateService.SECURITY_INDEX_NAME);
            shieldIndexExists = true;
        } else {
            // always set the value - it may have changed...
            shieldIndexExists = false;
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
        return transformRole(response.getId(), response.getSourceAsBytesRef());
    }

    @Nullable
    private RoleDescriptor transformRole(String name, BytesReference sourceBytes) {
        try {
            return RoleDescriptor.parse(name, sourceBytes);
        } catch (Exception e) {
            logger.error("error in the format of data for role [{}]", e, name);
            return null;
        }
    }

    private class RolesStorePoller extends AbstractRunnable {

        @Override
        protected void doRun() throws Exception {
            if (isStopped()) {
                return;
            }
            if (shieldIndexExists == false) {
                logger.trace("cannot poll for role changes since security index [{}] does not exist",
                        ShieldTemplateService.SECURITY_INDEX_NAME);
                return;
            }

            // hold a reference to the client since the poller may run after the class is stopped (we don't interrupt it running) and
            // we reset when we test which sets the client to null...
            final Client client = NativeRolesStore.this.client;

            logger.trace("starting polling of roles index to check for changes");
            SearchResponse response = null;
            // create a copy of the keys in the cache since we will be modifying this list
            final Set<String> existingRoles = new HashSet<>(roleCache.keySet());
            try {
                client.admin().indices().prepareRefresh(ShieldTemplateService.SECURITY_INDEX_NAME);
                SearchRequest request = client.prepareSearch(ShieldTemplateService.SECURITY_INDEX_NAME)
                        .setScroll(scrollKeepAlive)
                        .setQuery(QueryBuilders.typeQuery(ROLE_DOC_TYPE))
                        .setSize(scrollSize)
                        .setFetchSource(true)
                        .setVersion(true)
                        .request();
                response = client.search(request).actionGet();

                boolean keepScrolling = response.getHits().getHits().length > 0;
                while (keepScrolling) {
                    if (isStopped()) {
                        return;
                    }
                    for (SearchHit hit : response.getHits().getHits()) {
                        final String roleName = hit.getId();
                        final long version = hit.version();
                        existingRoles.remove(roleName);
                        // we use the locking mechanisms provided by the map/cache to help protect against concurrent operations
                        // that will leave the cache in a bad state
                        roleCache.computeIfPresent(roleName, new BiFunction<String, RoleAndVersion, RoleAndVersion>() {
                            @Override
                            public RoleAndVersion apply(String roleName, RoleAndVersion existing) {
                                if (version > existing.getVersion()) {
                                    RoleDescriptor rd = transformRole(hit.getId(), hit.getSourceRef());
                                    if (rd != null) {
                                        return new RoleAndVersion(rd, version);
                                    }
                                }
                                return existing;
                            }
                        });
                    }
                    SearchScrollRequest scrollRequest = client.prepareSearchScroll(response.getScrollId())
                            .setScroll(scrollKeepAlive).request();
                    response = client.searchScroll(scrollRequest).actionGet();
                    keepScrolling = response.getHits().getHits().length > 0;
                }

                // check to see if we had roles that do not exist in the index
                if (existingRoles.isEmpty() == false) {
                    for (String roleName : existingRoles) {
                        logger.trace("role [{}] does not exist anymore, removing from cache", roleName);
                        roleCache.remove(roleName);
                    }
                }
            } catch (IndexNotFoundException e) {
                logger.trace("security index does not exist", e);
            } finally {
                if (response != null) {
                    ClearScrollRequest clearScrollRequest = client.prepareClearScroll().addScrollId(response.getScrollId()).request();
                    client.clearScroll(clearScrollRequest).actionGet();
                }
            }
            logger.trace("completed polling of roles index");
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("error occurred while checking the native roles for changes", t);
        }

        private boolean isStopped() {
            State state = state();
            return state == State.STOPPED || state == State.STOPPING;
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

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(SCROLL_SIZE_SETTING);
        settingsModule.registerSetting(SCROLL_KEEP_ALIVE_SETTING);
        settingsModule.registerSetting(POLL_INTERVAL_SETTING);
    }
}
