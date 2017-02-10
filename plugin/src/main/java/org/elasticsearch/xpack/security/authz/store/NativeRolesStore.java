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
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.security.client.SecurityClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.SecurityTemplateService.securityIndexMappingAndTemplateSufficientToRead;
import static org.elasticsearch.xpack.security.SecurityTemplateService.securityIndexMappingAndTemplateUpToDate;

/**
 * NativeRolesStore is a {@code RolesStore} that, instead of reading from a
 * file, reads from an Elasticsearch index instead. Unlike the file-based roles
 * store, ESNativeRolesStore can be used to add a role to the store by inserting
 * the document into the administrative index.
 *
 * No caching is done by this class, it is handled at a higher level
 */
public class NativeRolesStore extends AbstractComponent implements ClusterStateListener {

    public enum State {
        INITIALIZED,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }

    // these are no longer used, but leave them around for users upgrading
    private static final Setting<Integer> CACHE_SIZE_SETTING =
            Setting.intSetting(setting("authz.store.roles.index.cache.max_size"), 10000, Property.NodeScope, Property.Deprecated);
    private static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(setting("authz.store.roles.index.cache.ttl"),
            TimeValue.timeValueMinutes(20), Property.NodeScope, Property.Deprecated);

    private static final String ROLE_DOC_TYPE = "role";

    private final InternalClient client;
    private final XPackLicenseState licenseState;
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final boolean isTribeNode;

    private SecurityClient securityClient;

    private volatile boolean securityIndexExists = false;
    private volatile boolean canWrite = false;

    public NativeRolesStore(Settings settings, InternalClient client, XPackLicenseState licenseState) {
        super(settings);
        this.client = client;
        this.isTribeNode = settings.getGroups("tribe", true).isEmpty() == false;
        this.securityClient = new SecurityClient(client);
        this.licenseState = licenseState;
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

        if (securityIndexMappingAndTemplateUpToDate(clusterState, logger)) {
            canWrite = true;
        } else if (securityIndexMappingAndTemplateSufficientToRead(clusterState, logger)) {
            canWrite = false;
        } else {
            canWrite = false;
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
    public void getRoleDescriptors(String[] names, final ActionListener<Collection<RoleDescriptor>> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to get roles before service was started");
            listener.onResponse(Collections.emptySet());
            return;
        }
        if (names != null && names.length == 1) {
            getRoleDescriptor(Objects.requireNonNull(names[0]), ActionListener.wrap(roleDescriptor ->
                    listener.onResponse(roleDescriptor == null ? Collections.emptyList() : Collections.singletonList(roleDescriptor)),
                    listener::onFailure));
        } else {
            try {
                QueryBuilder query;
                if (names == null || names.length == 0) {
                    query = QueryBuilders.matchAllQuery();
                } else {
                    query = QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery(ROLE_DOC_TYPE).addIds(names));
                }
                SearchRequest request = client.prepareSearch(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .setTypes(ROLE_DOC_TYPE)
                        .setScroll(TimeValue.timeValueSeconds(10L))
                        .setQuery(query)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                request.indicesOptions().ignoreUnavailable();
                InternalClient.fetchAllByEntity(client, request, listener,
                        (hit) -> transformRole(hit.getId(), hit.getSourceRef(), logger, licenseState));
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to retrieve roles {}", Arrays.toString(names)), e);
                listener.onFailure(e);
            }
        }
    }

    public void deleteRole(final DeleteRoleRequest deleteRoleRequest, final ActionListener<Boolean> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to delete role [{}] before service was started", deleteRoleRequest.name());
            listener.onResponse(false);
        } else if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("roles may not be deleted using a tribe node"));
            return;
        } else if (canWrite == false) {
            listener.onFailure(new IllegalStateException("role cannot be deleted as service cannot write until template and " +
                    "mappings are up to date"));
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
        } else if (canWrite == false) {
            listener.onFailure(new IllegalStateException("role cannot be created or modified as service cannot write until template and " +
                    "mappings are up to date"));
        } else if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
            innerPutRole(request, role, listener);
        } else if (role.isUsingDocumentOrFieldLevelSecurity()) {
            listener.onFailure(LicenseUtils.newComplianceException("field and document level security"));
        } else {
            innerPutRole(request, role, listener);
        }
    }

    // pkg-private for testing
    void innerPutRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        try {
            client.prepareIndex(SecurityTemplateService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role.getName())
                    .setSource(role.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS, false))
                    .setRefreshPolicy(request.getRefreshPolicy())
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            final boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
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

        // FIXME this needs to be async
        long count = 0L;
        // query for necessary information
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

    private void getRoleDescriptor(final String roleId, ActionListener<RoleDescriptor> roleActionListener) {
        if (securityIndexExists == false) {
            roleActionListener.onResponse(null);
        } else {
            executeGetRoleRequest(roleId, new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse response) {
                    final RoleDescriptor descriptor = transformRole(response);
                    roleActionListener.onResponse(descriptor);
                }

                @Override
                public void onFailure(Exception e) {
                    // if the index or the shard is not there / available we just claim the role is not there
                    if (TransportActions.isShardNotAvailableException(e)) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to load role [{}] index not available",
                                roleId), e);
                        roleActionListener.onResponse(null);
                    } else {
                        logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to load role [{}]", roleId), e);
                        roleActionListener.onFailure(e);
                    }
                }
            });
        }
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


    // FIXME hack for testing
    public void reset() {
        final State state = state();
        if (state != State.STOPPED && state != State.FAILED) {
            throw new IllegalStateException("can only reset if stopped!!!");
        }
        this.securityIndexExists = false;
        this.canWrite = false;
        this.state.set(State.INITIALIZED);
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
        securityIndexExists = event.state().metaData().indices().get(SecurityTemplateService.SECURITY_INDEX_NAME) != null;
        canWrite = securityIndexMappingAndTemplateUpToDate(event.state(), logger);
    }

    public State state() {
        return state.get();
    }

    @Nullable
    private RoleDescriptor transformRole(GetResponse response) {
        if (response.isExists() == false) {
            return null;
        }

        return transformRole(response.getId(), response.getSourceAsBytesRef(), logger, licenseState);
    }

    @Nullable
    static RoleDescriptor transformRole(String name, BytesReference sourceBytes, Logger logger, XPackLicenseState licenseState) {
        try {
            // we pass true as last parameter because we do not want to reject permissions if the field permissions
            // are given in 2.x syntax
            RoleDescriptor roleDescriptor = RoleDescriptor.parse(name, sourceBytes, true, XContentType.JSON);
            if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
                return roleDescriptor;
            } else {
                final boolean dlsEnabled =
                        Arrays.stream(roleDescriptor.getIndicesPrivileges()).anyMatch(IndicesPrivileges::isUsingDocumentLevelSecurity);
                final boolean flsEnabled =
                        Arrays.stream(roleDescriptor.getIndicesPrivileges()).anyMatch(IndicesPrivileges::isUsingFieldLevelSecurity);
                if (dlsEnabled || flsEnabled) {
                    List<String> unlicensedFeatures = new ArrayList<>(2);
                    if (flsEnabled) {
                        unlicensedFeatures.add("fls");
                    }
                    if (dlsEnabled) {
                        unlicensedFeatures.add("dls");
                    }
                    Map<String, Object> transientMap = new HashMap<>(2);
                    transientMap.put("unlicensed_features", unlicensedFeatures);
                    transientMap.put("enabled", false);
                    return new RoleDescriptor(roleDescriptor.getName(), roleDescriptor.getClusterPrivileges(),
                            roleDescriptor.getIndicesPrivileges(), roleDescriptor.getRunAs(), roleDescriptor.getMetadata(), transientMap);
                } else {
                    return roleDescriptor;
                }

            }
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("error in the format of data for role [{}]", name), e);
            return null;
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(CACHE_SIZE_SETTING);
        settings.add(CACHE_TTL_SETTING);
    }
}
