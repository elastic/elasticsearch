/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.stashWithOrigin;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ROLE_TYPE;

/**
 * NativeRolesStore is a {@code RolesStore} that, instead of reading from a
 * file, reads from an Elasticsearch index instead. Unlike the file-based roles
 * store, ESNativeRolesStore can be used to add a role to the store by inserting
 * the document into the administrative index.
 *
 * No caching is done by this class, it is handled at a higher level
 */
public class NativeRolesStore extends AbstractComponent {

    // these are no longer used, but leave them around for users upgrading
    private static final Setting<Integer> CACHE_SIZE_SETTING =
            Setting.intSetting(setting("authz.store.roles.index.cache.max_size"), 10000, Property.NodeScope, Property.Deprecated);
    private static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(setting("authz.store.roles.index.cache.ttl"),
            TimeValue.timeValueMinutes(20), Property.NodeScope, Property.Deprecated);
    private static final String ROLE_DOC_TYPE = "doc";

    private final Client client;
    private final XPackLicenseState licenseState;

    private SecurityClient securityClient;
    private final SecurityIndexManager securityIndex;

    public NativeRolesStore(Settings settings, Client client, XPackLicenseState licenseState, SecurityIndexManager securityIndex) {
        super(settings);
        this.client = client;
        this.securityClient = new SecurityClient(client);
        this.licenseState = licenseState;
        this.securityIndex = securityIndex;
    }

    /**
     * Retrieve a list of roles, if rolesToGet is null or empty, fetch all roles
     */
    public void getRoleDescriptors(String[] names, final ActionListener<Collection<RoleDescriptor>> listener) {
        if (securityIndex.indexExists() == false) {
            // TODO remove this short circuiting and fix tests that fail without this!
            listener.onResponse(Collections.emptyList());
        } else if (names != null && names.length == 1) {
            getRoleDescriptor(Objects.requireNonNull(names[0]), ActionListener.wrap(roleDescriptor ->
                    listener.onResponse(roleDescriptor == null ? Collections.emptyList() : Collections.singletonList(roleDescriptor)),
                    listener::onFailure));
        } else {
            securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
                QueryBuilder query;
                if (names == null || names.length == 0) {
                    query = QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE);
                } else {
                    final String[] roleNames = Arrays.stream(names).map(s -> getIdForUser(s)).toArray(String[]::new);
                    query = QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery(ROLE_DOC_TYPE).addIds(roleNames));
                }
                final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN)) {
                    SearchRequest request = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                            .setScroll(TimeValue.timeValueSeconds(10L))
                            .setQuery(query)
                            .setSize(1000)
                            .setFetchSource(true)
                            .request();
                    request.indicesOptions().ignoreUnavailable();
                    ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(supplier, listener),
                            (hit) -> transformRole(hit.getId(), hit.getSourceRef(), logger, licenseState));
                }
            });
        }
    }

    public void deleteRole(final DeleteRoleRequest deleteRoleRequest, final ActionListener<Boolean> listener) {
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            DeleteRequest request = client.prepareDelete(SecurityIndexManager.SECURITY_INDEX_NAME,
                    ROLE_DOC_TYPE, getIdForUser(deleteRoleRequest.name())).request();
            request.setRefreshPolicy(deleteRoleRequest.getRefreshPolicy());
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
                    new ActionListener<DeleteResponse>() {
                        @Override
                        public void onResponse(DeleteResponse deleteResponse) {
                            clearRoleCache(deleteRoleRequest.name(), listener,
                                    deleteResponse.getResult() == DocWriteResponse.Result.DELETED);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("failed to delete role from the index", e);
                            listener.onFailure(e);
                        }
                    }, client::delete);
        });
    }

    public void putRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
            innerPutRole(request, role, listener);
        } else if (role.isUsingDocumentOrFieldLevelSecurity()) {
            listener.onFailure(LicenseUtils.newComplianceException("field and document level security"));
        } else {
            innerPutRole(request, role, listener);
        }
    }

    // pkg-private for testing
    void innerPutRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final XContentBuilder xContentBuilder;
            try {
                xContentBuilder = role.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS, true);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                    client.prepareIndex(SecurityIndexManager.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, getIdForUser(role.getName()))
                            .setSource(xContentBuilder)
                            .setRefreshPolicy(request.getRefreshPolicy())
                            .request(),
                    new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            final boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                            clearRoleCache(role.getName(), listener, created);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(new ParameterizedMessage("failed to put role [{}]", request.name()), e);
                            listener.onFailure(e);
                        }
                    }, client::index);
        });
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        Map<String, Object> usageStats = new HashMap<>(3);
        if (securityIndex.indexExists() == false) {
            usageStats.put("size", 0L);
            usageStats.put("fls", false);
            usageStats.put("dls", false);
            listener.onResponse(usageStats);
        } else {
            securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () ->
                executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                    client.prepareMultiSearch()
                        .add(client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                            .setQuery(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                            .setSize(0))
                        .add(client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                            .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                .must(QueryBuilders.boolQuery()
                                    .should(existsQuery("indices.field_security.grant"))
                                    .should(existsQuery("indices.field_security.except"))
                                    // for backwardscompat with 2.x
                                    .should(existsQuery("indices.fields"))))
                            .setSize(0)
                            .setTerminateAfter(1))
                        .add(client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                            .setQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                .filter(existsQuery("indices.query")))
                            .setSize(0)
                            .setTerminateAfter(1))
                        .request(),
                    new ActionListener<MultiSearchResponse>() {
                        @Override
                        public void onResponse(MultiSearchResponse items) {
                            Item[] responses = items.getResponses();
                            if (responses[0].isFailure()) {
                                usageStats.put("size", 0);
                            } else {
                                usageStats.put("size", responses[0].getResponse().getHits().getTotalHits());
                            }

                            if (responses[1].isFailure()) {
                                usageStats.put("fls", false);
                            } else {
                                usageStats.put("fls", responses[1].getResponse().getHits().getTotalHits() > 0L);
                            }

                            if (responses[2].isFailure()) {
                                usageStats.put("dls", false);
                            } else {
                                usageStats.put("dls", responses[2].getResponse().getHits().getTotalHits() > 0L);
                            }
                            listener.onResponse(usageStats);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }, client::multiSearch));
        }
    }

    private void getRoleDescriptor(final String roleId, ActionListener<RoleDescriptor> roleActionListener) {
        if (securityIndex.indexExists() == false) {
            // TODO remove this short circuiting and fix tests that fail without this!
            roleActionListener.onResponse(null);
        } else {
            securityIndex.prepareIndexIfNeededThenExecute(roleActionListener::onFailure, () ->
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
                                logger.warn((org.apache.logging.log4j.util.Supplier<?>) () ->
                                        new ParameterizedMessage("failed to load role [{}] index not available", roleId), e);
                                roleActionListener.onResponse(null);
                            } else {
                                logger.error(new ParameterizedMessage("failed to load role [{}]", roleId), e);
                                roleActionListener.onFailure(e);
                            }
                        }
                    }));
        }
    }

    private void executeGetRoleRequest(String role, ActionListener<GetResponse> listener) {
        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () ->
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                    client.prepareGet(SecurityIndexManager.SECURITY_INDEX_NAME,
                            ROLE_DOC_TYPE, getIdForUser(role)).request(),
                    listener,
                    client::get));
    }

    private <Response> void clearRoleCache(final String role, ActionListener<Response> listener, Response response) {
        ClearRolesCacheRequest request = new ClearRolesCacheRequest().names(role);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, request,
                new ActionListener<ClearRolesCacheResponse>() {
                    @Override
                    public void onResponse(ClearRolesCacheResponse nodes) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(new ParameterizedMessage("unable to clear cache for role [{}]", role), e);
                        ElasticsearchException exception = new ElasticsearchException("clearing the cache for [" + role
                                + "] failed. please clear the role cache manually", e);
                        listener.onFailure(exception);
                    }
                }, securityClient::clearRolesCache);
    }

    @Nullable
    private RoleDescriptor transformRole(GetResponse response) {
        if (response.isExists() == false) {
            return null;
        }

        return transformRole(response.getId(), response.getSourceAsBytesRef(), logger, licenseState);
    }

    @Nullable
    static RoleDescriptor transformRole(String id, BytesReference sourceBytes, Logger logger, XPackLicenseState licenseState) {
        assert id.startsWith(ROLE_TYPE) : "[" + id + "] does not have role prefix";
        final String name = id.substring(ROLE_TYPE.length() + 1);
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
            logger.error(new ParameterizedMessage("error in the format of data for role [{}]", name), e);
            return null;
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(CACHE_SIZE_SETTING);
        settings.add(CACHE_TTL_SETTING);
    }

    /**
     * Gets the document's id field for the given role name.
     */
    private static String getIdForUser(final String roleName) {
        return ROLE_TYPE + "-" + roleName;
    }
}
