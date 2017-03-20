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
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.security.Security.setting;

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

    private static final String ROLE_DOC_TYPE = "role";

    private final InternalClient client;
    private final XPackLicenseState licenseState;
    private final boolean isTribeNode;

    private SecurityClient securityClient;
    private final SecurityLifecycleService securityLifecycleService;

    public NativeRolesStore(Settings settings, InternalClient client, XPackLicenseState licenseState,
                            SecurityLifecycleService securityLifecycleService) {
        super(settings);
        this.client = client;
        this.isTribeNode = settings.getGroups("tribe", true).isEmpty() == false;
        this.securityClient = new SecurityClient(client);
        this.licenseState = licenseState;
        this.securityLifecycleService = securityLifecycleService;
    }

    /**
     * Retrieve a list of roles, if rolesToGet is null or empty, fetch all roles
     */
    public void getRoleDescriptors(String[] names, final ActionListener<Collection<RoleDescriptor>> listener) {
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
                SearchRequest request = client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
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
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("roles may not be deleted using a tribe node"));
            return;
        } else if (securityLifecycleService.canWriteToSecurityIndex() == false) {
            listener.onFailure(new IllegalStateException("role cannot be deleted as service cannot write until template and " +
                    "mappings are up to date"));
            return;
        }

        try {
            DeleteRequest request = client.prepareDelete(SecurityLifecycleService.SECURITY_INDEX_NAME,
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
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("roles may not be created or modified using a tribe node"));
        } else if (securityLifecycleService.canWriteToSecurityIndex() == false) {
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
            client.prepareIndex(SecurityLifecycleService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role.getName())
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

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        Map<String, Object> usageStats = new HashMap<>();
        if (securityLifecycleService.securityIndexExists() == false) {
            usageStats.put("size", 0L);
            usageStats.put("fls", false);
            usageStats.put("dls", false);
            listener.onResponse(usageStats);
        } else {
            client.prepareMultiSearch()
                    .add(client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                            .setTypes(ROLE_DOC_TYPE)
                            .setQuery(QueryBuilders.matchAllQuery())
                            .setSize(0))
                    .add(client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                        .setTypes(ROLE_DOC_TYPE)
                        .setQuery(QueryBuilders.boolQuery()
                                .should(existsQuery("indices.field_security.grant"))
                                .should(existsQuery("indices.field_security.except"))
                                // for backwardscompat with 2.x
                                .should(existsQuery("indices.fields")))
                        .setSize(0)
                        .setTerminateAfter(1))
                    .add(client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                        .setTypes(ROLE_DOC_TYPE)
                        .setQuery(existsQuery("indices.query"))
                        .setSize(0)
                        .setTerminateAfter(1))
                    .execute(new ActionListener<MultiSearchResponse>() {
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
                    });
        }
    }

    private void getRoleDescriptor(final String roleId, ActionListener<RoleDescriptor> roleActionListener) {
        if (securityLifecycleService.securityIndexExists() == false) {
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
            GetRequest request = client.prepareGet(SecurityLifecycleService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role).request();
            client.get(request, listener);
        } catch (IndexNotFoundException e) {
            logger.trace(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "unable to retrieve role [{}] since security index does not exist", role), e);
            listener.onResponse(new GetResponse(
                    new GetResult(SecurityLifecycleService.SECURITY_INDEX_NAME, ROLE_DOC_TYPE, role, -1, false, null, null)));
        } catch (Exception e) {
            logger.error("unable to retrieve role", e);
            listener.onFailure(e);
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
