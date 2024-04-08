/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ROLE_TYPE;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * NativeRolesStore is a {@code RolesStore} that, instead of reading from a
 * file, reads from an Elasticsearch index instead. Unlike the file-based roles
 * store, ESNativeRolesStore can be used to add a role to the store by inserting
 * the document into the administrative index.
 *
 * No caching is done by this class, it is handled at a higher level
 */
public class NativeRolesStore implements BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> {

    /**
     * This setting is never registered by the security plugin - in order to disable the native role APIs
     * another plugin must register it as a boolean setting and cause it to be set to `false`.
     *
     * If this setting is set to <code>false</code> then
     * <ul>
     *     <li>the Rest APIs for native role management are disabled.</li>
     *     <li>The native roles store will not resolve any roles</li>
     * </ul>
     */
    public static final String NATIVE_ROLES_ENABLED = "xpack.security.authc.native_roles.enabled";

    private static final Logger logger = LogManager.getLogger(NativeRolesStore.class);

    private final Settings settings;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final boolean enabled;

    private final SecurityIndexManager securityIndex;

    private final ClusterService clusterService;

    public NativeRolesStore(
        Settings settings,
        Client client,
        XPackLicenseState licenseState,
        SecurityIndexManager securityIndex,
        ClusterService clusterService
    ) {
        this.settings = settings;
        this.client = client;
        this.licenseState = licenseState;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = settings.getAsBoolean(NATIVE_ROLES_ENABLED, true);
    }

    @Override
    public void accept(Set<String> names, ActionListener<RoleRetrievalResult> listener) {
        getRoleDescriptors(names, listener);
    }

    /**
     * Retrieve a list of roles, if rolesToGet is null or empty, fetch all roles
     */
    public void getRoleDescriptors(Set<String> names, final ActionListener<RoleRetrievalResult> listener) {
        if (enabled == false) {
            listener.onResponse(RoleRetrievalResult.success(Set.of()));
            return;
        }

        final SecurityIndexManager frozenSecurityIndex = this.securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            // TODO remove this short circuiting and fix tests that fail without this!
            listener.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
        } else if (frozenSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onResponse(RoleRetrievalResult.failure(frozenSecurityIndex.getUnavailableReason(SEARCH_SHARDS)));
        } else if (names == null || names.isEmpty()) {
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                QueryBuilder query = QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE);
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
                        new ContextPreservingActionListener<>(
                            supplier,
                            ActionListener.wrap(
                                roles -> listener.onResponse(RoleRetrievalResult.success(new HashSet<>(roles))),
                                e -> listener.onResponse(RoleRetrievalResult.failure(e))
                            )
                        ),
                        (hit) -> transformRole(hit.getId(), hit.getSourceRef(), logger, licenseState)
                    );
                }
            });
        } else if (names.size() == 1) {
            getRoleDescriptor(Objects.requireNonNull(names.iterator().next()), listener);
        } else {
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final String[] roleIds = names.stream().map(NativeRolesStore::getIdForRole).toArray(String[]::new);
                MultiGetRequest multiGetRequest = client.prepareMultiGet().addIds(SECURITY_MAIN_ALIAS, roleIds).request();
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    multiGetRequest,
                    ActionListener.<MultiGetResponse>wrap(mGetResponse -> {
                        final MultiGetItemResponse[] responses = mGetResponse.getResponses();
                        Set<RoleDescriptor> descriptors = new HashSet<>();
                        for (int i = 0; i < responses.length; i++) {
                            MultiGetItemResponse item = responses[i];
                            if (item.isFailed()) {
                                final Exception failure = item.getFailure().getFailure();
                                for (int j = i + 1; j < responses.length; j++) {
                                    item = responses[j];
                                    if (item.isFailed()) {
                                        failure.addSuppressed(failure);
                                    }
                                }
                                listener.onResponse(RoleRetrievalResult.failure(failure));
                                return;
                            } else if (item.getResponse().isExists()) {
                                descriptors.add(transformRole(item.getResponse()));
                            }
                        }
                        listener.onResponse(RoleRetrievalResult.success(descriptors));
                    }, e -> listener.onResponse(RoleRetrievalResult.failure(e))),
                    client::multiGet
                );
            });
        }
    }

    public void deleteRole(final DeleteRoleRequest deleteRoleRequest, final ActionListener<Boolean> listener) {
        if (enabled == false) {
            listener.onFailure(new IllegalStateException("Native role management is disabled"));
            return;
        }

        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(false);
        } else if (frozenSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            securityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                DeleteRequest request = client.prepareDelete(SECURITY_MAIN_ALIAS, getIdForRole(deleteRoleRequest.name())).request();
                request.setRefreshPolicy(deleteRoleRequest.getRefreshPolicy());
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    request,
                    new ActionListener<DeleteResponse>() {
                        @Override
                        public void onResponse(DeleteResponse deleteResponse) {
                            clearRoleCache(
                                deleteRoleRequest.name(),
                                listener,
                                deleteResponse.getResult() == DocWriteResponse.Result.DELETED
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("failed to delete role from the index", e);
                            listener.onFailure(e);
                        }
                    },
                    client::delete
                );
            });
        }
    }

    public void putRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        if (enabled == false) {
            listener.onFailure(new IllegalStateException("Native role management is disabled"));
            return;
        }

        if (role.isUsingDocumentOrFieldLevelSecurity() && DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException("field and document level security"));
        } else if (role.hasRemoteIndicesPrivileges()
            && clusterService.state().getMinTransportVersion().before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)) {
                listener.onFailure(
                    new IllegalStateException(
                        "all nodes must have transport version ["
                            + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY
                            + "] or higher to support remote indices privileges"
                    )
                );
            } else {
                innerPutRole(request, role, listener);
            }
    }

    // pkg-private for testing
    void innerPutRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        final String roleName = role.getName();
        assert NativeRealmValidationUtil.validateRoleName(roleName, false) == null : "Role name was invalid or reserved: " + roleName;
        assert false == role.hasRestriction() : "restriction is not supported for native roles";

        securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            final XContentBuilder xContentBuilder;
            try {
                xContentBuilder = role.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS, true);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(getIdForRole(roleName))
                .setSource(xContentBuilder)
                .setRefreshPolicy(request.getRefreshPolicy())
                .request();
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                indexRequest,
                new ActionListener<DocWriteResponse>() {
                    @Override
                    public void onResponse(DocWriteResponse indexResponse) {
                        final boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                        logger.trace("Created role: [{}]", indexRequest);
                        clearRoleCache(roleName, listener, created);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(() -> "failed to put role [" + roleName + "]", e);
                        listener.onFailure(e);
                    }
                },
                client::index
            );
        });
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        Map<String, Object> usageStats = Maps.newMapWithExpectedSize(3);
        if (securityIndex.isAvailable(SEARCH_SHARDS) == false) {
            usageStats.put("size", 0L);
            usageStats.put("fls", false);
            usageStats.put("dls", false);
            listener.onResponse(usageStats);
        } else {
            securityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    client.prepareMultiSearch()
                        .add(
                            client.prepareSearch(SECURITY_MAIN_ALIAS)
                                .setQuery(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                .setTrackTotalHits(true)
                                .setSize(0)
                        )
                        .add(
                            client.prepareSearch(SECURITY_MAIN_ALIAS)
                                .setQuery(
                                    QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                        .must(
                                            QueryBuilders.boolQuery()
                                                .should(existsQuery("indices.field_security.grant"))
                                                .should(existsQuery("indices.field_security.except"))
                                                // for backwardscompat with 2.x
                                                .should(existsQuery("indices.fields"))
                                        )
                                )
                                .setTrackTotalHits(true)
                                .setSize(0)
                                .setTerminateAfter(1)
                        )
                        .add(
                            client.prepareSearch(SECURITY_MAIN_ALIAS)
                                .setQuery(
                                    QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                        .filter(existsQuery("indices.query"))
                                )
                                .setTrackTotalHits(true)
                                .setSize(0)
                                .setTerminateAfter(1)
                        )
                        .add(
                            client.prepareSearch(SECURITY_MAIN_ALIAS)
                                .setQuery(
                                    QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                        .filter(existsQuery("remote_indices"))
                                )
                                .setTrackTotalHits(true)
                                .setSize(0)
                        )
                        .request(),
                    new DelegatingActionListener<MultiSearchResponse, Map<String, Object>>(listener) {
                        @Override
                        public void onResponse(MultiSearchResponse items) {
                            Item[] responses = items.getResponses();
                            if (responses[0].isFailure()) {
                                usageStats.put("size", 0);
                            } else {
                                usageStats.put("size", responses[0].getResponse().getHits().getTotalHits().value);
                            }
                            if (responses[1].isFailure()) {
                                usageStats.put("fls", false);
                            } else {
                                usageStats.put("fls", responses[1].getResponse().getHits().getTotalHits().value > 0L);
                            }

                            if (responses[2].isFailure()) {
                                usageStats.put("dls", false);
                            } else {
                                usageStats.put("dls", responses[2].getResponse().getHits().getTotalHits().value > 0L);
                            }
                            if (responses[3].isFailure()) {
                                usageStats.put("remote_indices", 0);
                            } else {
                                usageStats.put("remote_indices", responses[3].getResponse().getHits().getTotalHits().value);
                            }
                            delegate.onResponse(usageStats);
                        }
                    },
                    client::multiSearch
                )
            );
        }
    }

    @Override
    public String toString() {
        return "native roles store";
    }

    private void getRoleDescriptor(final String roleId, ActionListener<RoleRetrievalResult> resultListener) {
        final SecurityIndexManager frozenSecurityIndex = this.securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            // TODO remove this short circuiting and fix tests that fail without this!
            resultListener.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
        } else if (frozenSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            resultListener.onResponse(RoleRetrievalResult.failure(frozenSecurityIndex.getUnavailableReason(PRIMARY_SHARDS)));
        } else {
            securityIndex.checkIndexVersionThenExecute(
                e -> resultListener.onResponse(RoleRetrievalResult.failure(e)),
                () -> executeGetRoleRequest(roleId, new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse response) {
                        final RoleDescriptor descriptor = transformRole(response);
                        resultListener.onResponse(
                            RoleRetrievalResult.success(descriptor == null ? Collections.emptySet() : Collections.singleton(descriptor))
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        resultListener.onResponse(RoleRetrievalResult.failure(e));
                    }
                })
            );
        }
    }

    private void executeGetRoleRequest(String role, ActionListener<GetResponse> listener) {
        securityIndex.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareGet(SECURITY_MAIN_ALIAS, getIdForRole(role)).request(),
                listener,
                client::get
            )
        );
    }

    private <Response> void clearRoleCache(final String role, ActionListener<Response> listener, Response response) {
        ClearRolesCacheRequest request = new ClearRolesCacheRequest().names(role);
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearRolesCacheAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(ClearRolesCacheResponse nodes) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "unable to clear cache for role [" + role + "]", e);
                ElasticsearchException exception = new ElasticsearchException(
                    "clearing the cache for [" + role + "] failed. please clear the role cache manually",
                    e
                );
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
    static RoleDescriptor transformRole(String id, BytesReference sourceBytes, Logger logger, XPackLicenseState licenseState) {
        assert id.startsWith(ROLE_TYPE) : "[" + id + "] does not have role prefix";
        final String name = id.substring(ROLE_TYPE.length() + 1);
        try {
            // we pass true as allow2xFormat parameter because we do not want to reject permissions if the field permissions
            // are given in 2.x syntax
            RoleDescriptor roleDescriptor = RoleDescriptor.parse(name, sourceBytes, true, XContentType.JSON, false);
            final boolean dlsEnabled = Arrays.stream(roleDescriptor.getIndicesPrivileges())
                .anyMatch(IndicesPrivileges::isUsingDocumentLevelSecurity);
            final boolean flsEnabled = Arrays.stream(roleDescriptor.getIndicesPrivileges())
                .anyMatch(IndicesPrivileges::isUsingFieldLevelSecurity);
            if ((dlsEnabled || flsEnabled) && DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState) == false) {
                List<String> unlicensedFeatures = new ArrayList<>(2);
                if (flsEnabled) {
                    unlicensedFeatures.add("fls");
                }
                if (dlsEnabled) {
                    unlicensedFeatures.add("dls");
                }
                Map<String, Object> transientMap = Maps.newMapWithExpectedSize(2);
                transientMap.put("unlicensed_features", unlicensedFeatures);
                transientMap.put("enabled", false);
                return new RoleDescriptor(
                    roleDescriptor.getName(),
                    roleDescriptor.getClusterPrivileges(),
                    roleDescriptor.getIndicesPrivileges(),
                    roleDescriptor.getApplicationPrivileges(),
                    roleDescriptor.getConditionalClusterPrivileges(),
                    roleDescriptor.getRunAs(),
                    roleDescriptor.getMetadata(),
                    transientMap,
                    roleDescriptor.getRemoteIndicesPrivileges(),
                    roleDescriptor.getRestriction()
                );
            } else {
                return roleDescriptor;
            }
        } catch (Exception e) {
            logger.error("error in the format of data for role [" + name + "]", e);
            return null;
        }
    }

    /**
     * Gets the document's id field for the given role name.
     */
    private static String getIdForRole(final String roleName) {
        return ROLE_TYPE + "-" + roleName;
    }
}
