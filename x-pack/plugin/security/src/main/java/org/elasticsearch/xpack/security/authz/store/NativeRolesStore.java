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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleResponse;
import org.elasticsearch.xpack.core.security.action.role.QueryRoleResponse.QueryRoleResult;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.TransportVersions.ROLE_REMOTE_CLUSTER_PRIVS;
import static org.elasticsearch.action.ValidateActions.addValidationError;
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
import static org.elasticsearch.xpack.security.support.SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_ROLES_METADATA_FLATTENED;

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

    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder()
        .allow2xFormat(true)
        .allowDescription(true)
        .build();

    private static final Set<DocWriteResponse.Result> UPDATE_ROLES_REFRESH_CACHE_RESULTS = Set.of(
        DocWriteResponse.Result.CREATED,
        DocWriteResponse.Result.UPDATED,
        DocWriteResponse.Result.DELETED
    );

    private final Settings settings;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final boolean enabled;

    private final SecurityIndexManager securityIndex;

    private final ClusterService clusterService;

    private final FeatureService featureService;

    private final ReservedRoleNameChecker reservedRoleNameChecker;

    private final NamedXContentRegistry xContentRegistry;

    public NativeRolesStore(
        Settings settings,
        Client client,
        XPackLicenseState licenseState,
        SecurityIndexManager securityIndex,
        ClusterService clusterService,
        FeatureService featureService,
        ReservedRoleNameChecker reservedRoleNameChecker,
        NamedXContentRegistry xContentRegistry
    ) {
        this.settings = settings;
        this.client = client;
        this.licenseState = licenseState;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.reservedRoleNameChecker = reservedRoleNameChecker;
        this.xContentRegistry = xContentRegistry;
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

    public boolean isMetadataSearchable() {
        SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        // the metadata is searchable if:
        // * the security index has been created anew (using the latest index version),
        // i.e. it is NOT created in a previous ES version that potentially didn't index the role metadata
        // * or, the .security index has been migrated (using an internal update-by-query) such that the metadata is queryable
        return frozenSecurityIndex.isCreatedOnLatestVersion()
            || securityIndex.isMigrationsVersionAtLeast(ROLE_METADATA_FLATTENED_MIGRATION_VERSION);
    }

    public void queryRoleDescriptors(SearchSourceBuilder searchSourceBuilder, ActionListener<QueryRoleResult> listener) {
        SearchRequest searchRequest = new SearchRequest(new String[] { SECURITY_MAIN_ALIAS }, searchSourceBuilder);
        SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(QueryRoleResult.EMPTY);
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
                            logger.debug("No roles found for query [{}]", searchRequest.source().query());
                            listener.onResponse(QueryRoleResult.EMPTY);
                            return;
                        }
                        SearchHit[] hits = searchResponse.getHits().getHits();
                        List<QueryRoleResponse.Item> items = Arrays.stream(hits).map(hit -> {
                            RoleDescriptor roleDescriptor = transformRole(hit.getId(), hit.getSourceRef(), logger, licenseState);
                            if (roleDescriptor == null) {
                                return null;
                            }
                            return new QueryRoleResponse.Item(roleDescriptor, hit.getSortValues());
                        }).filter(Objects::nonNull).toList();
                        listener.onResponse(new QueryRoleResult(total, items));
                    }, listener::onFailure)
                )
            );
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
                DeleteRequest request = createRoleDeleteRequest(deleteRoleRequest.name());
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

    public void deleteRoles(
        final List<String> roleNames,
        WriteRequest.RefreshPolicy refreshPolicy,
        final ActionListener<BulkRolesResponse> listener
    ) {
        if (enabled == false) {
            listener.onFailure(new IllegalStateException("Native role management is disabled"));
            return;
        }

        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
        Map<String, Exception> validationErrorByRoleName = new HashMap<>();

        for (String roleName : roleNames) {
            if (reservedRoleNameChecker.isReserved(roleName)) {
                validationErrorByRoleName.put(
                    roleName,
                    new IllegalArgumentException("role [" + roleName + "] is reserved and cannot be deleted")
                );
            } else {
                bulkRequest.add(createRoleDeleteRequest(roleName));
            }
        }

        if (bulkRequest.numberOfActions() == 0) {
            bulkResponseWithOnlyValidationErrors(roleNames, validationErrorByRoleName, listener);
            return;
        }

        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();
        if (frozenSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(new BulkRolesResponse(List.of()));
        } else if (frozenSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            securityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    bulkRequest,
                    new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            bulkResponseAndRefreshRolesCache(roleNames, bulkResponse, validationErrorByRoleName, listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(() -> "failed to delete roles", e);
                            listener.onFailure(e);
                        }
                    },
                    client::bulk
                )
            );
        }
    }

    private void bulkResponseAndRefreshRolesCache(
        List<String> roleNames,
        BulkResponse bulkResponse,
        Map<String, Exception> validationErrorByRoleName,
        ActionListener<BulkRolesResponse> listener
    ) {
        Iterator<BulkItemResponse> bulkItemResponses = bulkResponse.iterator();
        BulkRolesResponse.Builder bulkPutRolesResponseBuilder = new BulkRolesResponse.Builder();
        List<String> rolesToRefreshInCache = new ArrayList<>(roleNames.size());
        roleNames.stream().map(roleName -> {
            if (validationErrorByRoleName.containsKey(roleName)) {
                return BulkRolesResponse.Item.failure(roleName, validationErrorByRoleName.get(roleName));
            }
            BulkItemResponse resp = bulkItemResponses.next();
            if (resp.isFailed()) {
                return BulkRolesResponse.Item.failure(roleName, resp.getFailure().getCause());
            }
            if (UPDATE_ROLES_REFRESH_CACHE_RESULTS.contains(resp.getResponse().getResult())) {
                rolesToRefreshInCache.add(roleName);
            }
            return BulkRolesResponse.Item.success(roleName, resp.getResponse().getResult());
        }).forEach(bulkPutRolesResponseBuilder::addItem);

        clearRoleCache(rolesToRefreshInCache.toArray(String[]::new), ActionListener.wrap(res -> {
            listener.onResponse(bulkPutRolesResponseBuilder.build());
        }, listener::onFailure), bulkResponse);
    }

    private void bulkResponseWithOnlyValidationErrors(
        List<String> roleNames,
        Map<String, Exception> validationErrorByRoleName,
        ActionListener<BulkRolesResponse> listener
    ) {
        BulkRolesResponse.Builder bulkRolesResponseBuilder = new BulkRolesResponse.Builder();
        roleNames.stream()
            .map(roleName -> BulkRolesResponse.Item.failure(roleName, validationErrorByRoleName.get(roleName)))
            .forEach(bulkRolesResponseBuilder::addItem);

        listener.onResponse(bulkRolesResponseBuilder.build());
    }

    private void executeAsyncRolesBulkRequest(BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        securityIndex.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN, bulkRequest, listener, client::bulk)
        );
    }

    private Exception validateRoleDescriptor(RoleDescriptor role) {
        ActionRequestValidationException validationException = null;
        validationException = RoleDescriptorRequestValidator.validate(role, validationException);

        if (reservedRoleNameChecker.isReserved(role.getName())) {
            throw addValidationError("Role [" + role.getName() + "] is reserved and may not be used.", validationException);
        }

        if (role.isUsingDocumentOrFieldLevelSecurity() && DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState) == false) {
            return LicenseUtils.newComplianceException("field and document level security");
        } else if (role.hasRemoteIndicesPrivileges()
            && clusterService.state().getMinTransportVersion().before(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)) {
                return new IllegalStateException(
                    "all nodes must have version ["
                        + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                        + "] or higher to support remote indices privileges"
                );
            } else if (role.hasRemoteClusterPermissions()
                && clusterService.state().getMinTransportVersion().before(ROLE_REMOTE_CLUSTER_PRIVS)) {
                    return new IllegalStateException(
                        "all nodes must have version [" + ROLE_REMOTE_CLUSTER_PRIVS + "] or higher to support remote cluster privileges"
                    );
                } else if (role.hasDescription()
                    && clusterService.state().getMinTransportVersion().before(TransportVersions.SECURITY_ROLE_DESCRIPTION)) {
                        return new IllegalStateException(
                            "all nodes must have version ["
                                + TransportVersions.SECURITY_ROLE_DESCRIPTION.toReleaseVersion()
                                + "] or higher to support specifying role description"
                        );
                    } else if (Arrays.stream(role.getConditionalClusterPrivileges())
                        .anyMatch(privilege -> privilege instanceof ConfigurableClusterPrivileges.ManageRolesPrivilege)
                        && clusterService.state().getMinTransportVersion().before(TransportVersions.ADD_MANAGE_ROLES_PRIVILEGE)) {
                            return new IllegalStateException(
                                "all nodes must have version ["
                                    + TransportVersions.ADD_MANAGE_ROLES_PRIVILEGE.toReleaseVersion()
                                    + "] or higher to support the manage roles privilege"
                            );
                        }
        try {
            DLSRoleQueryValidator.validateQueryField(role.getIndicesPrivileges(), xContentRegistry);
        } catch (ElasticsearchException | IllegalArgumentException e) {
            return e;
        }

        return validationException;
    }

    public void putRole(final WriteRequest.RefreshPolicy refreshPolicy, final RoleDescriptor role, final ActionListener<Boolean> listener) {
        if (enabled == false) {
            listener.onFailure(new IllegalStateException("Native role management is disabled"));
            return;
        }
        Exception validationException = validateRoleDescriptor(role);

        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }

        try {
            IndexRequest indexRequest = createRoleIndexRequest(role);
            indexRequest.setRefreshPolicy(refreshPolicy);
            securityIndex.prepareIndexIfNeededThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    indexRequest,
                    new ActionListener<DocWriteResponse>() {
                        @Override
                        public void onResponse(DocWriteResponse indexResponse) {
                            final boolean created = indexResponse.getResult() == DocWriteResponse.Result.CREATED;
                            logger.trace("Created role: [{}]", indexRequest);
                            clearRoleCache(role.getName(), listener, created);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error(() -> "failed to put role [" + role.getName() + "]", e);
                            listener.onFailure(e);
                        }
                    },
                    client::index
                )
            );
        } catch (IOException exception) {
            listener.onFailure(exception);
        }
    }

    public void putRoles(
        final WriteRequest.RefreshPolicy refreshPolicy,
        final List<RoleDescriptor> roles,
        final ActionListener<BulkRolesResponse> listener
    ) {
        if (enabled == false) {
            listener.onFailure(new IllegalStateException("Native role management is disabled"));
            return;
        }
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
        Map<String, Exception> validationErrorByRoleName = new HashMap<>();

        for (RoleDescriptor role : roles) {
            Exception validationException;
            try {
                validationException = validateRoleDescriptor(role);
            } catch (Exception e) {
                validationException = e;
            }

            if (validationException != null) {
                validationErrorByRoleName.put(role.getName(), validationException);
            } else {
                try {
                    bulkRequest.add(createRoleUpsertRequest(role));
                } catch (IOException ioException) {
                    listener.onFailure(ioException);
                }
            }
        }

        List<String> roleNames = roles.stream().map(RoleDescriptor::getName).toList();

        if (bulkRequest.numberOfActions() == 0) {
            bulkResponseWithOnlyValidationErrors(roleNames, validationErrorByRoleName, listener);
            return;
        }

        securityIndex.prepareIndexIfNeededThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                bulkRequest,
                new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        bulkResponseAndRefreshRolesCache(roleNames, bulkResponse, validationErrorByRoleName, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(() -> "failed to put roles", e);
                        listener.onFailure(e);
                    }
                },
                client::bulk
            )
        );
    }

    private IndexRequest createRoleIndexRequest(final RoleDescriptor role) throws IOException {
        return client.prepareIndex(SECURITY_MAIN_ALIAS)
            .setId(getIdForRole(role.getName()))
            .setSource(createRoleXContentBuilder(role))
            .request();
    }

    private UpdateRequest createRoleUpsertRequest(final RoleDescriptor role) throws IOException {
        return client.prepareUpdate(SECURITY_MAIN_ALIAS, getIdForRole(role.getName()))
            .setDoc(createRoleXContentBuilder(role))
            .setDocAsUpsert(true)
            .request();
    }

    private DeleteRequest createRoleDeleteRequest(final String roleName) {
        return client.prepareDelete(SECURITY_MAIN_ALIAS, getIdForRole(roleName)).request();
    }

    // Package private for testing
    XContentBuilder createRoleXContentBuilder(RoleDescriptor role) throws IOException {
        assert NativeRealmValidationUtil.validateRoleName(role.getName(), false) == null
            : "Role name was invalid or reserved: " + role.getName();
        assert false == role.hasRestriction() : "restriction is not supported for native roles";

        XContentBuilder builder = jsonBuilder().startObject();
        role.innerToXContent(builder, ToXContent.EMPTY_PARAMS, true);

        if (featureService.clusterHasFeature(clusterService.state(), SECURITY_ROLES_METADATA_FLATTENED)) {
            builder.field(RoleDescriptor.Fields.METADATA_FLATTENED.getPreferredName(), role.getMetadata());
        }

        // When role descriptor XContent is generated for the security index all empty fields need to have default values to make sure
        // existing values are overwritten if not present since the request to update could be an UpdateRequest
        // (update provided fields in existing document or create document) or IndexRequest (replace and reindex document)
        if (role.hasConfigurableClusterPrivileges() == false) {
            builder.startObject(RoleDescriptor.Fields.GLOBAL.getPreferredName()).endObject();
        }

        if (role.hasRemoteIndicesPrivileges() == false) {
            builder.field(RoleDescriptor.Fields.REMOTE_INDICES.getPreferredName(), RoleDescriptor.RemoteIndicesPrivileges.NONE);
        }

        if (role.hasRemoteClusterPermissions() == false
            && clusterService.state().getMinTransportVersion().onOrAfter(ROLE_REMOTE_CLUSTER_PRIVS)) {
            builder.array(RoleDescriptor.Fields.REMOTE_CLUSTER.getPreferredName(), RemoteClusterPermissions.NONE);
        }
        if (role.hasDescription() == false
            && clusterService.state().getMinTransportVersion().onOrAfter(TransportVersions.SECURITY_ROLE_DESCRIPTION)) {
            builder.field(RoleDescriptor.Fields.DESCRIPTION.getPreferredName(), "");
        }

        builder.endObject();
        return builder;
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
                        .add(
                            client.prepareSearch(SECURITY_MAIN_ALIAS)
                                .setQuery(
                                    QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termQuery(RoleDescriptor.Fields.TYPE.getPreferredName(), ROLE_TYPE))
                                        .filter(existsQuery("remote_cluster"))
                                )
                                .setTrackTotalHits(true)
                                .setSize(0)
                        )
                        .request(),
                    new DelegatingActionListener<MultiSearchResponse, Map<String, Object>>(listener) {
                        @Override
                        public void onResponse(MultiSearchResponse items) {
                            MultiSearchResponse.Item[] responses = items.getResponses();
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
                            if (responses[4].isFailure()) {
                                usageStats.put("remote_cluster", 0);
                            } else {
                                usageStats.put("remote_cluster", responses[4].getResponse().getHits().getTotalHits().value);
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
        clearRoleCache(new String[] { role }, listener, response);
    }

    private <Response> void clearRoleCache(final String[] roles, ActionListener<Response> listener, Response response) {
        ClearRolesCacheRequest request = new ClearRolesCacheRequest().names(roles);
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearRolesCacheAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(ClearRolesCacheResponse nodes) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "unable to clear cache for roles [" + Arrays.toString(roles) + "]", e);
                ElasticsearchException exception = new ElasticsearchException(
                    "clearing the cache for [" + Arrays.toString(roles) + "] failed. please clear the role cache manually",
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
            // we do not want to reject permissions if the field permissions are given in 2.x syntax, hence why we allow2xFormat
            RoleDescriptor roleDescriptor = ROLE_DESCRIPTOR_PARSER.parse(name, sourceBytes, XContentType.JSON);
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
                    roleDescriptor.getRemoteClusterPermissions(),
                    roleDescriptor.getRestriction(),
                    roleDescriptor.getDescription()
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
