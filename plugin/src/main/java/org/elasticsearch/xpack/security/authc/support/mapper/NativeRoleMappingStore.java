/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.client.SecurityClient;

import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.DELETED;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;

/**
 * This store reads + writes {@link ExpressionRoleMapping role mappings} in an Elasticsearch
 * {@link SecurityLifecycleService#SECURITY_INDEX_NAME index}.
 * <br>
 * The store is responsible for all read and write operations as well as
 * {@link #resolveRoles(UserData, ActionListener) resolving roles}.
 * <p>
 * No caching is done by this class, it is handled at a higher level and no polling for changes
 * is done by this class. Modification operations make a best effort attempt to clear the cache
 * on all nodes for the user that was modified.
 */
public class NativeRoleMappingStore extends AbstractComponent implements UserRoleMapper {

    static final String DOC_TYPE_FIELD = "doc_type";
    static final String DOC_TYPE_ROLE_MAPPING = "role-mapping";

    private static final String ID_PREFIX = DOC_TYPE_ROLE_MAPPING + "_";

    private static final String SECURITY_GENERIC_TYPE = "doc";

    private final InternalClient client;
    private final boolean isTribeNode;
    private final SecurityLifecycleService securityLifecycleService;
    private final List<String> realmsToRefresh = new CopyOnWriteArrayList<>();

    public NativeRoleMappingStore(Settings settings, InternalClient client, SecurityLifecycleService securityLifecycleService) {
        super(settings);
        this.client = client;
        this.isTribeNode = XPackPlugin.isTribeNode(settings);
        this.securityLifecycleService = securityLifecycleService;
    }

    private String getNameFromId(String id) {
        assert id.startsWith(ID_PREFIX);
        return id.substring(ID_PREFIX.length());
    }

    private String getIdForName(String name) {
        return ID_PREFIX + name;
    }

    /**
     * Loads all mappings from the index.
     * <em>package private</em> for unit testing
     */
    void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
        if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        }
        final QueryBuilder query = QueryBuilders.termQuery(DOC_TYPE_FIELD, DOC_TYPE_ROLE_MAPPING);
        SearchRequest request = client.prepareSearch(SECURITY_INDEX_NAME)
                .setScroll(TimeValue.timeValueSeconds(10L))
                .setTypes(SECURITY_GENERIC_TYPE)
                .setQuery(query)
                .setSize(1000)
                .setFetchSource(true)
                .request();
        request.indicesOptions().ignoreUnavailable();
        InternalClient.fetchAllByEntity(client, request, ActionListener.wrap((Collection<ExpressionRoleMapping> mappings) ->
                        listener.onResponse(mappings.stream().filter(Objects::nonNull).collect(Collectors.toList())),
                ex -> {
                    logger.error(new ParameterizedMessage("failed to load role mappings from index [{}] skipping all mappings.",
                            SECURITY_INDEX_NAME), ex);
                    listener.onResponse(Collections.emptyList());
                }),
                doc -> buildMapping(getNameFromId(doc.getId()), doc.getSourceRef()));
    }

    private ExpressionRoleMapping buildMapping(String id, BytesReference source) {
        try (XContentParser parser = getParser(source)) {
            return ExpressionRoleMapping.parse(id, parser);
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("Role mapping [{}] cannot be parsed and will be skipped", id), e);
            return null;
        }
    }

    private static XContentParser getParser(BytesReference source) throws IOException {
        return XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, source);
    }

    /**
     * Stores (create or update) a single mapping in the index
     */
    public void putRoleMapping(PutRoleMappingRequest request, ActionListener<Boolean> listener) {
        modifyMapping(request.getName(), this::innerPutMapping, request, listener);
    }

    /**
     * Deletes a named mapping from the index
     */
    public void deleteRoleMapping(DeleteRoleMappingRequest request, ActionListener<Boolean> listener) {
        modifyMapping(request.getName(), this::innerDeleteMapping, request, listener);
    }

    private <Request, Result> void modifyMapping(String name, CheckedBiConsumer<Request, ActionListener<Result>, Exception> inner,
                                                 Request request, ActionListener<Result> listener) {
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("role-mappings may not be modified using a tribe node"));
        } else if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
        } else if (securityLifecycleService.isSecurityIndexWriteable() == false) {
            listener.onFailure(new IllegalStateException("role-mappings cannot be modified until template and mappings are up to date"));
        } else {
            try {
                inner.accept(request, ActionListener.wrap(r -> refreshRealms(listener, r), listener::onFailure));
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("failed to modify role-mapping [{}]", name), e);
                listener.onFailure(e);
            }
        }
    }

    private void innerPutMapping(PutRoleMappingRequest request, ActionListener<Boolean> listener) {
        final ExpressionRoleMapping mapping = request.getMapping();
        securityLifecycleService.createIndexIfNeededThenExecute(listener, () -> {
            final XContentBuilder xContentBuilder;
            try {
                xContentBuilder = mapping.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS, true);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            client.prepareIndex(SECURITY_INDEX_NAME, SECURITY_GENERIC_TYPE, getIdForName(mapping.getName()))
                .setSource(xContentBuilder)
                .setRefreshPolicy(request.getRefreshPolicy())
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        boolean created = indexResponse.getResult() == CREATED;
                        listener.onResponse(created);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(new ParameterizedMessage("failed to put role-mapping [{}]", mapping.getName()), e);
                        listener.onFailure(e);
                    }
                });
        });
    }

    private void innerDeleteMapping(DeleteRoleMappingRequest request, ActionListener<Boolean> listener) throws IOException {
        if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        }
        client.prepareDelete(SECURITY_INDEX_NAME, SECURITY_GENERIC_TYPE, getIdForName(request.getName()))
                .setRefreshPolicy(request.getRefreshPolicy())
                .execute(new ActionListener<DeleteResponse>() {

                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        boolean deleted = deleteResponse.getResult() == DELETED;
                        listener.onResponse(deleted);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(new ParameterizedMessage("failed to delete role-mapping [{}]", request.getName()), e);
                        listener.onFailure(e);

                    }
                });
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
            getMappings(new ActionListener<List<ExpressionRoleMapping>>() {
                @Override
                public void onResponse(List<ExpressionRoleMapping> mappings) {
                    final List<ExpressionRoleMapping> filtered = mappings.stream()
                            .filter(m -> names.contains(m.getName()))
                            .collect(Collectors.toList());
                    listener.onResponse(filtered);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    private void getMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
        if (securityLifecycleService.isSecurityIndexAvailable()) {
            loadMappings(listener);
        } else {
            logger.info("The security index is not yet available - no role mappings can be loaded");
            if (logger.isDebugEnabled()) {
                logger.debug("Security Index [{}] [exists: {}] [available: {}] [writable: {}]",
                        SECURITY_INDEX_NAME,
                        securityLifecycleService.isSecurityIndexExisting(),
                        securityLifecycleService.isSecurityIndexAvailable(),
                        securityLifecycleService.isSecurityIndexWriteable()
                );
            }
            listener.onResponse(Collections.emptyList());
        }
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
        if (securityLifecycleService.isSecurityIndexExisting() == false) {
            reportStats(listener, Collections.emptyList());
        } else {
            getMappings(ActionListener.wrap(mappings -> reportStats(listener, mappings), listener::onFailure));
        }
    }

    private void reportStats(ActionListener<Map<String, Object>> listener, List<ExpressionRoleMapping> mappings) {
        Map<String, Object> usageStats = new HashMap<>();
        usageStats.put("size", mappings.size());
        usageStats.put("enabled", mappings.stream().filter(ExpressionRoleMapping::isEnabled).count());
        listener.onResponse(usageStats);
    }

    private <Result> void refreshRealms(ActionListener<Result> listener, Result result) {
        String[] realmNames = this.realmsToRefresh.toArray(new String[realmsToRefresh.size()]);
        new SecurityClient(this.client).prepareClearRealmCache().realms(realmNames).execute(ActionListener.wrap(
                response -> {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "Cleared cached in realms [{}] due to role mapping change", Arrays.toString(realmNames)));
                    listener.onResponse(result);
                },
                ex -> {
                    logger.warn("Failed to clear cache for realms [{}]", Arrays.toString(realmNames));
                    listener.onFailure(ex);
                })
        );
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        getRoleMappings(null, ActionListener.wrap(
                mappings -> {
                    final Map<String, Object> userDataMap = user.asMap();
                    Stream<ExpressionRoleMapping> stream = mappings.stream()
                            .filter(ExpressionRoleMapping::isEnabled)
                            .filter(m -> m.getExpression().match(userDataMap));
                    if (logger.isTraceEnabled()) {
                        stream = stream.map(m -> {
                            logger.trace("User [{}] matches role-mapping [{}] with roles [{}]", user.getUsername(), m.getName(),
                                    m.getRoles());
                            return m;
                        });
                    }
                    final Set<String> roles = stream.flatMap(m -> m.getRoles().stream()).collect(Collectors.toSet());
                    logger.debug("Mapping user [{}] to roles [{}]", user, roles);
                    listener.onResponse(roles);
                }, listener::onFailure
        ));
    }

    /**
     * Indicates that the provided realm should have its cache cleared if this store is updated
     * (that is, {@link #putRoleMapping(PutRoleMappingRequest, ActionListener)} or
     * {@link #deleteRoleMapping(DeleteRoleMappingRequest, ActionListener)} are called).
     * @see org.elasticsearch.xpack.security.action.realm.ClearRealmCacheAction
     */
    @Override
    public void refreshRealmOnChange(CachingUsernamePasswordRealm realm) {
        realmsToRefresh.add(realm.name());
    }
}
