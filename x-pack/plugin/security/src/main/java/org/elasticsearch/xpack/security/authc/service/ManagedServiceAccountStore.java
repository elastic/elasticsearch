/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheAction;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * Index-backed store for API-managed service account definitions. No caching is performed; each authentication reads the account document.
 */
public class ManagedServiceAccountStore {

    public static final TransportVersion MANAGED_SERVICE_ACCOUNTS = ServiceAccountInfo.MANAGED_SERVICE_ACCOUNTS;

    static final String SERVICE_ACCOUNT_DOC_TYPE = "service_account";
    static final String ACCOUNT_GENERATION_ID_FIELD = "account_generation_id";

    private static final Logger logger = LogManager.getLogger(ManagedServiceAccountStore.class);

    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Settings settings;

    public ManagedServiceAccountStore(Settings settings, Client client, SecurityIndexManager securityIndex, ClusterService clusterService) {
        this.settings = settings;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
    }

    public void getByPrincipal(String principal, ActionListener<ManagedServiceAccount> listener) {
        final String principalError = ManagedServiceAccountIdValidator.validatePrincipal(principal);
        if (principalError != null) {
            listener.onFailure(new IllegalArgumentException(principalError));
            return;
        }
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(null);
            return;
        }
        if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
            return;
        }
        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final GetRequest getRequest = client.prepareGet(SECURITY_MAIN_ALIAS, docIdForPrincipal(principal))
                .setFetchSource(true)
                .request();
            executeAsyncWithOrigin(client, SECURITY_ORIGIN, TransportGetAction.TYPE, getRequest, ActionListener.wrap(response -> {
                if (response.isExists()) {
                    listener.onResponse(parseAccountDocument(response));
                } else {
                    listener.onResponse(null);
                }
            }, listener::onFailure));
        });
    }

    public void listAccounts(
        @Nullable String namespace,
        @Nullable String serviceName,
        ActionListener<List<ManagedServiceAccount>> listener
    ) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
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
                var query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE));
                if (namespace != null) {
                    query = query.filter(QueryBuilders.prefixQuery("username", namespace + "/"));
                }
                if (serviceName != null) {
                    query = query.filter(QueryBuilders.wildcardQuery("username", "*/" + serviceName));
                }
                final SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
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
                        contextSupplier,
                        ActionListener.wrap(accounts -> listener.onResponse(List.copyOf(accounts)), listener::onFailure)
                    ),
                    hit -> parseAccountDocument(hit.getSourceAsMap())
                );
            }
        });
    }

    public void putAccount(
        ServiceAccount.ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<PutResult> listener
    ) {
        if (false == clusterService.state().getMinTransportVersion().supports(MANAGED_SERVICE_ACCOUNTS)) {
            listener.onFailure(
                new IllegalStateException(
                    "managed service accounts require all nodes to be upgraded to a version that supports ["
                        + MANAGED_SERVICE_ACCOUNTS
                        + "]"
                )
            );
            return;
        }
        final ValidationException validationException = validatePutRequest(accountId, roles);
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        getByPrincipal(accountId.asPrincipal(), ActionListener.wrap(existing -> {
            final String generationId = existing == null ? UUIDs.randomBase64UUID() : existing.generationId();
            final PutResult.Type type = existing == null ? PutResult.Type.CREATED : PutResult.Type.UPDATED;
            try (XContentBuilder builder = newAccountDocument(accountId, roles, enabled, generationId)) {
                final IndexRequest indexRequest = client.prepareIndex(SECURITY_MAIN_ALIAS)
                    .setId(docIdForPrincipal(accountId.asPrincipal()))
                    .setSource(builder)
                    .setOpType(DocWriteRequest.OpType.INDEX)
                    .setRefreshPolicy(refreshPolicy)
                    .request();
                final BulkRequest bulkRequest = toSingleItemBulkRequest(indexRequest);
                securityIndex.forCurrentProject()
                    .prepareIndexIfNeededThenExecute(
                        listener::onFailure,
                        () -> executeAsyncWithOrigin(
                            client,
                            SECURITY_ORIGIN,
                            TransportBulkAction.TYPE,
                            bulkRequest,
                            TransportBulkAction.<IndexResponse>unwrappingSingleItemBulkResponse(ActionListener.wrap(response -> {
                                listener.onResponse(
                                    new PutResult(type, new ManagedServiceAccount(accountId, roles, enabled, generationId))
                                );
                            }, listener::onFailure))
                        )
                    );
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    public void deleteAccount(
        ServiceAccount.ServiceAccountId accountId,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<Boolean> listener
    ) {
        if (false == clusterService.state().getMinTransportVersion().supports(MANAGED_SERVICE_ACCOUNTS)) {
            listener.onFailure(
                new IllegalStateException(
                    "managed service accounts require all nodes to be upgraded to a version that supports ["
                        + MANAGED_SERVICE_ACCOUNTS
                        + "]"
                )
            );
            return;
        }
        final String principalError = ManagedServiceAccountIdValidator.validatePrincipal(accountId.asPrincipal());
        if (principalError != null) {
            listener.onFailure(new IllegalArgumentException(principalError));
            return;
        }
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(false);
            return;
        }
        if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
            return;
        }
        projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
            final DeleteRequest deleteRequest = client.prepareDelete(SECURITY_MAIN_ALIAS, docIdForPrincipal(accountId.asPrincipal()))
                .setRefreshPolicy(refreshPolicy)
                .request();
            executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                TransportDeleteAction.TYPE,
                deleteRequest,
                ActionListener.wrap(deleteResponse -> {
                    final boolean deleted = deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                    if (deleted) {
                        clearManagedTokenCache(accountId, ActionListener.wrap(ignore -> listener.onResponse(true), listener::onFailure));
                    } else {
                        listener.onResponse(false);
                    }
                }, listener::onFailure)
            );
        });
    }

    static String docIdForPrincipal(String principal) {
        return SERVICE_ACCOUNT_DOC_TYPE + "-" + principal;
    }

    static List<String> deduplicateRoles(List<String> roles) {
        return new ArrayList<>(new TreeSet<>(roles));
    }

    @Nullable
    private ValidationException validatePutRequest(ServiceAccount.ServiceAccountId accountId, List<String> roles) {
        ValidationException validationException = null;
        final String namespaceError = ManagedServiceAccountIdValidator.validateNamespace(accountId.namespace());
        if (namespaceError != null) {
            validationException = new ValidationException().addValidationError(namespaceError);
        }
        final String serviceNameError = ManagedServiceAccountIdValidator.validateServiceName(accountId.serviceName());
        if (serviceNameError != null) {
            if (validationException == null) {
                validationException = new ValidationException();
            }
            validationException.addValidationError(serviceNameError);
        }
        if (roles == null) {
            if (validationException == null) {
                validationException = new ValidationException();
            }
            validationException.addValidationError("roles is required");
        } else {
            for (String role : roles) {
                final Validation.Error roleNameError = NativeRealmValidationUtil.validateRoleName(role, true);
                if (roleNameError != null) {
                    if (validationException == null) {
                        validationException = new ValidationException();
                    }
                    validationException.addValidationError(roleNameError.toString());
                }
            }
        }
        return validationException;
    }

    private XContentBuilder newAccountDocument(
        ServiceAccount.ServiceAccountId accountId,
        List<String> roles,
        boolean enabled,
        String generationId
    ) throws IOException {
        final Version version = clusterService.state().nodes().getMinNodeVersion();
        final List<String> deduplicatedRoles = deduplicateRoles(roles);
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("doc_type", SERVICE_ACCOUNT_DOC_TYPE)
            .field("version", version.id)
            .field("username", accountId.asPrincipal())
            .field("roles", deduplicatedRoles)
            .field("enabled", enabled)
            .field(ACCOUNT_GENERATION_ID_FIELD, generationId)
            .endObject();
    }

    @Nullable
    private ManagedServiceAccount parseAccountDocument(GetResponse response) {
        return parseAccountDocument(response.getSource());
    }

    @Nullable
    private ManagedServiceAccount parseAccountDocument(Map<String, Object> source) {
        if (source == null) {
            return null;
        }
        final String docType = (String) source.get("doc_type");
        if (SERVICE_ACCOUNT_DOC_TYPE.equals(docType) == false) {
            logger.warn("malformed managed service account document with unexpected doc_type [{}]", docType);
            return null;
        }
        final String principal = (String) source.get("username");
        if (principal == null) {
            logger.warn("malformed managed service account document missing username");
            return null;
        }
        final String generationId = (String) source.get(ACCOUNT_GENERATION_ID_FIELD);
        if (generationId == null) {
            logger.warn("malformed managed service account document [{}] missing generation id", principal);
            return null;
        }
        @SuppressWarnings("unchecked")
        final List<String> roles = source.get("roles") instanceof Collection<?> collection
            ? collection.stream().map(Object::toString).collect(Collectors.toCollection(ArrayList::new))
            : List.of();
        final boolean enabled = source.get("enabled") instanceof Boolean b ? b : true;
        return new ManagedServiceAccount(ServiceAccount.ServiceAccountId.fromPrincipal(principal), roles, enabled, generationId);
    }

    private void clearManagedTokenCache(ServiceAccount.ServiceAccountId accountId, ActionListener<Void> listener) {
        final ClearSecurityCacheRequest clearSecurityCacheRequest = new ClearSecurityCacheRequest().cacheName("index_service_account_token")
            .keys(accountId.asPrincipal() + "/");
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            ClearSecurityCacheAction.INSTANCE,
            clearSecurityCacheRequest,
            ActionListener.wrap(response -> listener.onResponse(null), e -> {
                final String message = org.elasticsearch.core.Strings.format(
                    "clearing managed service account token cache for [%s] failed; please clear the cache manually",
                    accountId.asPrincipal()
                );
                logger.error(message, e);
                listener.onFailure(new ElasticsearchException(message, e));
            })
        );
    }

    public record PutResult(Type type, ManagedServiceAccount account) {
        public enum Type {
            CREATED,
            UPDATED
        }
    }
}
