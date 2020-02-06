/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor.DOC_TYPE_VALUE;
import static org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor.Fields.APPLICATION;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;

/**
 * {@code NativePrivilegeStore} is a store that reads/writes {@link ApplicationPrivilegeDescriptor} objects,
 * from an Elasticsearch index.
 */
public class NativePrivilegeStore {

    private static final Collector<Tuple<String, String>, ?, Map<String, List<String>>> TUPLES_TO_MAP = Collectors.toMap(
        Tuple::v1,
        t -> CollectionUtils.newSingletonArrayList(t.v2()), (a, b) -> {
            a.addAll(b);
            return a;
        });
    private static final Logger logger = LogManager.getLogger(NativePrivilegeStore.class);

    private final Settings settings;
    private final Client client;
    private final SecurityIndexManager securityIndexManager;

    public NativePrivilegeStore(Settings settings, Client client, SecurityIndexManager securityIndexManager) {
        this.settings = settings;
        this.client = client;
        this.securityIndexManager = securityIndexManager;
    }

    public void getPrivileges(Collection<String> applications, Collection<String> names,
                              ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndexManager.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else if (isSinglePrivilegeMatch(applications, names)) {
            getPrivilege(Objects.requireNonNull(Iterables.get(applications, 0)), Objects.requireNonNull(Iterables.get(names, 0)),
                ActionListener.wrap(privilege ->
                        listener.onResponse(privilege == null ? Collections.emptyList() : Collections.singletonList(privilege)),
                    listener::onFailure));
        } else {
            securityIndexManager.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final QueryBuilder query;
                final TermQueryBuilder typeQuery = QueryBuilders
                    .termQuery(ApplicationPrivilegeDescriptor.Fields.TYPE.getPreferredName(), DOC_TYPE_VALUE);
                if (isEmpty(applications) && isEmpty(names)) {
                    query = typeQuery;
                } else if (isEmpty(names)) {
                    query = QueryBuilders.boolQuery().filter(typeQuery).filter(getApplicationNameQuery(applications));
                } else if (isEmpty(applications)) {
                    query = QueryBuilders.boolQuery().filter(typeQuery)
                        .filter(getPrivilegeNameQuery(names));
                } else if (hasWildcard(applications)) {
                    query = QueryBuilders.boolQuery().filter(typeQuery)
                        .filter(getApplicationNameQuery(applications))
                        .filter(getPrivilegeNameQuery(names));
                } else {
                    final String[] docIds = applications.stream()
                        .flatMap(a -> names.stream().map(n -> toDocId(a, n)))
                        .toArray(String[]::new);
                    query = QueryBuilders.boolQuery().filter(typeQuery).filter(QueryBuilders.idsQuery().addIds(docIds));
                }
                final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                    SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                        .setQuery(query)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                    logger.trace(() ->
                        new ParameterizedMessage("Searching for privileges [{}] with query [{}]", names, Strings.toString(query)));
                    request.indicesOptions().ignoreUnavailable();
                    ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(supplier, listener),
                        hit -> buildPrivilege(hit.getId(), hit.getSourceRef()));
                }
            });
        }
    }

    private boolean isSinglePrivilegeMatch(Collection<String> applications, Collection<String> names) {
        return applications != null && applications.size() == 1 && hasWildcard(applications) == false && names != null && names.size() == 1;
    }

    private boolean hasWildcard(Collection<String> applications) {
        return applications.stream().anyMatch(n -> n.endsWith("*"));
    }

    private QueryBuilder getPrivilegeNameQuery(Collection<String> names) {
        return QueryBuilders.termsQuery(ApplicationPrivilegeDescriptor.Fields.NAME.getPreferredName(), names);
    }

    private QueryBuilder getApplicationNameQuery(Collection<String> applications) {
        if (applications.contains("*")) {
            return QueryBuilders.existsQuery(APPLICATION.getPreferredName());
        }
        final List<String> rawNames = new ArrayList<>(applications.size());
        final List<String> wildcardNames = new ArrayList<>(applications.size());
        for (String name : applications) {
            if (name.endsWith("*")) {
                wildcardNames.add(name);
            } else {
                rawNames.add(name);
            }
        }

        assert rawNames.isEmpty() == false || wildcardNames.isEmpty() == false;

        TermsQueryBuilder termsQuery = rawNames.isEmpty() ? null : QueryBuilders.termsQuery(APPLICATION.getPreferredName(), rawNames);
        if (wildcardNames.isEmpty()) {
            return termsQuery;
        }
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (termsQuery != null) {
            boolQuery.should(termsQuery);
        }
        for (String wildcard : wildcardNames) {
            final String prefix = wildcard.substring(0, wildcard.length() - 1);
            boolQuery.should(QueryBuilders.prefixQuery(APPLICATION.getPreferredName(), prefix));
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    private static boolean isEmpty(Collection<String> collection) {
        return collection == null || collection.isEmpty();
    }

    void getPrivilege(String application, String name, ActionListener<ApplicationPrivilegeDescriptor> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndexManager.freeze();
        if (frozenSecurityIndex.isAvailable() == false) {
            logger.warn(new ParameterizedMessage("failed to load privilege [{}] index not available", name),
                frozenSecurityIndex.getUnavailableReason());
            listener.onResponse(null);
        } else {
            securityIndexManager.checkIndexVersionThenExecute(listener::onFailure,
                () -> executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                    client.prepareGet(SECURITY_MAIN_ALIAS, toDocId(application, name))
                            .request(),
                    new ActionListener<GetResponse>() {
                        @Override
                        public void onResponse(GetResponse response) {
                            if (response.isExists()) {
                                listener.onResponse(buildPrivilege(response.getId(), response.getSourceAsBytesRef()));
                            } else {
                                listener.onResponse(null);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // if the index or the shard is not there / available we just claim the privilege is not there
                            if (TransportActions.isShardNotAvailableException(e)) {
                                logger.warn(new ParameterizedMessage("failed to load privilege [{}] index not available", name), e);
                                listener.onResponse(null);
                            } else {
                                logger.error(new ParameterizedMessage("failed to load privilege [{}]", name), e);
                                listener.onFailure(e);
                            }
                        }
                    },
                    client::get));
        }
    }

    public void putPrivileges(Collection<ApplicationPrivilegeDescriptor> privileges, WriteRequest.RefreshPolicy refreshPolicy,
                              ActionListener<Map<String, List<String>>> listener) {
        securityIndexManager.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            ActionListener<IndexResponse> groupListener = new GroupedActionListener<>(
                ActionListener.wrap((Collection<IndexResponse> responses) -> {
                    final Map<String, List<String>> createdNames = responses.stream()
                        .filter(r -> r.getResult() == DocWriteResponse.Result.CREATED)
                        .map(r -> r.getId())
                        .map(NativePrivilegeStore::nameFromDocId)
                        .collect(TUPLES_TO_MAP);
                    clearRolesCache(listener, createdNames);
                }, listener::onFailure), privileges.size());
            for (ApplicationPrivilegeDescriptor privilege : privileges) {
                innerPutPrivilege(privilege, refreshPolicy, groupListener);
            }
        });
    }

    private void innerPutPrivilege(ApplicationPrivilegeDescriptor privilege, WriteRequest.RefreshPolicy refreshPolicy,
                                   ActionListener<IndexResponse> listener) {
        try {
            final String name = privilege.getName();
            final XContentBuilder xContentBuilder = privilege.toXContent(jsonBuilder(), true);
            ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                client.prepareIndex(SECURITY_MAIN_ALIAS).setId(toDocId(privilege.getApplication(), name))
                    .setSource(xContentBuilder)
                    .setRefreshPolicy(refreshPolicy)
                    .request(), listener, client::index);
        } catch (Exception e) {
            logger.warn("Failed to put privilege {} - {}", Strings.toString(privilege), e.toString());
            listener.onFailure(e);
        }

    }

    public void deletePrivileges(String application, Collection<String> names, WriteRequest.RefreshPolicy refreshPolicy,
                                 ActionListener<Map<String, List<String>>> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndexManager.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyMap());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            securityIndexManager.checkIndexVersionThenExecute(listener::onFailure, () -> {
                ActionListener<DeleteResponse> groupListener = new GroupedActionListener<>(
                    ActionListener.wrap(responses -> {
                        final Map<String, List<String>> deletedNames = responses.stream()
                            .filter(r -> r.getResult() == DocWriteResponse.Result.DELETED)
                            .map(r -> r.getId())
                            .map(NativePrivilegeStore::nameFromDocId)
                            .collect(TUPLES_TO_MAP);
                        clearRolesCache(listener, deletedNames);
                    }, listener::onFailure), names.size());
                for (String name : names) {
                    ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                        client.prepareDelete(SECURITY_MAIN_ALIAS, toDocId(application, name))
                            .setRefreshPolicy(refreshPolicy)
                            .request(), groupListener, client::delete);
                }
            });
        }
    }

    private <T> void clearRolesCache(ActionListener<T> listener, T value) {
        // This currently clears _all_ roles, but could be improved to clear only those roles that reference the affected application
        ClearRolesCacheRequest request = new ClearRolesCacheRequest();
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearRolesCacheAction.INSTANCE, request,
            new ActionListener<>() {
                @Override
                public void onResponse(ClearRolesCacheResponse nodes) {
                    listener.onResponse(value);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("unable to clear role cache", e);
                    listener.onFailure(
                        new ElasticsearchException("clearing the role cache failed. please clear the role cache manually", e));
                }
            });
    }

    private ApplicationPrivilegeDescriptor buildPrivilege(String docId, BytesReference source) {
        logger.trace("Building privilege from [{}] [{}]", docId, source == null ? "<<null>>" : source.utf8ToString());
        if (source == null) {
            return null;
        }
        final Tuple<String, String> name = nameFromDocId(docId);
        try {
            // EMPTY is safe here because we never use namedObject

            try (StreamInput input = source.streamInput();
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, input)) {
                final ApplicationPrivilegeDescriptor privilege = ApplicationPrivilegeDescriptor.parse(parser, null, null, true);
                assert privilege.getApplication().equals(name.v1())
                    : "Incorrect application name for privilege. Expected [" + name.v1() + "] but was " + privilege.getApplication();
                assert privilege.getName().equals(name.v2())
                    : "Incorrect name for application privilege. Expected [" + name.v2() + "] but was " + privilege.getName();
                return privilege;
            }
        } catch (IOException | XContentParseException e) {
            logger.error(new ParameterizedMessage("cannot parse application privilege [{}]", name), e);
            return null;
        }
    }

    private static Tuple<String, String> nameFromDocId(String docId) {
        final String name = docId.substring(DOC_TYPE_VALUE.length() + 1);
        assert name != null && name.length() > 0 : "Invalid name '" + name + "'";
        final int colon = name.indexOf(':');
        assert colon > 0 : "Invalid name '" + name + "' (missing colon)";
        return new Tuple<>(name.substring(0, colon), name.substring(colon + 1));
    }

    private static String toDocId(String application, String name) {
        return DOC_TYPE_VALUE + "_" + application + ":" + name;
    }

}
