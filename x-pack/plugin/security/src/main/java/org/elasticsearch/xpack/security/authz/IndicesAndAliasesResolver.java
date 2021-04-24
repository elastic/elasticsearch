/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.AsyncSupplier;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;

class IndicesAndAliasesResolver {

    //`*,-*` what we replace indices and aliases with if we need Elasticsearch to return empty responses without throwing exception
    static final String[] NO_INDICES_OR_ALIASES_ARRAY = new String[] { "*", "-*" };
    static final List<String> NO_INDICES_OR_ALIASES_LIST = Arrays.asList(NO_INDICES_OR_ALIASES_ARRAY);

    private final IndexNameExpressionResolver nameExpressionResolver;
    private final IndexAbstractionResolver indexAbstractionResolver;
    private final RemoteClusterResolver remoteClusterResolver;

    IndicesAndAliasesResolver(Settings settings, ClusterService clusterService, IndexNameExpressionResolver resolver) {
        this.nameExpressionResolver = resolver;
        this.indexAbstractionResolver = new IndexAbstractionResolver(resolver);
        this.remoteClusterResolver = new RemoteClusterResolver(settings, clusterService.getClusterSettings());
    }

    /**
     * Resolves, and if necessary updates, the list of index names in the provided <code>request</code> in accordance with the user's
     * <code>authorizedIndices</code>.
     * <p>
     * Wildcards are expanded at this phase to ensure that all security and execution decisions are made against a fixed set of index names
     * that is consistent and does not change during the life of the request.
     * </p>
     * <p>
     * If the provided <code>request</code> is of a type that {@link #allowsRemoteIndices(IndicesRequest) allows remote indices},
     * then the index names will be categorized into those that refer to {@link ResolvedIndices#getLocal() local indices}, and those that
     * refer to {@link ResolvedIndices#getRemote() remote indices}. This categorization follows the standard
     * {@link RemoteClusterAware#buildRemoteIndexName(String, String) remote index-name format} and also respects the currently defined
     * remote clusters}.
     * </p><br>
     * Thus an index name <em>N</em> will considered to be <em>remote</em> if-and-only-if all of the following are true
     * <ul>
     * <li><code>request</code> supports remote indices</li>
     * <li>
     * <em>N</em> is in the format <i>cluster</i><code>:</code><i>index</i>.
     * It is allowable for <i>cluster</i> and <i>index</i> to contain wildcards, but the separator (<code>:</code>) must be explicit.
     * </li>
     * <li><i>cluster</i> matches one or more remote cluster names that are registered within this cluster.</li>
     * </ul>
     * In which case, any wildcards in the <i>cluster</i> portion of the name will be expanded and the resulting remote-index-name(s) will
     * be added to the <em>remote</em> index list.
     * <br>
     * Otherwise, <em>N</em> will be added to the <em>local</em> index list.
     */

    void resolve(TransportRequest request, Metadata metadata, AsyncSupplier<List<String>> authorizedIndicesSupplier,
                 ActionListener<ResolvedIndices> listener) {
        if (request instanceof IndicesAliasesRequest) {
            IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;
            GroupedActionListener<ResolvedIndices> groupedListener = new GroupedActionListener<>(listener.map(allResolvedIndices -> {
                ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
                for (ResolvedIndices resolvedIndices : allResolvedIndices) {
                    resolvedIndicesBuilder.add(resolvedIndices);
                }
                return resolvedIndicesBuilder.build();
            }), indicesAliasesRequest.getAliasActions().size());
            for (IndicesRequest indicesRequest : indicesAliasesRequest.getAliasActions()) {
                resolveIndicesAndAliases(indicesRequest, metadata, authorizedIndicesSupplier, groupedListener);
            }
        } else {
            // if for some reason we are missing an action... just for safety we'll reject
            if (request instanceof IndicesRequest == false) {
                listener.onFailure(new IllegalStateException("Request [" + request + "] is not an Indices request, but should be."));
            } else {
                resolveIndicesAndAliases((IndicesRequest) request, metadata, authorizedIndicesSupplier, listener);
            }
        }
    }

    ResolvedIndices resolve(TransportRequest request, Metadata metadata, List<String> authorizedIndices) {
        PlainActionFuture<ResolvedIndices> future = PlainActionFuture.newFuture();
        // if the supplier is not async, the method is not async, hence get is non-blocking
        resolve(request, metadata, listener -> listener.onResponse(authorizedIndices), future);
        return FutureUtils.get(future, 0, TimeUnit.MILLISECONDS);
    }

    void resolveIndicesAndAliases(IndicesRequest indicesRequest, Metadata metadata, AsyncSupplier<List<String>> authorizedIndicesSupplier
            , ActionListener<ResolvedIndices> listener) {
        if (indicesRequest instanceof PutMappingRequest && ((PutMappingRequest) indicesRequest).getConcreteIndex() != null) {
            /*
             * This is a special case since PutMappingRequests from dynamic mapping updates have a concrete index
             * if this index is set and it's in the list of authorized indices we are good and don't need to put
             * the list of indices in there, if we do so it will result in an invalid request and the update will fail.
             */
            assert indicesRequest.indices() == null || indicesRequest.indices().length == 0
                    : "indices are: " + Arrays.toString(indicesRequest.indices()); // Arrays.toString() can handle null values - all good
            getPutMappingIndexOrAlias((PutMappingRequest) indicesRequest, authorizedIndicesSupplier, metadata,
                    ActionListener.wrap(resolvedIndexOrAlias -> {
                        ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
                        resolvedIndicesBuilder.addLocal(resolvedIndexOrAlias);
                        listener.onResponse(resolvedIndicesBuilder.build());
                    }, listener::onFailure));
        } else if (indicesRequest instanceof IndicesRequest.Replaceable) {
            final AtomicBoolean indicesReplacedWithNoIndices = new AtomicBoolean(false);
            final StepListener<ResolvedIndices.Builder> resolvedIndicesStepListener = new StepListener<>();
            final IndicesRequest.Replaceable replaceable = (IndicesRequest.Replaceable) indicesRequest;
            final IndicesOptions indicesOptions = indicesRequest.indicesOptions();
            final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();

            // check for all and return list of authorized indices
            if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
                if (replaceWildcards) {
                    authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
                        ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
                        for (String authorizedIndex : authorizedIndices) {
                            if (IndexAbstractionResolver.isIndexVisible("*", authorizedIndex, indicesOptions, metadata,
                                    indicesRequest.includeDataStreams())) {
                                resolvedIndicesBuilder.addLocal(authorizedIndex);
                            }
                        }
                        resolvedIndicesStepListener.onResponse(resolvedIndicesBuilder);
                    }, listener::onFailure));
                } else {
                    // if we cannot replace wildcards the indices list stays empty. Same if there are no authorized indices.
                    // we honour allow_no_indices like es core does.
                    resolvedIndicesStepListener.onResponse(new ResolvedIndices.Builder());
                }
            } else {
                final ResolvedIndices split;
                if (allowsRemoteIndices(indicesRequest)) {
                    split = remoteClusterResolver.splitLocalAndRemoteIndexNames(indicesRequest.indices());
                } else {
                    split = new ResolvedIndices(Arrays.asList(indicesRequest.indices()), Collections.emptyList());
                }
                authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
                    ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
                    // TODO pass the authorizedIndicesSupplier inside the index abstraction resolver
                    List<String> replaced = indexAbstractionResolver.resolveIndexAbstractions(split.getLocal(), indicesOptions, metadata,
                            authorizedIndices, replaceWildcards, indicesRequest.includeDataStreams());
                    if (indicesOptions.ignoreUnavailable()) {
                        //out of all the explicit names (expanded from wildcards and original ones that were left untouched)
                        //remove all the ones that the current user is not authorized for and ignore them
                        replaced = replaced.stream().filter(authorizedIndices::contains).collect(Collectors.toList());
                    }
                    resolvedIndicesBuilder.addLocal(replaced);
                    resolvedIndicesBuilder.addRemote(split.getRemote());
                    resolvedIndicesStepListener.onResponse(resolvedIndicesBuilder);
                }, listener::onFailure));
            }

            final StepListener<Void> indicesReplacedStepListener = new StepListener<>();
            resolvedIndicesStepListener.whenComplete(resolvedIndicesBuilder -> {
                if (resolvedIndicesBuilder.isEmpty()) {
                    if (indicesOptions.allowNoIndices()) {
                        //this is how we tell es core to return an empty response, we can let the request through being sure
                        //that the '-*' wildcard expression will be resolved to no indices. We can't let empty indices through
                        //as that would be resolved to _all by es core.
                        replaceable.indices(NO_INDICES_OR_ALIASES_ARRAY);
                        indicesReplacedWithNoIndices.set(true);
                        resolvedIndicesBuilder.addLocal(NO_INDEX_PLACEHOLDER);
                        indicesReplacedStepListener.onResponse(null);
                    } else {
                        listener.onFailure(new IndexNotFoundException(Arrays.toString(indicesRequest.indices())));
                    }
                } else {
                    replaceable.indices(resolvedIndicesBuilder.build().toArray());
                    indicesReplacedStepListener.onResponse(null);
                }
            }, listener::onFailure);

            indicesReplacedStepListener.whenComplete(aVoid -> {
                if (indicesRequest instanceof AliasesRequest) {
                    //special treatment for AliasesRequest since we need to replace wildcards among the specified aliases too.
                    //AliasesRequest extends IndicesRequest.Replaceable, hence its indices have already been properly replaced.
                    AliasesRequest aliasesRequest = (AliasesRequest) indicesRequest;
                    final StepListener<Void> aliasesReplacedStepListener = new StepListener<>();
                    if (aliasesRequest.expandAliasesWildcards()) {
                        authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
                            List<String> aliases = replaceWildcardsWithAuthorizedAliases(aliasesRequest.aliases(),
                                    loadAuthorizedAliases(authorizedIndices, metadata));
                            aliasesRequest.replaceAliases(aliases.toArray(new String[aliases.size()]));
                            aliasesReplacedStepListener.onResponse(null);
                        }, listener::onFailure));
                    } else {
                        aliasesReplacedStepListener.onResponse(null);
                    }
                    aliasesReplacedStepListener.whenComplete(anotherVoid -> {
                        if (indicesReplacedWithNoIndices.get()) {
                            if (indicesRequest instanceof GetAliasesRequest == false) {
                                listener.onFailure(new IllegalStateException(GetAliasesRequest.class.getSimpleName() + " is the only " +
                                        "known request implementing " + AliasesRequest.class.getSimpleName() +
                                        " that may allow no indices. Found [" + indicesRequest.getClass().getName() +
                                        "] which ended up with an empty set of indices."));
                            }
                            //if we replaced the indices with '-*' we shouldn't be adding the aliases to the list otherwise the request will
                            //not get authorized. Leave only '-*' and ignore the rest, result will anyway be empty.
                        } else {
                            resolvedIndicesStepListener.whenComplete(resolvedIndicesBuilder -> {
                                resolvedIndicesBuilder.addLocal(aliasesRequest.aliases());
                                /*
                                 * If no aliases are authorized, then fill in an expression that Metadata#findAliases evaluates to an
                                 * empty alias list. We can not put an empty list here because core resolves this as _all. For other
                                 * request types, this replacement is not needed and can trigger issues when we rewrite the request
                                 * on the coordinating node. For example, for a remove index request, if we did this replacement,
                                 * the request would be rewritten to include "*","-*" and for a user that does not have permissions
                                 * on "*", the master node would not authorize the request.
                                 */
                                if (aliasesRequest.expandAliasesWildcards() && aliasesRequest.aliases().length == 0) {
                                    aliasesRequest.replaceAliases(NO_INDICES_OR_ALIASES_ARRAY);
                                }
                                listener.onResponse(resolvedIndicesBuilder.build());
                            }, listener::onFailure);
                        }
                    }, listener::onFailure);
                } else {
                    resolvedIndicesStepListener.whenComplete(resolvedIndicesBuilder -> {
                        listener.onResponse(resolvedIndicesBuilder.build());
                    }, listener::onFailure);
                }
            }, listener::onFailure);
        } else {
            final ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
            if (containsWildcards(indicesRequest)) {
                listener.onFailure(new IllegalStateException("There are no external requests known to support wildcards that don't " +
                        "support replacing their indices"));
            } else {
                //NOTE: shard level requests do support wildcards (as they hold the original indices options) but don't support
                // replacing their indices.
                //That is fine though because they never contain wildcards, as they get replaced as part of the authorization of their
                //corresponding parent request on the coordinating node. Hence wildcards don't need to get replaced nor exploded for
                // shard level requests.
                for (String name : indicesRequest.indices()) {
                    resolvedIndicesBuilder.addLocal(nameExpressionResolver.resolveDateMathExpression(name));
                }
                listener.onResponse(resolvedIndicesBuilder.build());
            }
        }
    }

    ResolvedIndices resolveIndicesAndAliases(IndicesRequest indicesRequest, Metadata metadata,
                                             List<String> authorizedIndices) {
        PlainActionFuture<ResolvedIndices> future = PlainActionFuture.newFuture();
        // if the supplier is not async, the method is not async, hence get is non-blocking
        resolveIndicesAndAliases(indicesRequest, metadata, listener -> listener.onResponse(authorizedIndices), future);
        return FutureUtils.get(future, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Special handling of the value to authorize for a put mapping request. Dynamic put mapping
     * requests use a concrete index, but we allow permissions to be defined on aliases so if the
     * request's concrete index is not in the list of authorized indices, then we need to look to
     * see if this can be authorized against an alias
     */
    static void getPutMappingIndexOrAlias(PutMappingRequest request, AsyncSupplier<List<String>> authorizedIndicesSupplier,
                                          Metadata metadata, ActionListener<String> listener) {
        final String concreteIndexName = request.getConcreteIndex().getName();
        // validate that the concrete index exists, otherwise there is no remapping that we could do
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(concreteIndexName);
        if (indexAbstraction == null) {
            listener.onResponse(concreteIndexName);
        } else if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            listener.onFailure(new IllegalStateException("concrete index [" + concreteIndexName + "] is a [" +
                    indexAbstraction.getType().getDisplayName() + "], but a concrete index is expected"));
        } else {
            authorizedIndicesSupplier.getAsync(ActionListener.wrap(authorizedIndices -> {
                if (authorizedIndices.contains(concreteIndexName)) {
                    // user is authorized to put mappings for this index
                    listener.onResponse(concreteIndexName);
                } else {
                    // the user is not authorized to put mappings for this index, but could have been
                    // authorized for a write using an alias that triggered a dynamic mapping update
                    ImmutableOpenMap<String, List<AliasMetadata>> foundAliases = metadata.findAllAliases(new String[]{concreteIndexName});
                    List<AliasMetadata> aliasMetadata = foundAliases.get(concreteIndexName);
                    if (aliasMetadata != null) {
                        Optional<String> foundAlias = aliasMetadata.stream()
                                .map(AliasMetadata::alias)
                                .filter(authorizedIndices::contains)
                                .filter(aliasName -> {
                                    IndexAbstraction alias = metadata.getIndicesLookup().get(aliasName);
                                    List<IndexMetadata> indexMetadata = alias.getIndices();
                                    if (indexMetadata.size() == 1) {
                                        return true;
                                    } else {
                                        assert alias.getType() == IndexAbstraction.Type.ALIAS;
                                        IndexMetadata idxMeta = alias.getWriteIndex();
                                        return idxMeta != null && idxMeta.getIndex().getName().equals(concreteIndexName);
                                    }
                                })
                                .findFirst();
                        listener.onResponse(foundAlias.orElse(concreteIndexName));
                    } else {
                        listener.onResponse(concreteIndexName);
                    }
                }
            }, listener::onFailure));
        }
    }

    static String getPutMappingIndexOrAlias(PutMappingRequest request, List<String> authorizedIndicesList, Metadata metadata) {
        PlainActionFuture<String> future = PlainActionFuture.newFuture();
        // if the supplier is not async, the method is not async, hence get is non-blocking
        getPutMappingIndexOrAlias(request,
                listener -> listener.onResponse(authorizedIndicesList),
                metadata,
                future);
        return FutureUtils.get(future, 0, TimeUnit.MILLISECONDS);
    }

    static boolean allowsRemoteIndices(IndicesRequest request) {
        return request instanceof SearchRequest || request instanceof FieldCapabilitiesRequest
                || request instanceof GraphExploreRequest || request instanceof ResolveIndexAction.Request
                || request instanceof OpenPointInTimeRequest;
    }

    private List<String> loadAuthorizedAliases(Collection<String> authorizedIndices, Metadata metadata) {
        List<String> authorizedAliases = new ArrayList<>();
        SortedMap<String, IndexAbstraction> existingAliases = metadata.getIndicesLookup();
        for (String authorizedIndex : authorizedIndices) {
            IndexAbstraction indexAbstraction = existingAliases.get(authorizedIndex);
            if (indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                authorizedAliases.add(authorizedIndex);
            }
        }
        return authorizedAliases;
    }

    private List<String> replaceWildcardsWithAuthorizedAliases(String[] aliases, List<String> authorizedAliases) {
        final List<String> finalAliases = new ArrayList<>();

        // IndicesAliasesRequest doesn't support empty aliases (validation fails) but
        // GetAliasesRequest does (in which case empty means _all)
        if (aliases.length == 0) {
            finalAliases.addAll(authorizedAliases);
        }

        for (String aliasExpression : aliases) {
            boolean include = true;
            if (aliasExpression.charAt(0) == '-') {
                include = false;
                aliasExpression = aliasExpression.substring(1);
            }
            if (Metadata.ALL.equals(aliasExpression) || Regex.isSimpleMatchPattern(aliasExpression)) {
                final Set<String> resolvedAliases = new HashSet<>();
                for (final String authorizedAlias : authorizedAliases) {
                    if (Metadata.ALL.equals(aliasExpression) || Regex.simpleMatch(aliasExpression, authorizedAlias)) {
                        resolvedAliases.add(authorizedAlias);
                    }
                }
                if (include) {
                    finalAliases.addAll(resolvedAliases);
                } else {
                    finalAliases.removeAll(resolvedAliases);
                }
            } else if (include) {
                finalAliases.add(aliasExpression);
            } else {
                finalAliases.remove(aliasExpression);
            }
        }

        return finalAliases;
    }

    private boolean containsWildcards(IndicesRequest indicesRequest) {
        if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
            return true;
        }
        for (String index : indicesRequest.indices()) {
            if (Regex.isSimpleMatchPattern(index)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> indicesList(String[] list) {
        return (list == null) ? null : Arrays.asList(list);
    }

    private static class RemoteClusterResolver extends RemoteClusterAware {

        private final CopyOnWriteArraySet<String> clusters;

        private RemoteClusterResolver(Settings settings, ClusterSettings clusterSettings) {
            super(settings);
            clusters = new CopyOnWriteArraySet<>(getEnabledRemoteClusters(settings));
            listenForUpdates(clusterSettings);
        }

        @Override
        protected void updateRemoteCluster(String clusterAlias, Settings settings) {
            if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings)) {
                clusters.add(clusterAlias);
            } else {
                clusters.remove(clusterAlias);
            }
        }

        ResolvedIndices splitLocalAndRemoteIndexNames(String... indices) {
            final Map<String, List<String>> map = super.groupClusterIndices(clusters, indices);
            final List<String> local = map.remove(LOCAL_CLUSTER_GROUP_KEY);
            final List<String> remote = map.entrySet().stream()
                    .flatMap(e -> e.getValue().stream().map(v -> e.getKey() + REMOTE_CLUSTER_INDEX_SEPARATOR + v))
                    .collect(Collectors.toList());
            return new ResolvedIndices(local == null ? Collections.emptyList() : local, remote);
        }
    }

}
