/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;

class IndicesAndAliasesResolver {

    //`*,-*` what we replace indices and aliases with if we need Elasticsearch to return empty responses without throwing exception
    static final String[] NO_INDICES_OR_ALIASES_ARRAY = new String[] { "*", "-*" };
    static final List<String> NO_INDICES_OR_ALIASES_LIST = Arrays.asList(NO_INDICES_OR_ALIASES_ARRAY);

    private final IndexNameExpressionResolver nameExpressionResolver;
    private final RemoteClusterResolver remoteClusterResolver;

    IndicesAndAliasesResolver(Settings settings, ClusterService clusterService, IndexNameExpressionResolver resolver) {
        this.nameExpressionResolver = resolver;
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

    ResolvedIndices resolve(TransportRequest request, Metadata metadata, List<String> authorizedIndices) {
        if (request instanceof IndicesAliasesRequest) {
            ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
            IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;
            for (IndicesRequest indicesRequest : indicesAliasesRequest.getAliasActions()) {
                final ResolvedIndices resolved = resolveIndicesAndAliases(indicesRequest, metadata, authorizedIndices);
                resolvedIndicesBuilder.addLocal(resolved.getLocal());
                resolvedIndicesBuilder.addRemote(resolved.getRemote());
            }
            return resolvedIndicesBuilder.build();
        }

        // if for some reason we are missing an action... just for safety we'll reject
        if (request instanceof IndicesRequest == false) {
            throw new IllegalStateException("Request [" + request + "] is not an Indices request, but should be.");
        }
        return resolveIndicesAndAliases((IndicesRequest) request, metadata, authorizedIndices);
    }


    ResolvedIndices resolveIndicesAndAliases(IndicesRequest indicesRequest, Metadata metadata, List<String> authorizedIndices) {
        final ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
        boolean indicesReplacedWithNoIndices = false;
        if (indicesRequest instanceof PutMappingRequest && ((PutMappingRequest) indicesRequest).getConcreteIndex() != null) {
            /*
             * This is a special case since PutMappingRequests from dynamic mapping updates have a concrete index
             * if this index is set and it's in the list of authorized indices we are good and don't need to put
             * the list of indices in there, if we do so it will result in an invalid request and the update will fail.
             */
            assert indicesRequest.indices() == null || indicesRequest.indices().length == 0
                    : "indices are: " + Arrays.toString(indicesRequest.indices()); // Arrays.toString() can handle null values - all good
            resolvedIndicesBuilder.addLocal(getPutMappingIndexOrAlias((PutMappingRequest) indicesRequest, authorizedIndices, metadata));
        } else if (indicesRequest instanceof IndicesRequest.Replaceable) {
            final IndicesRequest.Replaceable replaceable = (IndicesRequest.Replaceable) indicesRequest;
            final IndicesOptions indicesOptions = indicesRequest.indicesOptions();
            final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();

            // check for all and return list of authorized indices
            if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
                if (replaceWildcards) {
                    for (String authorizedIndex : authorizedIndices) {
                        if (isIndexVisible("*", authorizedIndex, indicesOptions, metadata)) {
                            resolvedIndicesBuilder.addLocal(authorizedIndex);
                        }
                    }
                }
                // if we cannot replace wildcards the indices list stays empty. Same if there are no authorized indices.
                // we honour allow_no_indices like es core does.
            } else {
                final ResolvedIndices split;
                if (allowsRemoteIndices(indicesRequest)) {
                    split = remoteClusterResolver.splitLocalAndRemoteIndexNames(indicesRequest.indices());
                } else {
                    split = new ResolvedIndices(Arrays.asList(indicesRequest.indices()), Collections.emptyList());
                }
                List<String> replaced = replaceWildcardsWithAuthorizedIndices(split.getLocal(), indicesOptions, metadata,
                        authorizedIndices, replaceWildcards);
                if (indicesOptions.ignoreUnavailable()) {
                    //out of all the explicit names (expanded from wildcards and original ones that were left untouched)
                    //remove all the ones that the current user is not authorized for and ignore them
                    replaced = replaced.stream().filter(authorizedIndices::contains).collect(Collectors.toList());
                }
                resolvedIndicesBuilder.addLocal(replaced);
                resolvedIndicesBuilder.addRemote(split.getRemote());
            }

            if (resolvedIndicesBuilder.isEmpty()) {
                if (indicesOptions.allowNoIndices()) {
                    //this is how we tell es core to return an empty response, we can let the request through being sure
                    //that the '-*' wildcard expression will be resolved to no indices. We can't let empty indices through
                    //as that would be resolved to _all by es core.
                    replaceable.indices(NO_INDICES_OR_ALIASES_ARRAY);
                    indicesReplacedWithNoIndices = true;
                    resolvedIndicesBuilder.addLocal(NO_INDEX_PLACEHOLDER);
                } else {
                    throw new IndexNotFoundException(Arrays.toString(indicesRequest.indices()));
                }
            } else {
                replaceable.indices(resolvedIndicesBuilder.build().toArray());
            }
        } else {
            if (containsWildcards(indicesRequest)) {
                throw new IllegalStateException("There are no external requests known to support wildcards that don't support replacing " +
                        "their indices");
            }
            //NOTE: shard level requests do support wildcards (as they hold the original indices options) but don't support
            // replacing their indices.
            //That is fine though because they never contain wildcards, as they get replaced as part of the authorization of their
            //corresponding parent request on the coordinating node. Hence wildcards don't need to get replaced nor exploded for
            // shard level requests.
            for (String name : indicesRequest.indices()) {
                resolvedIndicesBuilder.addLocal(nameExpressionResolver.resolveDateMathExpression(name));
            }
        }

        if (indicesRequest instanceof AliasesRequest) {
            //special treatment for AliasesRequest since we need to replace wildcards among the specified aliases too.
            //AliasesRequest extends IndicesRequest.Replaceable, hence its indices have already been properly replaced.
            AliasesRequest aliasesRequest = (AliasesRequest) indicesRequest;
            if (aliasesRequest.expandAliasesWildcards()) {
                List<String> aliases = replaceWildcardsWithAuthorizedAliases(aliasesRequest.aliases(),
                        loadAuthorizedAliases(authorizedIndices, metadata));
                aliasesRequest.replaceAliases(aliases.toArray(new String[aliases.size()]));
            }
            if (indicesReplacedWithNoIndices) {
                if (indicesRequest instanceof GetAliasesRequest == false) {
                    throw new IllegalStateException(GetAliasesRequest.class.getSimpleName() + " is the only known " +
                            "request implementing " + AliasesRequest.class.getSimpleName() + " that may allow no indices. Found [" +
                            indicesRequest.getClass().getName() + "] which ended up with an empty set of indices.");
                }
                //if we replaced the indices with '-*' we shouldn't be adding the aliases to the list otherwise the request will
                //not get authorized. Leave only '-*' and ignore the rest, result will anyway be empty.
            } else {
                resolvedIndicesBuilder.addLocal(aliasesRequest.aliases());
            }
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
        }
        return resolvedIndicesBuilder.build();
    }

    /**
     * Special handling of the value to authorize for a put mapping request. Dynamic put mapping
     * requests use a concrete index, but we allow permissions to be defined on aliases so if the
     * request's concrete index is not in the list of authorized indices, then we need to look to
     * see if this can be authorized against an alias
     */
    static String getPutMappingIndexOrAlias(PutMappingRequest request, List<String> authorizedIndicesList, Metadata metadata) {
        final String concreteIndexName = request.getConcreteIndex().getName();

        // validate that the concrete index exists, otherwise there is no remapping that we could do
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(concreteIndexName);
        final String resolvedAliasOrIndex;
        if (indexAbstraction == null) {
            resolvedAliasOrIndex = concreteIndexName;
        } else if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalStateException("concrete index [" + concreteIndexName + "] is a [" +
                indexAbstraction.getType().getDisplayName() + "], but a concrete index is expected");
        } else if (authorizedIndicesList.contains(concreteIndexName)) {
            // user is authorized to put mappings for this index
            resolvedAliasOrIndex = concreteIndexName;
        } else {
            // the user is not authorized to put mappings for this index, but could have been
            // authorized for a write using an alias that triggered a dynamic mapping update
            ImmutableOpenMap<String, List<AliasMetadata>> foundAliases = metadata.findAllAliases(new String[] { concreteIndexName });
            List<AliasMetadata> aliasMetadata = foundAliases.get(concreteIndexName);
            if (aliasMetadata != null) {
                Optional<String> foundAlias = aliasMetadata.stream()
                    .map(AliasMetadata::alias)
                    .filter(authorizedIndicesList::contains)
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
                resolvedAliasOrIndex = foundAlias.orElse(concreteIndexName);
            } else {
                resolvedAliasOrIndex = concreteIndexName;
            }
        }

        return resolvedAliasOrIndex;
    }

    static boolean allowsRemoteIndices(IndicesRequest request) {
        return request instanceof SearchRequest || request instanceof FieldCapabilitiesRequest
                || request instanceof GraphExploreRequest;
    }

    private List<String> loadAuthorizedAliases(List<String> authorizedIndices, Metadata metadata) {
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

    //TODO Investigate reusing code from vanilla es to resolve index names and wildcards
    private List<String> replaceWildcardsWithAuthorizedIndices(Iterable<String> indices, IndicesOptions indicesOptions, Metadata metadata,
                                                               List<String> authorizedIndices, boolean replaceWildcards) {
        //the order matters when it comes to exclusions
        List<String> finalIndices = new ArrayList<>();
        boolean wildcardSeen = false;
        for (String index : indices) {
            String aliasOrIndex;
            boolean minus = false;
            if (index.charAt(0) == '-' && wildcardSeen) {
                aliasOrIndex = index.substring(1);
                minus = true;
            } else {
                aliasOrIndex = index;
            }

            // we always need to check for date math expressions
            final String dateMathName = nameExpressionResolver.resolveDateMathExpression(aliasOrIndex);
            if (dateMathName != aliasOrIndex) {
                assert dateMathName.equals(aliasOrIndex) == false;
                if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                    // continue
                    aliasOrIndex = dateMathName;
                } else if (authorizedIndices.contains(dateMathName) &&
                    isIndexVisible(aliasOrIndex, dateMathName, indicesOptions, metadata, true)) {
                    if (minus) {
                        finalIndices.remove(dateMathName);
                    } else {
                        finalIndices.add(dateMathName);
                    }
                } else {
                    if (indicesOptions.ignoreUnavailable() == false) {
                        throw new IndexNotFoundException(dateMathName);
                    }
                }
            }

            if (replaceWildcards && Regex.isSimpleMatchPattern(aliasOrIndex)) {
                wildcardSeen = true;
                Set<String> resolvedIndices = new HashSet<>();
                for (String authorizedIndex : authorizedIndices) {
                    if (Regex.simpleMatch(aliasOrIndex, authorizedIndex) &&
                        isIndexVisible(aliasOrIndex, authorizedIndex, indicesOptions, metadata)) {
                        resolvedIndices.add(authorizedIndex);
                    }
                }
                if (resolvedIndices.isEmpty()) {
                    //es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                    if (indicesOptions.allowNoIndices() == false) {
                        throw new IndexNotFoundException(aliasOrIndex);
                    }
                } else {
                    if (minus) {
                        finalIndices.removeAll(resolvedIndices);
                    } else {
                        finalIndices.addAll(resolvedIndices);
                    }
                }
            } else if (dateMathName == aliasOrIndex) {
                // we can use == here to compare strings since the name expression resolver returns the same instance, but add an assert
                // to ensure we catch this if it changes

                assert dateMathName.equals(aliasOrIndex);
                //Metadata#convertFromWildcards checks if the index exists here and throws IndexNotFoundException if not (based on
                // ignore_unavailable). We only add/remove the index: if the index is missing or the current user is not authorized
                // to access it either an AuthorizationException will be thrown later in AuthorizationService, or the index will be
                // removed from the list, based on the ignore_unavailable option.
                if (minus) {
                    finalIndices.remove(aliasOrIndex);
                } else {
                    finalIndices.add(aliasOrIndex);
                }
            }
        }
        return finalIndices;
    }

    private static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata) {
        return isIndexVisible(expression, index, indicesOptions, metadata, false);
    }

    private static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata,
                                          boolean dateMathExpression) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
        final boolean isHidden = indexAbstraction.isHidden();
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            //it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            //complicated to support those options with aliases pointing to multiple indices...
            //TODO investigate supporting expandWildcards option for aliases too, like es core does.
            if (indicesOptions.ignoreAliases()) {
                return false;
            } else if (isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index)) {
                return true;
            } else {
                return false;
            }
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
            // If indicesOptions.includeDataStreams() returns false then we fail later in IndexNameExpressionResolver.
            if (isHidden == false || indicesOptions.expandWildcardsHidden()) {
                return true;
            } else {
                return false;
            }
        }
        assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
        IndexMetadata indexMetadata = indexAbstraction.getIndices().get(0);
        if (isHidden && indicesOptions.expandWildcardsHidden() == false && isVisibleDueToImplicitHidden(expression, index) == false) {
            return false;
        }

        // the index is not hidden and since it is a date math expression, we consider it visible regardless of open/closed
        if (dateMathExpression) {
            assert State.values().length == 2 : "a new IndexMetadata.State value may need to be handled!";
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
            return true;
        }
        return false;
    }

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
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
