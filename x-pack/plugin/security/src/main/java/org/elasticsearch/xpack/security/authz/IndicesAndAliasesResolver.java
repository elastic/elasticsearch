/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
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
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;

class IndicesAndAliasesResolver {

    // `*,-*` what we replace indices and aliases with if we need Elasticsearch to return empty responses without throwing exception
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
     * If the provided <code>request</code> is of a type that
     * {@link IndicesRequest.Replaceable#allowsRemoteIndices() allows remote indices},
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

    ResolvedIndices resolve(String action, TransportRequest request, Metadata metadata, Set<String> authorizedIndices) {
        if (request instanceof IndicesAliasesRequest indicesAliasesRequest) {
            ResolvedIndices.Builder resolvedIndicesBuilder = new ResolvedIndices.Builder();
            for (IndicesRequest indicesRequest : indicesAliasesRequest.getAliasActions()) {
                final ResolvedIndices resolved = resolveIndicesAndAliases(action, indicesRequest, metadata, authorizedIndices);
                resolvedIndicesBuilder.addLocal(resolved.getLocal());
                resolvedIndicesBuilder.addRemote(resolved.getRemote());
            }
            return resolvedIndicesBuilder.build();
        }

        // if for some reason we are missing an action... just for safety we'll reject
        if (request instanceof IndicesRequest == false) {
            throw new IllegalStateException("Request [" + request + "] is not an Indices request, but should be.");
        }
        return resolveIndicesAndAliases(action, (IndicesRequest) request, metadata, authorizedIndices);
    }

    /**
     * Attempt to resolve requested indices without expanding any wildcards.
     * @return The {@link ResolvedIndices} or null if wildcard expansion must be performed.
     */
    @Nullable
    static ResolvedIndices tryResolveWithoutWildcards(String action, TransportRequest transportRequest) {
        // We only take care of IndicesRequest
        if (false == transportRequest instanceof IndicesRequest) {
            return null;
        }
        final IndicesRequest indicesRequest = (IndicesRequest) transportRequest;
        if (requiresWildcardExpansion(indicesRequest)) {
            return null;
        }
        // It's safe to cast IndicesRequest since the above test guarantees it
        return resolveIndicesAndAliasesWithoutWildcards(action, indicesRequest);
    }

    private static boolean requiresWildcardExpansion(IndicesRequest indicesRequest) {
        // IndicesAliasesRequest requires special handling because it can have wildcards in request body
        if (indicesRequest instanceof IndicesAliasesRequest) {
            return true;
        }
        // Replaceable requests always require wildcard expansion
        if (indicesRequest instanceof IndicesRequest.Replaceable) {
            return true;
        }
        return false;
    }

    static ResolvedIndices resolveIndicesAndAliasesWithoutWildcards(String action, IndicesRequest indicesRequest) {
        assert false == requiresWildcardExpansion(indicesRequest) : "request must not require wildcard expansion";
        final String[] indices = indicesRequest.indices();
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("the action " + action + " requires explicit index names, but none were provided");
        }
        if (IndexNameExpressionResolver.isAllIndices(Arrays.asList(indices))) {
            throw new IllegalArgumentException(
                "the action "
                    + action
                    + " does not support accessing all indices;"
                    + " the provided index expression ["
                    + Strings.arrayToCommaDelimitedString(indices)
                    + "] is not allowed"
            );
        }

        // TODO: Shard level requests have wildcard expanded already and do not need go through this check
        final List<String> wildcards = Stream.of(indices).filter(Regex::isSimpleMatchPattern).toList();
        if (wildcards.isEmpty() == false) {
            throw new IllegalArgumentException(
                "the action "
                    + action
                    + " does not support wildcards;"
                    + " the provided index expression(s) ["
                    + Strings.collectionToCommaDelimitedString(wildcards)
                    + "] are not allowed"
            );
        }

        // NOTE: shard level requests do support wildcards (as they hold the original indices options) but don't support
        // replacing their indices.
        // That is fine though because they never contain wildcards, as they get replaced as part of the authorization of their
        // corresponding parent request on the coordinating node. Hence wildcards don't need to get replaced nor exploded for
        // shard level requests.
        final List<String> localIndices = new ArrayList<>(indices.length);
        for (String name : indices) {
            localIndices.add(IndexNameExpressionResolver.resolveDateMathExpression(name));
        }
        return new ResolvedIndices(localIndices, List.of());
    }

    ResolvedIndices resolveIndicesAndAliases(
        String action,
        IndicesRequest indicesRequest,
        Metadata metadata,
        Set<String> authorizedIndices
    ) {
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
        } else if (indicesRequest instanceof final IndicesRequest.Replaceable replaceable) {
            final IndicesOptions indicesOptions = indicesRequest.indicesOptions();
            final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();

            // check for all and return list of authorized indices
            if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
                if (replaceWildcards) {
                    for (String authorizedIndex : authorizedIndices) {
                        if (IndexAbstractionResolver.isIndexVisible(
                            "*",
                            authorizedIndex,
                            indicesOptions,
                            metadata,
                            nameExpressionResolver,
                            indicesRequest.includeDataStreams()
                        )) {
                            resolvedIndicesBuilder.addLocal(authorizedIndex);
                        }
                    }
                }
                // if we cannot replace wildcards the indices list stays empty. Same if there are no authorized indices.
                // we honour allow_no_indices like es core does.
            } else {
                final ResolvedIndices split;
                if (replaceable.allowsRemoteIndices()) {
                    split = remoteClusterResolver.splitLocalAndRemoteIndexNames(indicesRequest.indices());
                } else {
                    split = new ResolvedIndices(Arrays.asList(indicesRequest.indices()), Collections.emptyList());
                }
                List<String> replaced = indexAbstractionResolver.resolveIndexAbstractions(
                    split.getLocal(),
                    indicesOptions,
                    metadata,
                    authorizedIndices,
                    replaceWildcards,
                    indicesRequest.includeDataStreams()
                );
                resolvedIndicesBuilder.addLocal(replaced);
                resolvedIndicesBuilder.addRemote(split.getRemote());
            }

            if (resolvedIndicesBuilder.isEmpty()) {
                if (indicesOptions.allowNoIndices()) {
                    // this is how we tell es core to return an empty response, we can let the request through being sure
                    // that the '-*' wildcard expression will be resolved to no indices. We can't let empty indices through
                    // as that would be resolved to _all by es core.
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
            // For performance reasons, non-replaceable requests should be directly handled by
            // resolveIndicesAndAliasesWithoutWildcards instead of being delegated here.
            // That's why an assertion error is triggered here so that we can catch the erroneous usage in testing.
            // But we still delegate in production to avoid our (potential) programing error becoming an end-user problem.
            assert false : "Request [" + indicesRequest + "] is not a replaceable request, but should be.";
            return resolveIndicesAndAliasesWithoutWildcards(action, indicesRequest);
        }

        if (indicesRequest instanceof AliasesRequest aliasesRequest) {
            // special treatment for AliasesRequest since we need to replace wildcards among the specified aliases too.
            // AliasesRequest extends IndicesRequest.Replaceable, hence its indices have already been properly replaced.
            if (aliasesRequest.expandAliasesWildcards()) {
                List<String> aliases = replaceWildcardsWithAuthorizedAliases(
                    aliasesRequest.aliases(),
                    loadAuthorizedAliases(authorizedIndices, metadata)
                );
                aliasesRequest.replaceAliases(aliases.toArray(new String[aliases.size()]));
            }
            if (indicesReplacedWithNoIndices) {
                if (indicesRequest instanceof GetAliasesRequest == false) {
                    throw new IllegalStateException(
                        GetAliasesRequest.class.getSimpleName()
                            + " is the only known "
                            + "request implementing "
                            + AliasesRequest.class.getSimpleName()
                            + " that may allow no indices. Found ["
                            + indicesRequest.getClass().getName()
                            + "] which ended up with an empty set of indices."
                    );
                }
                // if we replaced the indices with '-*' we shouldn't be adding the aliases to the list otherwise the request will
                // not get authorized. Leave only '-*' and ignore the rest, result will anyway be empty.
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
    static String getPutMappingIndexOrAlias(PutMappingRequest request, Set<String> authorizedIndicesList, Metadata metadata) {
        final String concreteIndexName = request.getConcreteIndex().getName();

        // validate that the concrete index exists, otherwise there is no remapping that we could do
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(concreteIndexName);
        final String resolvedAliasOrIndex;
        if (indexAbstraction == null) {
            resolvedAliasOrIndex = concreteIndexName;
        } else if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalStateException(
                "concrete index ["
                    + concreteIndexName
                    + "] is a ["
                    + indexAbstraction.getType().getDisplayName()
                    + "], but a concrete index is expected"
            );
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
                        List<Index> indices = alias.getIndices();
                        if (indices.size() == 1) {
                            return true;
                        } else {
                            assert alias.getType() == IndexAbstraction.Type.ALIAS;
                            Index writeIndex = alias.getWriteIndex();
                            return writeIndex != null && writeIndex.getName().equals(concreteIndexName);
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

    private static List<String> loadAuthorizedAliases(Set<String> authorizedIndices, Metadata metadata) {
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

    private static List<String> replaceWildcardsWithAuthorizedAliases(String[] aliases, List<String> authorizedAliases) {
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
            final List<String> remote = map.entrySet()
                .stream()
                .flatMap(e -> e.getValue().stream().map(v -> e.getKey() + REMOTE_CLUSTER_INDEX_SEPARATOR + v))
                .toList();
            return new ResolvedIndices(local == null ? List.of() : local, remote);
        }
    }

}
