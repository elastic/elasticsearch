/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.hppc.ObjectLookupContainer;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.transport.TransportRequest;

import java.util.*;

/**
 *
 */
public class DefaultIndicesResolver implements IndicesResolver<TransportRequest> {

    private final AuthorizationService authzService;

    public DefaultIndicesResolver(AuthorizationService authzService) {
        this.authzService = authzService;
    }

    @Override
    public Class<TransportRequest> requestType() {
        return TransportRequest.class;
    }

    @Override
    public Set<String> resolve(User user, String action, TransportRequest request, MetaData metaData) {

        boolean isIndicesRequest = request instanceof CompositeIndicesRequest || request instanceof IndicesRequest;
        assert isIndicesRequest : "Request [" + request + "] is not an Indices request. The only requests passing the action matcher should be IndicesRequests";

        // if for some reason we are missing an action... just for safety we'll reject
        if (!isIndicesRequest) {
            return Collections.emptySet();
        }

        if (request instanceof CompositeIndicesRequest) {
            Set<String> indices = Sets.newHashSet();
            CompositeIndicesRequest compositeIndicesRequest = (CompositeIndicesRequest) request;
            for (IndicesRequest indicesRequest : compositeIndicesRequest.subRequests()) {
                indices.addAll(resolveIndices(user, action, indicesRequest, metaData));
            }
            return indices;
        }

        return resolveIndices(user, action, (IndicesRequest) request, metaData);
    }

    private Set<String> resolveIndices(User user, String action, IndicesRequest indicesRequest, MetaData metaData) {
        if (indicesRequest.indicesOptions().expandWildcardsOpen() || indicesRequest.indicesOptions().expandWildcardsClosed()) {
            if (indicesRequest instanceof IndicesRequest.Replaceable) {
                ImmutableList<String> authorizedIndices = authzService.authorizedIndicesAndAliases(user, action);
                List<String> indices = replaceWildcardsWithAuthorizedIndices(indicesRequest.indices(), indicesRequest.indicesOptions(), metaData, authorizedIndices);
                ((IndicesRequest.Replaceable) indicesRequest).indices(indices.toArray(new String[indices.size()]));
            } else {
                assert indicesRequest instanceof IndicesAliasesRequest || !containsWildcards(indicesRequest) :
                        "IndicesAliasesRequest is the only external request known to support wildcards that doesn't support replacing its indices";

                //NOTE: shard level requests do support wildcards (as they hold the original indices options) but don't support replacing their indices.
                //That is fine though because they never contain wildcards, as they get replaced as part of the authorization of their
                //corresponding parent request on the coordinating node. Hence wildcards don't need to get replaced nor exploded for shard level requests.
            }
        }

        if (indicesRequest instanceof IndicesAliasesRequest) {
            //special treatment for IndicesAliasesRequest since we need to extract indices from indices() as well as aliases()
            //Also, we need to replace wildcards in both with authorized indices and/or aliases (IndicesAliasesRequest doesn't implement Replaceable)
            return resolveIndicesAliasesRequest(user, action, (IndicesAliasesRequest) indicesRequest, metaData);
        }

        if (indicesRequest instanceof GetAliasesRequest) {
            //special treatment for GetAliasesRequest since we need to replace wildcards among the specified aliases.
            //GetAliasesRequest implements IndicesRequest.Replaceable, hence its indices have already been properly replaced.
            return resolveGetAliasesRequest(user, action, (GetAliasesRequest) indicesRequest, metaData);
        }

        return Sets.newHashSet(indicesRequest.indices());
    }

    private Set<String> resolveGetAliasesRequest(User user, String action, GetAliasesRequest request, MetaData metaData) {
        ImmutableList<String> authorizedIndices = authzService.authorizedIndicesAndAliases(user, action);
        List<String> aliases = replaceWildcardsWithAuthorizedAliases(request.aliases(), loadAuthorizedAliases(authorizedIndices, metaData));
        request.aliases(aliases.toArray(new String[aliases.size()]));
        Set<String> indices = Sets.newHashSet(request.indices());
        indices.addAll(aliases);
        return indices;
    }

    private Set<String> resolveIndicesAliasesRequest(User user, String action, IndicesAliasesRequest request, MetaData metaData) {
        ImmutableList<String> authorizedIndices = authzService.authorizedIndicesAndAliases(user, action);
        Set<String> finalIndices = Sets.newHashSet();

        List<String> authorizedAliases = null;

        for (IndicesAliasesRequest.AliasActions aliasActions : request.getAliasActions()) {
            //replace indices with authorized ones if needed
            if (request.indicesOptions().expandWildcardsOpen() || request.indicesOptions().expandWildcardsClosed()) {
                //Note: the indices that the alias operation maps to might end up containing aliases, since authorized indices can also be aliases.
                //This is fine as es core resolves them to concrete indices anyway before executing the actual operation.
                //Also es core already allows to specify aliases among indices, they will just be resolved (alias to alias is not supported).
                //e.g. index: foo* gets resolved in core to anything that matches the expression, aliases included, hence their corresponding indices.
                List<String> indices = replaceWildcardsWithAuthorizedIndices(aliasActions.indices(), request.indicesOptions(), metaData, authorizedIndices);
                aliasActions.indices(indices.toArray(new String[indices.size()]));
            }
            Collections.addAll(finalIndices, aliasActions.indices());

            //replace aliases with authorized ones if needed
            if (aliasActions.actionType() == AliasAction.Type.REMOVE) {
                //lazily initialize a list of all the authorized aliases (filtering concrete indices out)
                if (authorizedAliases == null) {
                    authorizedAliases = loadAuthorizedAliases(authorizedIndices, metaData);
                }

                List<String> aliases = replaceWildcardsWithAuthorizedAliases(aliasActions.aliases(), authorizedAliases);
                aliasActions.aliases(aliases.toArray(new String[aliases.size()]));
            }
            Collections.addAll(finalIndices, aliasActions.aliases());
        }
        return finalIndices;
    }

    private List<String> loadAuthorizedAliases(List<String> authorizedIndices, MetaData metaData) {
        List<String> authorizedAliases = Lists.newArrayList();
        ObjectLookupContainer<String> existingAliases = metaData.aliases().keys();
        for (String authorizedIndex : authorizedIndices) {
            if (existingAliases.contains(authorizedIndex)) {
                authorizedAliases.add(authorizedIndex);
            }
        }
        return authorizedAliases;
    }

    private List<String> replaceWildcardsWithAuthorizedAliases(String[] aliases, List<String> authorizedAliases) {
        List<String> finalAliases = Lists.newArrayList();
        for (String aliasPattern : aliases) {
            if (aliasPattern.equals(MetaData.ALL)) {
                finalAliases.addAll(authorizedAliases);
            } else if (Regex.isSimpleMatchPattern(aliasPattern)) {
                for (String authorizedAlias : authorizedAliases) {
                    if (Regex.simpleMatch(aliasPattern, authorizedAlias)) {
                        finalAliases.add(authorizedAlias);
                    }
                }
            } else {
                finalAliases.add(aliasPattern);
            }
        }

        //throw exception if the wildcards expansion to authorized aliases resulted in no indices.
        // This is important as we always need to replace wildcards for security reason,
        //to make sure that the operation is executed on the aliases that we authorized it to execute on.
        //If we can't replace because we got an empty set, we can only throw exception.
        if (finalAliases.isEmpty()) {
            throw new IndexMissingException(new Index(Arrays.toString(aliases)));
        }
        return finalAliases;
    }

    private boolean containsWildcards(IndicesRequest indicesRequest) {
        if (MetaData.isAllIndices(indicesRequest.indices())) {
            return true;
        }
        for (String index : indicesRequest.indices()) {
            if (index.startsWith("+") || index.startsWith("-") || Regex.isSimpleMatchPattern(index)) {
                return true;
            }
        }
        return false;
    }

    private List<String> replaceWildcardsWithAuthorizedIndices(String[] indices, IndicesOptions indicesOptions, MetaData metaData, List<String> authorizedIndices) {

        if (MetaData.isAllIndices(indices)) {
            List<String> visibleIndices = new ArrayList<>();
            for (String authorizedIndex : authorizedIndices) {
                if (isIndexVisible(authorizedIndex, indicesOptions, metaData)) {
                    visibleIndices.add(authorizedIndex);
                }
            }
            return throwExceptionIfNoIndicesWereResolved(indices, visibleIndices);
        }

        //the order matters when it comes to + and - (see MetaData#convertFromWildcards)
        List<String> finalIndices = new ArrayList<>();
        for (int i = 0; i < indices.length; i++) {
            String index = indices[i];
            String aliasOrIndex;
            boolean minus = false;
            if (index.charAt(0) == '+') {
                aliasOrIndex = index.substring(1);
            } else if (index.charAt(0) == '-') {
                if (i == 0) {
                    //mimic the MetaData#convertFromWilcards behaviour with "-index" syntax
                    //but instead of adding all the indices, add only the ones that the user is authorized for
                    for (String authorizedIndex : authorizedIndices) {
                        if (isIndexVisible(authorizedIndex, indicesOptions, metaData)) {
                            finalIndices.add(authorizedIndex);
                        }
                    }
                }
                aliasOrIndex = index.substring(1);
                minus = true;
            } else {
                aliasOrIndex = index;
            }

            if (Regex.isSimpleMatchPattern(aliasOrIndex)) {
                for (String authorizedIndex : authorizedIndices) {
                    if (Regex.simpleMatch(aliasOrIndex, authorizedIndex)) {
                        if (minus) {
                            finalIndices.remove(authorizedIndex);
                        } else {
                            if (isIndexVisible(authorizedIndex, indicesOptions, metaData)) {
                                finalIndices.add(authorizedIndex);
                            }
                        }
                    }
                }
            } else {
                //MetaData#convertFromWildcards checks if the index exists here and throws IndexMissingException if not (based on ignore_unavailable).
                //Do nothing as if the index is missing but the user is not authorized to it an AuthorizationException will be thrown.
                //If the index is missing and the user is authorized to it, core will throw IndexMissingException later on.
                //There is no problem with deferring this as we are dealing with an explicit name, not with wildcards.
                if (minus) {
                    finalIndices.remove(aliasOrIndex);
                } else {
                    finalIndices.add(aliasOrIndex);
                }
            }
        }

        return throwExceptionIfNoIndicesWereResolved(indices, finalIndices);
    }

    private List<String> throwExceptionIfNoIndicesWereResolved(String[] originalIndices, List<String> resolvedIndices) {
        //ignore the IndicesOptions#allowNoIndices and just throw exception if the wildcards expansion to authorized
        //indices resulted in no indices. This is important as we always need to replace wildcards for security reason,
        //to make sure that the operation is executed on the indices that we authorized it to execute on.
        //If we can't replace because we got an empty set, we can only throw exception.
        //Downside of this is that a single item exception is going to make fail the composite request that holds it as a whole.
        if (resolvedIndices == null || resolvedIndices.isEmpty()) {
            if (MetaData.isAllIndices(originalIndices)) {
                throw new IndexMissingException(new Index(MetaData.ALL));
            }
            throw new IndexMissingException(new Index(Arrays.toString(originalIndices)));
        }
        return resolvedIndices;
    }

    private static boolean isIndexVisible(String index, IndicesOptions indicesOptions, MetaData metaData) {
        if (metaData.hasConcreteIndex(index)) {
            IndexMetaData indexMetaData = metaData.index(index);
            if (indexMetaData == null) {
                //it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
                //complicated to support those options with aliases pointing to multiple indices...
                return true;
            }
            if (indexMetaData.state() == IndexMetaData.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
                return true;
            }
            if (indexMetaData.state() == IndexMetaData.State.OPEN && indicesOptions.expandWildcardsOpen()) {
                return true;
            }
        }
        return false;
    }
}
