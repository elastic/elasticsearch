/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.transport.TransportRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

/**
 *
 */
public class DefaultIndicesAndAliasesResolver implements IndicesAndAliasesResolver<TransportRequest> {

    private final AuthorizationService authzService;
    private final IndexNameExpressionResolver nameExpressionResolver;

    public DefaultIndicesAndAliasesResolver(AuthorizationService authzService, IndexNameExpressionResolver nameExpressionResolver) {
        this.authzService = authzService;
        this.nameExpressionResolver = nameExpressionResolver;
    }

    @Override
    public Class<TransportRequest> requestType() {
        return TransportRequest.class;
    }

    @Override
    public Set<String> resolve(User user, String action, TransportRequest request, MetaData metaData) {

        boolean isIndicesRequest = request instanceof CompositeIndicesRequest || request instanceof IndicesRequest;
        assert isIndicesRequest : "Request [" + request + "] is not an Indices request, but should be.";

        // if for some reason we are missing an action... just for safety we'll reject
        if (!isIndicesRequest) {
            return Collections.emptySet();
        }

        if (request instanceof CompositeIndicesRequest) {
            Set<String> indices = new HashSet<>();
            CompositeIndicesRequest compositeIndicesRequest = (CompositeIndicesRequest) request;
            for (IndicesRequest indicesRequest : compositeIndicesRequest.subRequests()) {
                indices.addAll(resolveIndicesAndAliases(user, action, indicesRequest, metaData));
            }
            return indices;
        }

        return resolveIndicesAndAliases(user, action, (IndicesRequest) request, metaData);
    }

    private Set<String> resolveIndicesAndAliases(User user, String action, IndicesRequest indicesRequest, MetaData metaData) {
        final Set<String> indices;
        if (indicesRequest instanceof PutMappingRequest
                && ((PutMappingRequest) indicesRequest).getConcreteIndex() != null) {
            /**
             * This is a special case since PutMappingRequests from dynamic mapping updates have a concrete index
             * if this index is set and it's in the list of authorized indices we are good and don't need to put
             * the list of indices in there, if we do so it will result in an invalid request and the update will fail.
             */
            indices = Collections.singleton(((PutMappingRequest) indicesRequest).getConcreteIndex().getName());
            assert indicesRequest.indices() == null || indicesRequest.indices().length == 0
                    : "indices are: " + Arrays.toString(indicesRequest.indices()); // Arrays.toString() can handle null values - all good
        } else if (indicesRequest instanceof IndicesRequest.Replaceable) {
            IndicesRequest.Replaceable replaceable = (IndicesRequest.Replaceable) indicesRequest;
            final boolean replaceWildcards = indicesRequest.indicesOptions().expandWildcardsOpen()
                    || indicesRequest.indicesOptions().expandWildcardsClosed();
            List<String> authorizedIndices = replaceWildcardsWithAuthorizedIndices(indicesRequest.indices(),
                    indicesRequest.indicesOptions(),
                    metaData,
                    authzService.authorizedIndicesAndAliases(user, action),
                    replaceWildcards);
            replaceable.indices(authorizedIndices.toArray(new String[authorizedIndices.size()]));
            indices = Sets.newHashSet(indicesRequest.indices());
        } else {
            assert !containsWildcards(indicesRequest) :
                    "There are no external requests known to support wildcards that don't support replacing their indices";
            //NOTE: shard level requests do support wildcards (as they hold the original indices options) but don't support
            // replacing their indices.
            //That is fine though because they never contain wildcards, as they get replaced as part of the authorization of their
            //corresponding parent request on the coordinating node. Hence wildcards don't need to get replaced nor exploded for
            // shard level requests.
            List<String> resolvedNames = new ArrayList<>();
            for (String name : indicesRequest.indices()) {
                resolvedNames.add(nameExpressionResolver.resolveDateMathExpression(name));
            }
            indices = Sets.newHashSet(resolvedNames);
        }

        if (indicesRequest instanceof AliasesRequest) {
            //special treatment for AliasesRequest since we need to replace wildcards among the specified aliases.
            //AliasesRequest extends IndicesRequest.Replaceable, hence its indices have already been properly replaced.
            AliasesRequest aliasesRequest = (AliasesRequest) indicesRequest;
            if (aliasesRequest.expandAliasesWildcards()) {
                List<String> authorizedIndices = authzService.authorizedIndicesAndAliases(user, action);
                List<String> aliases = replaceWildcardsWithAuthorizedAliases(aliasesRequest.aliases(), loadAuthorizedAliases
                        (authorizedIndices, metaData));
                aliasesRequest.aliases(aliases.toArray(new String[aliases.size()]));
            }
            Collections.addAll(indices, aliasesRequest.aliases());
        }

        return indices;
    }

    private List<String> loadAuthorizedAliases(List<String> authorizedIndices, MetaData metaData) {
        List<String> authorizedAliases = new ArrayList<>();
        SortedMap<String, AliasOrIndex> existingAliases = metaData.getAliasAndIndexLookup();
        for (String authorizedIndex : authorizedIndices) {
            AliasOrIndex aliasOrIndex = existingAliases.get(authorizedIndex);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                authorizedAliases.add(authorizedIndex);
            }
        }
        return authorizedAliases;
    }

    private List<String> replaceWildcardsWithAuthorizedAliases(String[] aliases, List<String> authorizedAliases) {
        List<String> finalAliases = new ArrayList<>();

        //IndicesAliasesRequest doesn't support empty aliases (validation fails) but GetAliasesRequest does (in which case empty means _all)
        boolean matchAllAliases = aliases.length == 0;
        if (matchAllAliases) {
            finalAliases.addAll(authorizedAliases);
        }

        for (String aliasPattern : aliases) {
            if (aliasPattern.equals(MetaData.ALL)) {
                matchAllAliases = true;
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
            String indexName = matchAllAliases ? MetaData.ALL : Arrays.toString(aliases);
            throw new IndexNotFoundException(indexName);
        }
        return finalAliases;
    }

    private boolean containsWildcards(IndicesRequest indicesRequest) {
        if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
            return true;
        }
        for (String index : indicesRequest.indices()) {
            if (index.startsWith("+") || index.startsWith("-") || Regex.isSimpleMatchPattern(index)) {
                return true;
            }
        }
        return false;
    }

    private List<String> replaceWildcardsWithAuthorizedIndices(String[] indices, IndicesOptions indicesOptions, MetaData metaData,
                                                               List<String> authorizedIndices, boolean replaceWildcards) {

        // check for all and return list of authorized indices
        if (IndexNameExpressionResolver.isAllIndices(indicesList(indices))) {
            if (replaceWildcards == false) {
                // if we cannot replace wildcards, then we should not set all indices
                return throwExceptionIfNoIndicesWereResolved(indices, null);
            }
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

            if (replaceWildcards && Regex.isSimpleMatchPattern(aliasOrIndex)) {
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
                // we always need to check for date math expressions
                String dateMathName = nameExpressionResolver.resolveDateMathExpression(aliasOrIndex);
                // we can use != here to compare strings since the name expression resolver returns the same instance, but add an assert
                // to ensure we catch this if it changes
                if (dateMathName != aliasOrIndex) {
                    assert dateMathName.equals(aliasOrIndex) == false;
                    if (authorizedIndices.contains(dateMathName)) {
                        if (minus) {
                            finalIndices.remove(dateMathName);
                        } else {
                            if (isIndexVisible(dateMathName, indicesOptions, metaData, true)) {
                                finalIndices.add(dateMathName);
                            }
                        }
                    }
                } else {
                    //MetaData#convertFromWildcards checks if the index exists here and throws IndexNotFoundException if not (based on
                    // ignore_unavailable).
                    //Do nothing as if the index is missing but the user is not authorized to it an AuthorizationException will be thrown.
                    //If the index is missing and the user is authorized to it, core will throw IndexNotFoundException later on.
                    //There is no problem with deferring this as we are dealing with an explicit name, not with wildcards.
                    if (minus) {
                        finalIndices.remove(aliasOrIndex);
                    } else {
                        finalIndices.add(aliasOrIndex);
                    }
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
            String indexName = IndexNameExpressionResolver.isAllIndices(indicesList(originalIndices)) ? MetaData.ALL : Arrays.toString
                    (originalIndices);
            throw new IndexNotFoundException(indexName);
        }
        return resolvedIndices;
    }

    private static boolean isIndexVisible(String index, IndicesOptions indicesOptions, MetaData metaData) {
        return isIndexVisible(index, indicesOptions, metaData, false);
    }

    private static boolean isIndexVisible(String index, IndicesOptions indicesOptions, MetaData metaData, boolean dateMathExpression) {
        if (metaData.hasConcreteIndex(index)) {
            IndexMetaData indexMetaData = metaData.index(index);
            if (indexMetaData == null) {
                //it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
                //complicated to support those options with aliases pointing to multiple indices...
                return true;
            }
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE && (indicesOptions.expandWildcardsClosed() || dateMathExpression)) {
                return true;
            }
            if (indexMetaData.getState() == IndexMetaData.State.OPEN && (indicesOptions.expandWildcardsOpen() || dateMathExpression)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> indicesList(String[] list) {
        return (list == null) ? null : Arrays.asList(list);
    }
}
