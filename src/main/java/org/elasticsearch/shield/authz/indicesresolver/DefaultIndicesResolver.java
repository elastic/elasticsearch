/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Sets;
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
                List<String> indices = replaceWildcardsWithAuthorizedIndices(indicesRequest, metaData, authorizedIndices);
                //ignore the IndicesOptions#allowNoIndices and just throw exception if the wildcards expansion to authorized
                //indices resulted in no indices. This is important as we always need to replace wildcards for security reason,
                //to make sure that the operation is executed on the indices that we authorized it to execute on.
                //If we can't replace because we got an empty set, we can only throw exception.
                //Downside of this is that a single item exception is going to make fail the composite request that holds it as a whole.
                if (indices == null || indices.isEmpty()) {
                    if (MetaData.isAllIndices(indicesRequest.indices())) {
                        throw new IndexMissingException(new Index(MetaData.ALL));
                    }
                    throw new IndexMissingException(new Index(Arrays.toString(indicesRequest.indices())));
                }
                ((IndicesRequest.Replaceable) indicesRequest).indices(indices.toArray(new String[indices.size()]));
                return Sets.newHashSet(indices);
            }

            if (containsWildcards(indicesRequest)) {
                //used for requests that support wildcards but don't allow to replace their indices (e.g. IndicesAliasesRequest)
                //potentially insecure as cluster state may change hence we may end up resolving to different indices on different nodes
                assert indicesRequest instanceof IndicesAliasesRequest
                        : "IndicesAliasesRequest is the only request known to support wildcards that doesn't support replacing its indices";
                return Sets.newHashSet(explodeWildcards(indicesRequest, metaData));
            }
            //NOTE: shard level requests do support wildcards (as they hold the original indices options) but don't support replacing their indices.
            //That is fine though because they never contain wildcards, as they get replaced as part of the authorization of their
            //corresponding parent request on the coordinating node. Hence wildcards don't get replaced nor exploded for shard level requests.
        }
        return Sets.newHashSet(indicesRequest.indices());
    }

    /*
     * Explodes wildcards based on default core behaviour. Used for IndicesAliasesRequest only as it doesn't support
     * replacing its indices. It will go away once that gets fixed.
     */
    private String[] explodeWildcards(IndicesRequest indicesRequest, MetaData metaData) {
        //note that "_all" will map to concrete indices only, as the same happens in core
        //which is different from "*" as the latter expands to all indices and aliases
        if (MetaData.isAllIndices(indicesRequest.indices())) {
            if (indicesRequest.indicesOptions().expandWildcardsOpen() && indicesRequest.indicesOptions().expandWildcardsClosed()) {
                return metaData.concreteAllIndices();
            }
            if (indicesRequest.indicesOptions().expandWildcardsOpen()) {
                return metaData.concreteAllOpenIndices();
            }
            return metaData.concreteAllClosedIndices();

        }
        return metaData.convertFromWildcards(indicesRequest.indices(), indicesRequest.indicesOptions());
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

    private List<String> replaceWildcardsWithAuthorizedIndices(IndicesRequest indicesRequest, MetaData metaData, List<String> authorizedIndices) {

        if (MetaData.isAllIndices(indicesRequest.indices())) {
            List<String> visibleIndices = new ArrayList<>();
            for (String authorizedIndex : authorizedIndices) {
                if (isIndexVisible(authorizedIndex, indicesRequest.indicesOptions(), metaData)) {
                    visibleIndices.add(authorizedIndex);
                }
            }
            return visibleIndices;
        }

        //the order matters when it comes to + and - (see MetaData#convertFromWildcards)
        List<String> finalIndices = new ArrayList<>();
        for (int i = 0; i < indicesRequest.indices().length; i++) {
            String index = indicesRequest.indices()[i];
            String aliasOrIndex;
            boolean minus = false;
            if (index.charAt(0) == '+') {
                aliasOrIndex = index.substring(1);
            } else if (index.charAt(0) == '-') {
                if (i == 0) {
                    //mimic the MetaData#convertFromWilcards behaviour with "-index" syntax
                    //but instead of adding all the indices, add only the ones that the user is authorized for
                    for (String authorizedIndex : authorizedIndices) {
                        if (isIndexVisible(authorizedIndex, indicesRequest.indicesOptions(), metaData)) {
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
                            if (isIndexVisible(authorizedIndex, indicesRequest.indicesOptions(), metaData)) {
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

        return finalIndices;
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
