/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.transport.TransportRequest;

import java.util.Collections;
import java.util.Set;

/**
*
*/
public class DefaultIndicesResolver implements IndicesResolver<TransportRequest> {

    @Override
    public Class<TransportRequest> requestType() {
        return TransportRequest.class;
    }

    @Override
    public Set<String> resolve(TransportRequest request, MetaData metaData) {

        boolean isIndicesRequest = request instanceof CompositeIndicesRequest || request instanceof IndicesRequest;
        assert isIndicesRequest : "the only requests passing the action matcher should be IndicesRequests";

        // if for some reason we are missing an action... just for safety we'll reject
        if (!isIndicesRequest) {
            return Collections.emptySet();
        }

        if (request instanceof CompositeIndicesRequest) {
            Set<String> indices = Sets.newHashSet();
            CompositeIndicesRequest compositeIndicesRequest = (CompositeIndicesRequest) request;
            for (IndicesRequest indicesRequest : compositeIndicesRequest.subRequests()) {
                Collections.addAll(indices, explodeWildcards(indicesRequest, metaData));
            }
            return indices;
        }

        return Sets.newHashSet(explodeWildcards((IndicesRequest) request, metaData));
    }

    private String[] explodeWildcards(IndicesRequest indicesRequest, MetaData metaData) {
        if (indicesRequest.indicesOptions().expandWildcardsOpen() || indicesRequest.indicesOptions().expandWildcardsClosed()) {
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
        return indicesRequest.indices();
    }
}
