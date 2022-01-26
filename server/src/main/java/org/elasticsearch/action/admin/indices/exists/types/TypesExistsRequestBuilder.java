/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.exists.types;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;

/**
 * A builder for {@link TypesExistsRequest}.
 */
@Deprecated
public class TypesExistsRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    TypesExistsRequest,
    TypesExistsResponse,
    TypesExistsRequestBuilder> {

    /**
     * @param indices What indices to check for types
     */
    public TypesExistsRequestBuilder(ElasticsearchClient client, TypesExistsAction action, String... indices) {
        super(client, action, new TypesExistsRequest(indices, Strings.EMPTY_ARRAY));
    }

    TypesExistsRequestBuilder(ElasticsearchClient client, TypesExistsAction action) {
        super(client, action, new TypesExistsRequest());
    }

    /**
     * @param indices What indices to check for types
     */
    public TypesExistsRequestBuilder setIndices(String[] indices) {
        request.indices(indices);
        return this;
    }

    /**
     * @param types The types to check if they exist
     */
    public TypesExistsRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * @param indicesOptions Specifies how to resolve indices that aren't active / ready and indices wildcard expressions
     */
    public TypesExistsRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
