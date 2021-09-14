/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

/** A helper class to build {@link GetFieldMappingsRequest} objects */
public class GetFieldMappingsRequestBuilder
        extends ActionRequestBuilder<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    public GetFieldMappingsRequestBuilder(ElasticsearchClient client, GetFieldMappingsAction action, String... indices) {
        super(client, action, new GetFieldMappingsRequest().indices(indices));
    }

    public GetFieldMappingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetFieldMappingsRequestBuilder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return this;
    }

    public GetFieldMappingsRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }


    /** Sets the fields to retrieve. */
    public GetFieldMappingsRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    /** Indicates whether default mapping settings should be returned */
    public GetFieldMappingsRequestBuilder includeDefaults(boolean includeDefaults) {
        request.includeDefaults(includeDefaults);
        return this;
    }
}
