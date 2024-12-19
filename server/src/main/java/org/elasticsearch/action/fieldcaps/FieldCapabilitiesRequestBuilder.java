/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

public class FieldCapabilitiesRequestBuilder extends ActionRequestBuilder<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public FieldCapabilitiesRequestBuilder(ElasticsearchClient client, String... indices) {
        super(client, TransportFieldCapabilitiesAction.TYPE, new FieldCapabilitiesRequest().indices(indices));
    }

    /**
     * The list of field names to retrieve.
     */
    public FieldCapabilitiesRequestBuilder setFields(String... fields) {
        request().fields(fields);
        return this;
    }

    public FieldCapabilitiesRequestBuilder setIncludeUnmapped(boolean includeUnmapped) {
        request().includeUnmapped(includeUnmapped);
        return this;
    }

    public FieldCapabilitiesRequestBuilder setincludeEmptyFields(boolean includeEmptyFields) {
        request().includeEmptyFields(includeEmptyFields);
        return this;
    }

    public FieldCapabilitiesRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    public FieldCapabilitiesRequestBuilder setIndexFilter(QueryBuilder indexFilter) {
        request().indexFilter(indexFilter);
        return this;
    }

    public FieldCapabilitiesRequestBuilder setRuntimeFields(Map<String, Object> runtimeFieldSection) {
        request().runtimeFields(runtimeFieldSection);
        return this;
    }
}
