/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.TimeValue;

public class GetIndexRequestBuilder extends ActionRequestBuilder<GetIndexRequest, GetIndexResponse> {

    public GetIndexRequestBuilder(ElasticsearchClient client, TimeValue masterTimeout, String... indices) {
        super(client, GetIndexAction.INSTANCE, new GetIndexRequest(masterTimeout).indices(indices));
    }

    public GetIndexRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetIndexRequestBuilder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return this;
    }

    public GetIndexRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    public GetIndexRequestBuilder setFeatures(Feature... features) {
        request.features(features);
        return this;
    }

    public GetIndexRequestBuilder addFeatures(Feature... features) {
        request.addFeatures(features);
        return this;
    }
}
