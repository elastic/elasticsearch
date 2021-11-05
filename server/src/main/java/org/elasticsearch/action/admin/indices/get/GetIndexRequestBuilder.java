/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.master.info.ClusterInfoRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class GetIndexRequestBuilder extends ClusterInfoRequestBuilder<GetIndexRequest, GetIndexResponse, GetIndexRequestBuilder> {

    public GetIndexRequestBuilder(ElasticsearchClient client, GetIndexAction action, String... indices) {
        super(client, action, new GetIndexRequest().indices(indices));
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
