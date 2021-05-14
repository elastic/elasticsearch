/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

public class GetSettingsRequestBuilder
        extends MasterNodeReadOperationRequestBuilder<GetSettingsRequest, GetSettingsResponse, GetSettingsRequestBuilder> {

    public GetSettingsRequestBuilder(ElasticsearchClient client, GetSettingsAction action, String... indices) {
        super(client, action, new GetSettingsRequest().indices(indices));
    }

    public GetSettingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetSettingsRequestBuilder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public GetSettingsRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    public GetSettingsRequestBuilder setNames(String... names) {
        request.names(names);
        return this;
    }
}
