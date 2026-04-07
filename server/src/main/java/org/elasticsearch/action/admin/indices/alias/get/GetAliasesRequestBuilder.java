/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

public class GetAliasesRequestBuilder extends ActionRequestBuilder<GetAliasesRequest, GetAliasesResponse> {
    public GetAliasesRequestBuilder(ElasticsearchClient client, TimeValue masterTimeout, String... aliases) {
        super(client, GetAliasesAction.INSTANCE, new GetAliasesRequest(masterTimeout, aliases));
    }

    public GetAliasesRequestBuilder setAliases(String... aliases) {
        request.aliases(aliases);
        return this;
    }

    public GetAliasesRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetAliasesRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }
}
