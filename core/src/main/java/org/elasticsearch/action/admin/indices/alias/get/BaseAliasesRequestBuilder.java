/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

/**
 */
public abstract class BaseAliasesRequestBuilder<Response extends ActionResponse, Builder extends BaseAliasesRequestBuilder<Response, Builder>> extends MasterNodeReadOperationRequestBuilder<GetAliasesRequest, Response, Builder> {

    public BaseAliasesRequestBuilder(ElasticsearchClient client, Action<GetAliasesRequest, Response, Builder> action, String... aliases) {
        super(client, action, new GetAliasesRequest(aliases));
    }

    @SuppressWarnings("unchecked")
    public Builder setAliases(String... aliases) {
        request.aliases(aliases);
        return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder addAliases(String... aliases) {
        request.aliases(ArrayUtils.concat(request.aliases(), aliases));
        return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setIndices(String... indices) {
        request.indices(indices);
        return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return (Builder) this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     *
     * For example indices that don't exist.
     */
    @SuppressWarnings("unchecked")
    public Builder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return (Builder) this;
    }

}
