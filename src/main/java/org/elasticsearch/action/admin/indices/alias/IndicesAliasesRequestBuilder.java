/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.index.query.FilterBuilder;

import java.util.Map;

/**
 *
 */
public class IndicesAliasesRequestBuilder extends AcknowledgedRequestBuilder<IndicesAliasesRequest, IndicesAliasesResponse, IndicesAliasesRequestBuilder> {

    public IndicesAliasesRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new IndicesAliasesRequest());
    }

    /**
     * Adds an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias) {
        request.addAlias(index, alias);
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The index
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, String filter) {
        request.addAlias(index, alias, filter);
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The index
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, Map<String, Object> filter) {
        request.addAlias(index, alias, filter);
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index         The index
     * @param alias         The alias
     * @param filterBuilder The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, FilterBuilder filterBuilder) {
        request.addAlias(index, alias, filterBuilder);
        return this;
    }

    /**
     * Adds an alias action to the request.
     *
     * @param aliasAction The alias Action
     */
    public IndicesAliasesRequestBuilder addAliasAction(AliasAction aliasAction) {
        request.addAliasAction(aliasAction);
        return this;
    }

    /**
     * Removes an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String alias) {
        request.removeAlias(index, alias);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<IndicesAliasesResponse> listener) {
        ((IndicesAdminClient) client).aliases(request, listener);
    }
}
