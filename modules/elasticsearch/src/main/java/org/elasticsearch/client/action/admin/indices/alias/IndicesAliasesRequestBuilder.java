/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.client.action.admin.indices.alias;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.action.admin.indices.support.BaseIndicesRequestBuilder;

/**
 * @author kimchy (shay.banon)
 */
public class IndicesAliasesRequestBuilder extends BaseIndicesRequestBuilder<IndicesAliasesRequest, IndicesAliasesResponse> {

    public IndicesAliasesRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new IndicesAliasesRequest());
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
     * Removes an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String alias) {
        request.removeAlias(index, alias);
        return this;
    }

    @Override protected void doExecute(ActionListener<IndicesAliasesResponse> listener) {
        client.aliases(request, listener);
    }
}
