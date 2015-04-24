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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.index.query.FilterBuilder;

import java.util.Map;

/**
 *
 */
public class IndicesAliasesRequestBuilder extends AcknowledgedRequestBuilder<IndicesAliasesRequest, IndicesAliasesResponse, IndicesAliasesRequestBuilder, IndicesAdminClient> {

    public IndicesAliasesRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new IndicesAliasesRequest());
    }
    
    /**
     * Adds an alias to the index.
     *
     * @param index         The index
     * @param alias         The alias
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias) {
        request.addAlias(alias, index);
        return this;
    }
    
    /**
     * Adds an alias to the index.
     *
     * @param indices The indices
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder addAlias(String[] indices, String alias) {
        request.addAlias(alias, indices);
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
        AliasActions action = new AliasActions(AliasAction.Type.ADD, index, alias).filter(filter);
        request.addAliasAction(action);
        return this;
    }
    
    /**
     * Adds an alias to the index.
     *
     * @param indices       The indices
     * @param alias         The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String indices[], String alias, String filter) {
        AliasActions action = new AliasActions(AliasAction.Type.ADD, indices, alias).filter(filter);
        request.addAliasAction(action);
        return this;
    }
   
    /**
     * Adds an alias to the index.
     *
     * @param indices  The indices
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String[] indices, String alias, Map<String, Object> filter) {
        request.addAlias(alias, filter, indices);
        return this;
    }
    
    /**
     * Adds an alias to the index.
     *
     * @param index  The indices
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, Map<String, Object> filter) {
        request.addAlias(alias, filter, index);
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param indices       The indices
     * @param alias         The alias
     * @param filterBuilder The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String indices[], String alias, FilterBuilder filterBuilder) {
        request.addAlias(alias, filterBuilder, indices);
        return this;
    }
    
    /**
     * Adds an alias to the index.
     *
     * @param index       The index
     * @param alias         The alias
     * @param filterBuilder The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, FilterBuilder filterBuilder) {
        request.addAlias(alias, filterBuilder, index);
        return this;
    }

    /**
     * Removes an alias from the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String alias) {
        request.removeAlias(index, alias);
        return this;
    }
    
    /**
     * Removes aliases from the index.
     *
     * @param indices The indices
     * @param aliases The aliases
     */
    public IndicesAliasesRequestBuilder removeAlias(String[] indices, String... aliases) {
        request.removeAlias(indices, aliases);
        return this;
    }
    
    /**
     * Removes aliases from the index.
     *
     * @param index The index
     * @param aliases The aliases
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String[] aliases) {
        request.removeAlias(index, aliases);
        return this;
    }
    
    @Override
    protected void doExecute(ActionListener<IndicesAliasesResponse> listener) {
        client.aliases(request, listener);
    }
    
    /**
     * Adds an alias action to the request.
     *
     * @param aliasAction The alias action
     */
    public IndicesAliasesRequestBuilder addAliasAction(AliasAction aliasAction) {
        request.addAliasAction(aliasAction);
        return this;
    }

    /**
     * Adds an alias action to the request.
     *
     * @param action The alias action
     */
    public IndicesAliasesRequestBuilder addAliasAction(AliasActions action) {
        request.addAliasAction(action);
        return this;
    }
}
