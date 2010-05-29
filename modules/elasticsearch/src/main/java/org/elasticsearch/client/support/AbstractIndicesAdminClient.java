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

package org.elasticsearch.client.support;

import org.elasticsearch.client.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.client.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractIndicesAdminClient implements InternalIndicesAdminClient {

    @Override public IndicesAliasesRequestBuilder prepareAliases() {
        return new IndicesAliasesRequestBuilder(this);
    }

    @Override public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
        return new ClearIndicesCacheRequestBuilder(this).setIndices(indices);
    }

    @Override public CreateIndexRequestBuilder prepareCreate(String index) {
        return new CreateIndexRequestBuilder(this, index);
    }

    @Override public DeleteIndexRequestBuilder prepareDelete(String index) {
        return new DeleteIndexRequestBuilder(this, index);
    }
}
