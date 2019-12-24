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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class DeleteByQueryRequestBuilder extends
        AbstractBulkByScrollRequestBuilder<DeleteByQueryRequest, DeleteByQueryRequestBuilder> {

    public DeleteByQueryRequestBuilder(ElasticsearchClient client, ActionType<BulkByScrollResponse> action) {
        this(client, action, new SearchRequestBuilder(client, SearchAction.INSTANCE));
    }

    private DeleteByQueryRequestBuilder(ElasticsearchClient client,
                                        ActionType<BulkByScrollResponse> action,
                                        SearchRequestBuilder search) {
        super(client, action, search, new DeleteByQueryRequest(search.request()));
    }

    @Override
    protected DeleteByQueryRequestBuilder self() {
        return this;
    }

    @Override
    public DeleteByQueryRequestBuilder abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.setAbortOnVersionConflict(abortOnVersionConflict);
        return this;
    }
}
