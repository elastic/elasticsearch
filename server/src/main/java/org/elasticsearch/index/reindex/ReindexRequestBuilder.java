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
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class ReindexRequestBuilder extends
        AbstractBulkIndexByScrollRequestBuilder<ReindexRequest, ReindexRequestBuilder> {
    private final IndexRequestBuilder destination;

    public ReindexRequestBuilder(ElasticsearchClient client,
            ActionType<BulkByScrollResponse> action) {
        this(client, action, new SearchRequestBuilder(client, SearchAction.INSTANCE),
                new IndexRequestBuilder(client, IndexAction.INSTANCE));
    }

    private ReindexRequestBuilder(ElasticsearchClient client,
            ActionType<BulkByScrollResponse> action,
            SearchRequestBuilder search, IndexRequestBuilder destination) {
        super(client, action, search, new ReindexRequest(search.request(), destination.request()));
        this.destination = destination;
    }

    @Override
    protected ReindexRequestBuilder self() {
        return this;
    }

    public IndexRequestBuilder destination() {
        return destination;
    }

    /**
     * Set the destination index.
     */
    public ReindexRequestBuilder destination(String index) {
        destination.setIndex(index);
        return this;
    }

    /**
     * Setup reindexing from a remote cluster.
     */
    public ReindexRequestBuilder setRemoteInfo(RemoteInfo remoteInfo) {
        request().setRemoteInfo(remoteInfo);
        return this;
    }
}
