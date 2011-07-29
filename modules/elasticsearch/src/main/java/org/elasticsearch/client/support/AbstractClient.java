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

import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.action.count.CountRequestBuilder;
import org.elasticsearch.client.action.delete.DeleteRequestBuilder;
import org.elasticsearch.client.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.client.action.get.GetRequestBuilder;
import org.elasticsearch.client.action.get.MultiGetRequestBuilder;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.client.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.client.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.client.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractClient implements InternalClient {

    @Override public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, null);
    }

    @Override public IndexRequestBuilder prepareIndex(String index, String type) {
        return prepareIndex(index, type, null);
    }

    @Override public IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return prepareIndex().setIndex(index).setType(type).setId(id);
    }

    @Override public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, null);
    }

    @Override public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return prepareDelete().setIndex(index).setType(type).setId(id);
    }

    @Override public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this);
    }

    @Override public DeleteByQueryRequestBuilder prepareDeleteByQuery(String... indices) {
        return new DeleteByQueryRequestBuilder(this).setIndices(indices);
    }

    @Override public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, null);
    }

    @Override public GetRequestBuilder prepareGet(String index, String type, String id) {
        return prepareGet().setIndex(index).setType(type).setId(id);
    }

    @Override public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this);
    }

    @Override public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this).setIndices(indices);
    }

    @Override public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, scrollId);
    }

    @Override public CountRequestBuilder prepareCount(String... indices) {
        return new CountRequestBuilder(this).setIndices(indices);
    }

    @Override public MoreLikeThisRequestBuilder prepareMoreLikeThis(String index, String type, String id) {
        return new MoreLikeThisRequestBuilder(this, index, type, id);
    }

    @Override public PercolateRequestBuilder preparePercolate(String index, String type) {
        return new PercolateRequestBuilder(this, index, type);
    }
}
