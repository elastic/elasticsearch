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

package org.elasticsearch.client.support;

import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.mlt.MoreLikeThisAction;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvector.*;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;

/**
 *
 */
public abstract class AbstractClient implements InternalClient {

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(final Action<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ActionFuture<IndexResponse> index(final IndexRequest request) {
        return execute(IndexAction.INSTANCE, request);
    }

    @Override
    public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        execute(IndexAction.INSTANCE, request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, null);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type) {
        return prepareIndex(index, type, null);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return prepareIndex().setIndex(index).setType(type).setId(id);
    }

    @Override
    public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return execute(UpdateAction.INSTANCE, request);
    }

    @Override
    public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        execute(UpdateAction.INSTANCE, request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, null, null, null);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return new UpdateRequestBuilder(this, index, type, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return execute(DeleteAction.INSTANCE, request);
    }

    @Override
    public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        execute(DeleteAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, null);
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return prepareDelete().setIndex(index).setType(type).setId(id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return execute(BulkAction.INSTANCE, request);
    }

    @Override
    public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        execute(BulkAction.INSTANCE, request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this);
    }

    @Override
    public ActionFuture<DeleteByQueryResponse> deleteByQuery(final DeleteByQueryRequest request) {
        return execute(DeleteByQueryAction.INSTANCE, request);
    }

    @Override
    public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {
        execute(DeleteByQueryAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteByQueryRequestBuilder prepareDeleteByQuery(String... indices) {
        return new DeleteByQueryRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<GetResponse> get(final GetRequest request) {
        return execute(GetAction.INSTANCE, request);
    }

    @Override
    public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        execute(GetAction.INSTANCE, request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, null);
    }

    @Override
    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return prepareGet().setIndex(index).setType(type).setId(id);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return execute(MultiGetAction.INSTANCE, request);
    }

    @Override
    public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        execute(MultiGetAction.INSTANCE, request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this);
    }

    @Override
    public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return execute(SearchAction.INSTANCE, request);
    }

    @Override
    public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        execute(SearchAction.INSTANCE, request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return execute(SearchScrollAction.INSTANCE, request);
    }

    @Override
    public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        execute(SearchScrollAction.INSTANCE, request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, scrollId);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return execute(MultiSearchAction.INSTANCE, request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        execute(MultiSearchAction.INSTANCE, request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this);
    }

    @Override
    public ActionFuture<CountResponse> count(final CountRequest request) {
        return execute(CountAction.INSTANCE, request);
    }

    @Override
    public void count(final CountRequest request, final ActionListener<CountResponse> listener) {
        execute(CountAction.INSTANCE, request, listener);
    }

    @Override
    public CountRequestBuilder prepareCount(String... indices) {
        return new CountRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<SuggestResponse> suggest(final SuggestRequest request) {
        return execute(SuggestAction.INSTANCE, request);
    }

    @Override
    public void suggest(final SuggestRequest request, final ActionListener<SuggestResponse> listener) {
        execute(SuggestAction.INSTANCE, request, listener);
    }

    @Override
    public SuggestRequestBuilder prepareSuggest(String... indices) {
        return new SuggestRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<SearchResponse> moreLikeThis(final MoreLikeThisRequest request) {
        return execute(MoreLikeThisAction.INSTANCE, request);
    }

    @Override
    public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        execute(MoreLikeThisAction.INSTANCE, request, listener);
    }

    @Override
    public MoreLikeThisRequestBuilder prepareMoreLikeThis(String index, String type, String id) {
        return new MoreLikeThisRequestBuilder(this, index, type, id);
    }

    @Override
    public ActionFuture<TermVectorResponse> termVector(final TermVectorRequest request) {
        return execute(TermVectorAction.INSTANCE, request);
    }

    @Override
    public void termVector(final TermVectorRequest request, final ActionListener<TermVectorResponse> listener) {
        execute(TermVectorAction.INSTANCE, request, listener);
    }

    @Override
    public TermVectorRequestBuilder prepareTermVector(String index, String type, String id) {
        return new TermVectorRequestBuilder(this, index, type, id);
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        return execute(MultiTermVectorsAction.INSTANCE, request);
    }

    @Override
    public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        execute(MultiTermVectorsAction.INSTANCE, request, listener);
    }

    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return new MultiTermVectorsRequestBuilder(this);
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(final PercolateRequest request) {
        return execute(PercolateAction.INSTANCE, request);
    }

    @Override
    public void percolate(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        execute(PercolateAction.INSTANCE, request, listener);
    }

    @Override
    public PercolateRequestBuilder preparePercolate() {
        return new PercolateRequestBuilder(this);
    }

    @Override
    public MultiPercolateRequestBuilder prepareMultiPercolate() {
        return new MultiPercolateRequestBuilder(this);
    }

    @Override
    public void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        execute(MultiPercolateAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request) {
        return execute(MultiPercolateAction.INSTANCE, request);
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return new ExplainRequestBuilder(this, index, type, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return execute(ExplainAction.INSTANCE, request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        execute(ExplainAction.INSTANCE, request, listener);
    }

    @Override
    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        execute(ClearScrollAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return execute(ClearScrollAction.INSTANCE, request);
    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this);
    }
}
