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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldstats.FieldStatsAction;
import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.action.fieldstats.FieldStatsRequestBuilder;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.MultiPercolateAction;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequestBuilder;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequestBuilder;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * A client provides a one stop interface for performing actions/operations against the cluster.
 * <p>
 * All operations performed are asynchronous by nature. Each action/operation has two flavors, the first
 * simply returns an {@link org.elasticsearch.action.ActionFuture}, while the second accepts an
 * {@link org.elasticsearch.action.ActionListener}.
 * <p>
 * A client can either be retrieved from a {@link org.elasticsearch.node.Node} started, or connected remotely
 * to one or more nodes using {@link org.elasticsearch.client.transport.TransportClient}.
 *
 * @see org.elasticsearch.node.Node#client()
 * @see org.elasticsearch.client.transport.TransportClient
 */
public interface Client extends ElasticsearchClient, Releasable {

    Setting<String> CLIENT_TYPE_SETTING_S = new Setting<>("client.type", "node", (s) -> {
        switch (s) {
            case "node":
            case "transport":
                return s;
            default:
                throw new IllegalArgumentException("Can't parse [client.type] must be one of [node, transport]");
        }
    }, Property.NodeScope);

    /**
     * The admin client that can be used to perform administrative operations.
     */
    AdminClient admin();


    /**
     * Index a JSON source associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request The index request
     * @return The result future
     * @see Requests#indexRequest(String)
     */
    default ActionFuture<IndexResponse> index(IndexRequest request) {
        return execute(IndexAction.INSTANCE, request);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request  The index request
     * @param listener A listener to be notified with a result
     * @see Requests#indexRequest(String)
     */
    default void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        execute(IndexAction.INSTANCE, request, listener);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     */
    default IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, IndexAction.INSTANCE, null);
    }

    /**
     * Updates a document based on a script.
     *
     * @param request The update request
     * @return The result future
     */
    default ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return execute(UpdateAction.INSTANCE, request);
    }

    /**
     * Updates a document based on a script.
     *
     * @param request  The update request
     * @param listener A listener to be notified with a result
     */
    default void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        execute(UpdateAction.INSTANCE, request, listener);
    }

    /**
     * Updates a document based on a script.
     */
    default UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, null, null, null);
    }

    /**
     * Updates a document based on a script.
     */
    default UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, index, type, id);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @param type  The type to index the document to
     */
    default IndexRequestBuilder prepareIndex(String index, String type) {
        return prepareIndex(index, type, null);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @param type  The type to index the document to
     * @param id    The id of the document
     */
    default IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return prepareIndex().setIndex(index).setType(type).setId(id);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param request The delete request
     * @return The result future
     * @see Requests#deleteRequest(String)
     */
    default ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return execute(DeleteAction.INSTANCE, request);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param request  The delete request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteRequest(String)
     */
    default void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        execute(DeleteAction.INSTANCE, request, listener);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     */
    default DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, DeleteAction.INSTANCE, null);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param index The index to delete the document from
     * @param type  The type of the document to delete
     * @param id    The id of the document to delete
     */
    default DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return prepareDelete().setIndex(index).setType(type).setId(id);
    }

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request The bulk request
     * @return The result future
     * @see org.elasticsearch.client.Requests#bulkRequest()
     */
    default ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return execute(BulkAction.INSTANCE, request);
    }

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request  The bulk request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#bulkRequest()
     */
    default void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        execute(BulkAction.INSTANCE, request, listener);
    }

    /**
     * Executes a bulk of index / delete operations.
     */
    default BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this, BulkAction.INSTANCE);
    }

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request The get request
     * @return The result future
     * @see Requests#getRequest(String)
     */
    default ActionFuture<GetResponse> get(GetRequest request) {
        return execute(GetAction.INSTANCE, request);
    }

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request  The get request
     * @param listener A listener to be notified with a result
     * @see Requests#getRequest(String)
     */
    default void get(GetRequest request, ActionListener<GetResponse> listener){
        execute(GetAction.INSTANCE, request, listener);
    }

    /**
     * Gets the document that was indexed from an index with a type and id.
     */
    default GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, GetAction.INSTANCE, null);
    }

    /**
     * Gets the document that was indexed from an index with a type (optional) and id.
     */
    default GetRequestBuilder prepareGet(String index, @Nullable String type, String id) {
        return prepareGet().setIndex(index).setType(type).setId(id);
    }

    /**
     * Multi get documents.
     */
    default ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return execute(MultiGetAction.INSTANCE, request);
    }

    /**
     * Multi get documents.
     */
    default void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        execute(MultiGetAction.INSTANCE, request, listener);
    }

    /**
     * Multi get documents.
     */
    default MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this, MultiGetAction.INSTANCE);
    }

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param request The search request
     * @return The result future
     * @see Requests#searchRequest(String...)
     */
    default ActionFuture<SearchResponse> search(SearchRequest request) {
        return execute(SearchAction.INSTANCE, request);
    }

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param request  The search request
     * @param listener A listener to be notified of the result
     * @see Requests#searchRequest(String...)
     */
    default void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        execute(SearchAction.INSTANCE, request, listener);
    }

    /**
     * Search across one or more indices and one or more types with a query.
     */
    default SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this, SearchAction.INSTANCE).setIndices(indices);
    }

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request The search scroll request
     * @return The result future
     * @see Requests#searchScrollRequest(String)
     */
    default ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return execute(SearchScrollAction.INSTANCE, request);
    }

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request  The search scroll request
     * @param listener A listener to be notified of the result
     * @see Requests#searchScrollRequest(String)
     */
    default void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        execute(SearchScrollAction.INSTANCE, request, listener);
    }

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     */
    default SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, SearchScrollAction.INSTANCE, scrollId);
    }

    /**
     * Performs multiple search requests.
     */
    default ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return execute(MultiSearchAction.INSTANCE, request);
    }

    /**
     * Performs multiple search requests.
     */
    default void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        execute(MultiSearchAction.INSTANCE, request, listener);
    }

    /**
     * Performs multiple search requests.
     */
    default MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this, MultiSearchAction.INSTANCE);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    default ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request) {
        return execute(TermVectorsAction.INSTANCE, request);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     */
    default void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        execute(TermVectorsAction.INSTANCE, request, listener);
    }

    /**
     * Builder for the term vector request.
     */
    default TermVectorsRequestBuilder prepareTermVectors() {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE);
    }

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    default TermVectorsRequestBuilder prepareTermVectors(String index, String type, String id) {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE, index, type, id);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    @Deprecated
    default ActionFuture<TermVectorsResponse> termVector(TermVectorsRequest request) {
        return termVectors(request);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     */
    @Deprecated
    default void termVector(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        termVectors(request, listener);
    }

    /**
     * Builder for the term vector request.
     */
    @Deprecated
    default TermVectorsRequestBuilder prepareTermVector() {
        return prepareTermVectors();
    }

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    @Deprecated
    default TermVectorsRequestBuilder prepareTermVector(String index, String type, String id) {
        return prepareTermVectors(index, type, id);
    }

    /**
     * Multi get term vectors.
     */
    default ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request) {
        return execute(MultiTermVectorsAction.INSTANCE, request);
    }

    /**
     * Multi get term vectors.
     */
    default void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener) {
        execute(MultiTermVectorsAction.INSTANCE, request, listener);
    }

    /**
     * Multi get term vectors.
     */
    default MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return new MultiTermVectorsRequestBuilder(this, MultiTermVectorsAction.INSTANCE);
    }

    /**
     * Percolates a request returning the matches documents.
     */
    default ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return execute(PercolateAction.INSTANCE, request);
    }

    /**
     * Percolates a request returning the matches documents.
     */
    default void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        execute(PercolateAction.INSTANCE, request, listener);
    }

    /**
     * Percolates a request returning the matches documents.
     */
    default PercolateRequestBuilder preparePercolate() {
        return new PercolateRequestBuilder(this, PercolateAction.INSTANCE);
    }

    /**
     * Performs multiple percolate requests.
     */
    default ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request) {
        return execute(MultiPercolateAction.INSTANCE, request);
    }

    /**
     * Performs multiple percolate requests.
     */
    default void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        execute(MultiPercolateAction.INSTANCE, request, listener);
    }

    /**
     * Performs multiple percolate requests.
     */
    default MultiPercolateRequestBuilder prepareMultiPercolate() {
        return new MultiPercolateRequestBuilder(this, MultiPercolateAction.INSTANCE);
    }

    /**
     * Computes a score explanation for the specified request.
     *
     * @param index The index this explain is targeted for
     * @param type  The type this explain is targeted for
     * @param id    The document identifier this explain is targeted for
     */
    default ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return new ExplainRequestBuilder(this, ExplainAction.INSTANCE, index, type, id);
    }

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request The request encapsulating the query and document identifier to compute a score explanation for
     */
    default ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return execute(ExplainAction.INSTANCE, request);
    }

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request  The request encapsulating the query and document identifier to compute a score explanation for
     * @param listener A listener to be notified of the result
     */
    default void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        execute(ExplainAction.INSTANCE, request, listener);
    }

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    default ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this, ClearScrollAction.INSTANCE);
    }

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    default ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return execute(ClearScrollAction.INSTANCE, request);
    }

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    default void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        execute(ClearScrollAction.INSTANCE, request, listener);
    }

    default FieldStatsRequestBuilder prepareFieldStats() {
        return new FieldStatsRequestBuilder(this, FieldStatsAction.INSTANCE);
    }

    default ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest request) {
        return execute(FieldStatsAction.INSTANCE, request);
    }

    default void fieldStats(FieldStatsRequest request, ActionListener<FieldStatsResponse> listener) {
        execute(FieldStatsAction.INSTANCE, request, listener);
    }

    /**
     * Returns this clients settings
     */
    Settings settings();

    /**
     * Returns a new lightweight Client that applies all given headers to each of the requests
     * issued from it.
     */
    Client filterWithHeader(Map<String, String> headers);
}
