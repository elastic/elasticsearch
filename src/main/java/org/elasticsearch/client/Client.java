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

import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.action.fieldstats.FieldStatsRequestBuilder;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;

/**
 * A client provides a one stop interface for performing actions/operations against the cluster.
 * <p/>
 * <p>All operations performed are asynchronous by nature. Each action/operation has two flavors, the first
 * simply returns an {@link org.elasticsearch.action.ActionFuture}, while the second accepts an
 * {@link org.elasticsearch.action.ActionListener}.
 * <p/>
 * <p>A client can either be retrieved from a {@link org.elasticsearch.node.Node} started, or connected remotely
 * to one or more nodes using {@link org.elasticsearch.client.transport.TransportClient}.
 *
 * @see org.elasticsearch.node.Node#client()
 * @see org.elasticsearch.client.transport.TransportClient
 */
public interface Client extends ElasticsearchClient<Client>, Releasable {

    String CLIENT_TYPE_SETTING = "client.type";

    /**
     * The admin client that can be used to perform administrative operations.
     */
    AdminClient admin();


    /**
     * Index a JSON source associated with a given index and type.
     * <p/>
     * <p>The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request The index request
     * @return The result future
     * @see Requests#indexRequest(String)
     */
    ActionFuture<IndexResponse> index(IndexRequest request);

    /**
     * Index a document associated with a given index and type.
     * <p/>
     * <p>The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request  The index request
     * @param listener A listener to be notified with a result
     * @see Requests#indexRequest(String)
     */
    void index(IndexRequest request, ActionListener<IndexResponse> listener);

    /**
     * Index a document associated with a given index and type.
     * <p/>
     * <p>The id is optional, if it is not provided, one will be generated automatically.
     */
    IndexRequestBuilder prepareIndex();

    /**
     * Updates a document based on a script.
     *
     * @param request The update request
     * @return The result future
     */
    ActionFuture<UpdateResponse> update(UpdateRequest request);

    /**
     * Updates a document based on a script.
     *
     * @param request  The update request
     * @param listener A listener to be notified with a result
     */
    void update(UpdateRequest request, ActionListener<UpdateResponse> listener);

    /**
     * Updates a document based on a script.
     */
    UpdateRequestBuilder prepareUpdate();

    /**
     * Updates a document based on a script.
     */
    UpdateRequestBuilder prepareUpdate(String index, String type, String id);

    /**
     * Index a document associated with a given index and type.
     * <p/>
     * <p>The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @param type  The type to index the document to
     */
    IndexRequestBuilder prepareIndex(String index, String type);

    /**
     * Index a document associated with a given index and type.
     * <p/>
     * <p>The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @param type  The type to index the document to
     * @param id    The id of the document
     */
    IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id);

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param request The delete request
     * @return The result future
     * @see Requests#deleteRequest(String)
     */
    ActionFuture<DeleteResponse> delete(DeleteRequest request);

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param request  The delete request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteRequest(String)
     */
    void delete(DeleteRequest request, ActionListener<DeleteResponse> listener);

    /**
     * Deletes a document from the index based on the index, type and id.
     */
    DeleteRequestBuilder prepareDelete();

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param index The index to delete the document from
     * @param type  The type of the document to delete
     * @param id    The id of the document to delete
     */
    DeleteRequestBuilder prepareDelete(String index, String type, String id);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request The bulk request
     * @return The result future
     * @see org.elasticsearch.client.Requests#bulkRequest()
     */
    ActionFuture<BulkResponse> bulk(BulkRequest request);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request  The bulk request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#bulkRequest()
     */
    void bulk(BulkRequest request, ActionListener<BulkResponse> listener);

    /**
     * Executes a bulk of index / delete operations.
     */
    BulkRequestBuilder prepareBulk();

    /**
     * Deletes all documents from one or more indices based on a query.
     *
     * @param request The delete by query request
     * @return The result future
     * @see Requests#deleteByQueryRequest(String...)
     */
    ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request);

    /**
     * Deletes all documents from one or more indices based on a query.
     *
     * @param request  The delete by query request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteByQueryRequest(String...)
     */
    void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener);

    /**
     * Deletes all documents from one or more indices based on a query.
     */
    DeleteByQueryRequestBuilder prepareDeleteByQuery(String... indices);

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request The get request
     * @return The result future
     * @see Requests#getRequest(String)
     */
    ActionFuture<GetResponse> get(GetRequest request);

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request  The get request
     * @param listener A listener to be notified with a result
     * @see Requests#getRequest(String)
     */
    void get(GetRequest request, ActionListener<GetResponse> listener);

    /**
     * Gets the document that was indexed from an index with a type and id.
     */
    GetRequestBuilder prepareGet();

    /**
     * Gets the document that was indexed from an index with a type (optional) and id.
     */
    GetRequestBuilder prepareGet(String index, @Nullable String type, String id);


    /**
     * Put an indexed script
     */
    PutIndexedScriptRequestBuilder preparePutIndexedScript();

    /**
     * Put the indexed script
     * @param scriptLang
     * @param id
     * @param source
     * @return
     */
    PutIndexedScriptRequestBuilder preparePutIndexedScript(@Nullable String scriptLang, String id, String source);

    /**
     * delete an indexed script
     *
     * @param request
     * @param listener
     */
    void deleteIndexedScript(DeleteIndexedScriptRequest request, ActionListener<DeleteIndexedScriptResponse> listener);

    /**
     * Delete an indexed script
     *
     * @param request The put request
     * @return The result future
     */
    ActionFuture<DeleteIndexedScriptResponse> deleteIndexedScript(DeleteIndexedScriptRequest request);


    /**
     * Delete an indexed script
     */
    DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript();

    /**
     * Delete an indexed script
     * @param scriptLang
     * @param id
     * @return
     */
    DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(@Nullable String scriptLang, String id);

    /**
     * Put an indexed script
     *
     * @param request
     * @param listener
     */
    void putIndexedScript(PutIndexedScriptRequest request, ActionListener<PutIndexedScriptResponse> listener);

    /**
     * Put an indexed script
     *
     * @param request The put request
     * @return The result future
     */
    ActionFuture<PutIndexedScriptResponse> putIndexedScript(PutIndexedScriptRequest request);


    /**
     * Get an indexed script
     */
    GetIndexedScriptRequestBuilder prepareGetIndexedScript();

    /**
     * Get the indexed script
     * @param scriptLang
     * @param id
     * @return
     */
    GetIndexedScriptRequestBuilder prepareGetIndexedScript(@Nullable String scriptLang, String id);

    /**
     * Get an indexed script
     *
     * @param request
     * @param listener
     */
    void getIndexedScript(GetIndexedScriptRequest request, ActionListener<GetIndexedScriptResponse> listener);

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request The get request
     * @return The result future
     * @see Requests#getRequest(String)
     */
    ActionFuture<GetIndexedScriptResponse> getIndexedScript(GetIndexedScriptRequest request);


    /**
     * Multi get documents.
     */
    ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request);

    /**
     * Multi get documents.
     */
    void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener);

    /**
     * Multi get documents.
     */
    MultiGetRequestBuilder prepareMultiGet();

    /**
     * A count of all the documents matching a specific query.
     *
     * @param request The count request
     * @return The result future
     * @see Requests#countRequest(String...)
     */
    ActionFuture<CountResponse> count(CountRequest request);

    /**
     * A count of all the documents matching a specific query.
     *
     * @param request  The count request
     * @param listener A listener to be notified of the result
     * @see Requests#countRequest(String...)
     */
    void count(CountRequest request, ActionListener<CountResponse> listener);

    /**
     * A count of all the documents matching a specific query.
     */
    CountRequestBuilder prepareCount(String... indices);

    /**
     * Checks existence of any documents matching a specific query.
     *
     * @param request The exists request
     * @return The result future
     * @see Requests#existsRequest(String...)
     */
    ActionFuture<ExistsResponse> exists(ExistsRequest request);

    /**
     * Checks existence of any documents matching a specific query.
     *
     * @param request The exists request
     * @param listener A listener to be notified of the result
     * @see Requests#existsRequest(String...)
     */
    void exists(ExistsRequest request, ActionListener<ExistsResponse> listener);

    /**
     * Checks existence of any documents matching a specific query.
     */
    ExistsRequestBuilder prepareExists(String... indices);

    /**
     * Suggestion matching a specific phrase.
     *
     * @param request The suggest request
     * @return The result future
     * @see Requests#suggestRequest(String...)
     */
    ActionFuture<SuggestResponse> suggest(SuggestRequest request);

    /**
     * Suggestions matching a specific phrase.
     *
     * @param request  The suggest request
     * @param listener A listener to be notified of the result
     * @see Requests#suggestRequest(String...)
     */
    void suggest(SuggestRequest request, ActionListener<SuggestResponse> listener);

    /**
     * Suggestions matching a specific phrase.
     */
    SuggestRequestBuilder prepareSuggest(String... indices);

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param request The search request
     * @return The result future
     * @see Requests#searchRequest(String...)
     */
    ActionFuture<SearchResponse> search(SearchRequest request);

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param request  The search request
     * @param listener A listener to be notified of the result
     * @see Requests#searchRequest(String...)
     */
    void search(SearchRequest request, ActionListener<SearchResponse> listener);

    /**
     * Search across one or more indices and one or more types with a query.
     */
    SearchRequestBuilder prepareSearch(String... indices);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request The search scroll request
     * @return The result future
     * @see Requests#searchScrollRequest(String)
     */
    ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request  The search scroll request
     * @param listener A listener to be notified of the result
     * @see Requests#searchScrollRequest(String)
     */
    void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     */
    SearchScrollRequestBuilder prepareSearchScroll(String scrollId);

    /**
     * Performs multiple search requests.
     */
    ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request);

    /**
     * Performs multiple search requests.
     */
    void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener);

    /**
     * Performs multiple search requests.
     */
    MultiSearchRequestBuilder prepareMultiSearch();

    /**
     * A more like this action to search for documents that are "like" a specific document.
     *
     * @param request The more like this request
     * @return The response future
     */
    ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request);

    /**
     * A more like this action to search for documents that are "like" a specific document.
     *
     * @param request  The more like this request
     * @param listener A listener to be notified of the result
     */
    void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener);

    /**
     * A more like this action to search for documents that are "like" a specific document.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    MoreLikeThisRequestBuilder prepareMoreLikeThis(String index, String type, String id);

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request);

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener);

    /**
     * Builder for the term vector request.
     */
    TermVectorsRequestBuilder prepareTermVectors();

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    TermVectorsRequestBuilder prepareTermVectors(String index, String type, String id);

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    @Deprecated
    ActionFuture<TermVectorsResponse> termVector(TermVectorsRequest request);

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    @Deprecated
    void termVector(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener);

    /**
     * Builder for the term vector request.
     */
    @Deprecated
    TermVectorsRequestBuilder prepareTermVector();

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    @Deprecated
    TermVectorsRequestBuilder prepareTermVector(String index, String type, String id);

    /**
     * Multi get term vectors.
     */
    ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request);

    /**
     * Multi get term vectors.
     */
    void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener);

    /**
     * Multi get term vectors.
     */
    MultiTermVectorsRequestBuilder prepareMultiTermVectors();

    /**
     * Percolates a request returning the matches documents.
     */
    ActionFuture<PercolateResponse> percolate(PercolateRequest request);

    /**
     * Percolates a request returning the matches documents.
     */
    void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener);

    /**
     * Percolates a request returning the matches documents.
     */
    PercolateRequestBuilder preparePercolate();

    /**
     * Performs multiple percolate requests.
     */
    ActionFuture<MultiPercolateResponse> multiPercolate(MultiPercolateRequest request);

    /**
     * Performs multiple percolate requests.
     */
    void multiPercolate(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener);

    /**
     * Performs multiple percolate requests.
     */
    MultiPercolateRequestBuilder prepareMultiPercolate();

    /**
     * Computes a score explanation for the specified request.
     *
     * @param index The index this explain is targeted for
     * @param type  The type this explain is targeted for
     * @param id    The document identifier this explain is targeted for
     */
    ExplainRequestBuilder prepareExplain(String index, String type, String id);

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request The request encapsulating the query and document identifier to compute a score explanation for
     */
    ActionFuture<ExplainResponse> explain(ExplainRequest request);

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request  The request encapsulating the query and document identifier to compute a score explanation for
     * @param listener A listener to be notified of the result
     */
    void explain(ExplainRequest request, ActionListener<ExplainResponse> listener);

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    ClearScrollRequestBuilder prepareClearScroll();

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request);

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener);

    FieldStatsRequestBuilder prepareFieldStats();

    ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest request);

    void fieldStats(FieldStatsRequest request, ActionListener<FieldStatsResponse> listener);

    /**
     * Returns this clients settings
     */
    Settings settings();

}
