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

package org.elasticsearch.groovy.client

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.count.CountRequest
import org.elasticsearch.action.count.CountResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.mlt.MoreLikeThisRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.terms.TermsRequest
import org.elasticsearch.action.terms.TermsResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.internal.InternalClient
import org.elasticsearch.groovy.client.action.GActionFuture
import org.elasticsearch.groovy.util.xcontent.GXContentBuilder
import org.elasticsearch.util.xcontent.XContentType

/**
 * @author kimchy (shay.banon)
 */
class GClient {

    static {
        IndexRequest.metaClass.setSource = {Closure c ->
            delegate.source(new GXContentBuilder().buildAsBytes(c, indexContentType))
        }
        IndexRequest.metaClass.source = {Closure c ->
            delegate.source(new GXContentBuilder().buildAsBytes(c, indexContentType))
        }

        DeleteByQueryRequest.metaClass.setQuery = {Closure c ->
            delegate.query(new GXContentBuilder().buildAsBytes(c, contentType))
        }
        DeleteByQueryRequest.metaClass.query = {Closure c ->
            delegate.query(new GXContentBuilder().buildAsBytes(c, contentType))
        }

        CountRequest.metaClass.setQuery = {Closure c ->
            delegate.query(new GXContentBuilder().buildAsBytes(c, contentType))
        }
        CountRequest.metaClass.query = {Closure c ->
            delegate.query(new GXContentBuilder().buildAsBytes(c, contentType))
        }

        SearchRequest.metaClass.setSource = {Closure c ->
            delegate.source(new GXContentBuilder().buildAsBytes(c, contentType))
        }
        SearchRequest.metaClass.source = {Closure c ->
            delegate.source(new GXContentBuilder().buildAsBytes(c, contentType))
        }
        SearchRequest.metaClass.setExtraSource = {Closure c ->
            delegate.extraSource(new GXContentBuilder().buildAsBytes(c, contentType))
        }
        SearchRequest.metaClass.extraSource = {Closure c ->
            delegate.extraSource(new GXContentBuilder().buildAsBytes(c, contentType))
        }

        MoreLikeThisRequest.metaClass.setSearchSource = {Closure c ->
            delegate.searchSource(new GXContentBuilder().buildAsBytes(c, contentType))
        }
        MoreLikeThisRequest.metaClass.searchSource = {Closure c ->
            delegate.searchSource(new GXContentBuilder().buildAsBytes(c, contentType))
        }
    }

    public static XContentType contentType = XContentType.XSON;

    public static XContentType indexContentType = XContentType.JSON;

    final Client client;

    int resolveStrategy = Closure.DELEGATE_FIRST

    private final InternalClient internalClient

    final GAdminClient admin;

    def GClient(client) {
        this.client = client;
        this.internalClient = client;

        this.admin = new GAdminClient(this)
    }

    GActionFuture<IndexResponse> index(Closure c) {
        IndexRequest request = new IndexRequest()
        c.setDelegate request
        c.resolveStrategy = resolveStrategy
        c.call()
        index(request)
    }

    GActionFuture<IndexResponse> index(IndexRequest request) {
        GActionFuture<IndexResponse> future = new GActionFuture<IndexResponse>(internalClient.threadPool(), request);
        client.index(request, future)
        return future
    }

    void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        client.index(request, listener)
    }

    GActionFuture<GetResponse> get(Closure c) {
        GetRequest request = new GetRequest()
        c.setDelegate request
        c.resolveStrategy = resolveStrategy
        c.call()
        get(request)
    }

    GActionFuture<GetResponse> get(GetRequest request) {
        GActionFuture<GetResponse> future = new GActionFuture<GetResponse>(internalClient.threadPool(), request);
        client.get(request, future)
        return future
    }

    void get(GetRequest request, ActionListener<GetResponse> listener) {
        client.get(request, listener)
    }

    GActionFuture<DeleteResponse> delete(Closure c) {
        DeleteRequest request = new DeleteRequest()
        c.resolveStrategy = resolveStrategy
        c.setDelegate request
        c.call()
        delete(request)
    }

    GActionFuture<DeleteResponse> delete(DeleteRequest request) {
        GActionFuture<DeleteResponse> future = new GActionFuture<DeleteResponse>(internalClient.threadPool(), request);
        client.delete(request, future)
        return future
    }

    void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        client.delete(request, listener)
    }

    GActionFuture<DeleteByQueryResponse> deleteByQuery(Closure c) {
        DeleteByQueryRequest request = new DeleteByQueryRequest()
        c.resolveStrategy = resolveStrategy
        c.setDelegate request
        c.call()
        deleteByQuery(request)
    }

    GActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        GActionFuture<DeleteByQueryResponse> future = new GActionFuture<DeleteByQueryResponse>(internalClient.threadPool(), request);
        client.deleteByQuery(request, future)
        return future
    }

    void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        client.deleteByQuery(request, listener)
    }

    GActionFuture<CountResponse> count(Closure c) {
        CountRequest request = new CountRequest()
        c.resolveStrategy = resolveStrategy
        c.setDelegate request
        c.call()
        count(request)
    }

    GActionFuture<CountResponse> count(CountRequest request) {
        GActionFuture<CountResponse> future = new GActionFuture<CountResponse>(internalClient.threadPool(), request);
        client.count(request, future)
        return future
    }

    void count(CountRequest request, ActionListener<CountResponse> listener) {
        client.count(request, listener)
    }

    GActionFuture<SearchResponse> search(Closure c) {
        SearchRequest request = new SearchRequest()
        c.resolveStrategy = resolveStrategy
        c.setDelegate request
        c.call()
        search(request)
    }

    GActionFuture<SearchResponse> search(SearchRequest request) {
        GActionFuture<SearchResponse> future = new GActionFuture<SearchResponse>(internalClient.threadPool(), request);
        client.search(request, future)
        return future
    }

    void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        client.search(request, listener)
    }

    GActionFuture<TermsResponse> terms(Closure c) {
        TermsRequest request = new TermsRequest()
        c.resolveStrategy = resolveStrategy
        c.setDelegate request
        c.call()
        terms(request)
    }

    GActionFuture<TermsResponse> terms(TermsRequest request) {
        GActionFuture<TermsResponse> future = new GActionFuture<TermsResponse>(internalClient.threadPool(), request);
        client.terms(request, future)
        return future
    }

    void terms(TermsRequest request, ActionListener<TermsResponse> listener) {
        client.terms(request, listener)
    }

    GActionFuture<SearchResponse> moreLikeThis(Closure c) {
        MoreLikeThisRequest request = new MoreLikeThisRequest()
        c.resolveStrategy = resolveStrategy
        c.setDelegate request
        c.call()
        moreLikeThis(request)
    }

    GActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        GActionFuture<SearchResponse> future = new GActionFuture<SearchResponse>(internalClient.threadPool(), request);
        client.moreLikeThis(request, future)
        return future
    }

    void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        client(request, listener)
    }
}
