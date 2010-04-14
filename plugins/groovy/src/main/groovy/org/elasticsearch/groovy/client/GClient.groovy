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
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.internal.InternalClient
import org.elasticsearch.groovy.client.action.GActionFuture
import org.elasticsearch.groovy.util.json.JsonBuilder

/**
 * @author kimchy (shay.banon)
 */
class GClient {

    static {
        IndexRequest.metaClass.setSource = {Closure c ->
            delegate.source(new JsonBuilder().buildAsBytes(c))
        }
        IndexRequest.metaClass.source = {Closure c ->
            delegate.source(new JsonBuilder().buildAsBytes(c))
        }

        DeleteByQueryRequest.metaClass.setQuery = {Closure c ->
            delegate.query(new JsonBuilder().buildAsBytes(c))
        }
        DeleteByQueryRequest.metaClass.query = {Closure c ->
            delegate.query(new JsonBuilder().buildAsBytes(c))
        }
    }

    final Client client;

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
        c.resolveStrategy = Closure.DELEGATE_FIRST
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
        c.resolveStrategy = Closure.DELEGATE_FIRST
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
        c.resolveStrategy = Closure.DELEGATE_FIRST
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
        c.resolveStrategy = Closure.DELEGATE_FIRST
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
}
