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

package org.elasticsearch.client.server;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class ServerClient extends AbstractComponent implements Client {

    private final ServerAdminClient admin;

    private final TransportIndexAction indexAction;

    private final TransportDeleteAction deleteAction;

    private final TransportDeleteByQueryAction deleteByQueryAction;

    private final TransportGetAction getAction;

    private final TransportCountAction countAction;

    private final TransportSearchAction searchAction;

    private final TransportSearchScrollAction searchScrollAction;

    @Inject public ServerClient(Settings settings, ServerAdminClient admin,
                                TransportIndexAction indexAction, TransportDeleteAction deleteAction,
                                TransportDeleteByQueryAction deleteByQueryAction, TransportGetAction getAction, TransportCountAction countAction,
                                TransportSearchAction searchAction, TransportSearchScrollAction searchScrollAction) {
        super(settings);
        this.admin = admin;
        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.deleteByQueryAction = deleteByQueryAction;
        this.getAction = getAction;
        this.countAction = countAction;
        this.searchAction = searchAction;
        this.searchScrollAction = searchScrollAction;
    }

    @Override public void close() {
        // nothing really to do
    }

    @Override public AdminClient admin() {
        return this.admin;
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request) {
        return indexAction.submit(request);
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request, ActionListener<IndexResponse> listener) {
        return indexAction.submit(request, listener);
    }

    @Override public void execIndex(IndexRequest request, ActionListener<IndexResponse> listener) {
        indexAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return deleteAction.submit(request);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        return deleteAction.submit(request, listener);
    }

    @Override public void execDelete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        deleteAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return deleteByQueryAction.submit(request);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        return deleteByQueryAction.submit(request, listener);
    }

    @Override public void execDeleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        deleteByQueryAction.execute(request, listener);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request) {
        return getAction.submit(request);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request, ActionListener<GetResponse> listener) {
        return getAction.submit(request, listener);
    }

    @Override public void execGet(GetRequest request, ActionListener<GetResponse> listener) {
        getAction.execute(request, listener);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request) {
        return countAction.submit(request);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request, ActionListener<CountResponse> listener) {
        return countAction.submit(request, listener);
    }

    @Override public void execCount(CountRequest request, ActionListener<CountResponse> listener) {
        countAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request) {
        return searchAction.submit(request);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request, ActionListener<SearchResponse> listener) {
        return searchAction.submit(request, listener);
    }

    @Override public void execSearch(SearchRequest request, ActionListener<SearchResponse> listener) {
        searchAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return searchScrollAction.submit(request);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        return searchScrollAction.submit(request, listener);
    }

    @Override public void execSearchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        searchScrollAction.execute(request, listener);
    }
}
