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

package org.elasticsearch.client.transport.support;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.action.count.ClientTransportCountAction;
import org.elasticsearch.client.transport.action.delete.ClientTransportDeleteAction;
import org.elasticsearch.client.transport.action.deletebyquery.ClientTransportDeleteByQueryAction;
import org.elasticsearch.client.transport.action.get.ClientTransportGetAction;
import org.elasticsearch.client.transport.action.index.ClientTransportIndexAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchScrollAction;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalTransportClient extends AbstractComponent implements Client {

    private final TransportClientNodesService nodesService;

    private final InternalTransportAdminClient adminClient;

    private final ClientTransportIndexAction indexAction;

    private final ClientTransportDeleteAction deleteAction;

    private final ClientTransportGetAction getAction;

    private final ClientTransportDeleteByQueryAction deleteByQueryAction;

    private final ClientTransportCountAction countAction;

    private final ClientTransportSearchAction searchAction;

    private final ClientTransportSearchScrollAction searchScrollAction;

    @Inject public InternalTransportClient(Settings settings, TransportClientNodesService nodesService, InternalTransportAdminClient adminClient,
                                           ClientTransportIndexAction indexAction, ClientTransportDeleteAction deleteAction, ClientTransportGetAction getAction,
                                           ClientTransportDeleteByQueryAction deleteByQueryAction, ClientTransportCountAction countAction,
                                           ClientTransportSearchAction searchAction, ClientTransportSearchScrollAction searchScrollAction) {
        super(settings);
        this.nodesService = nodesService;
        this.adminClient = adminClient;

        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.getAction = getAction;
        this.deleteByQueryAction = deleteByQueryAction;
        this.countAction = countAction;
        this.searchAction = searchAction;
        this.searchScrollAction = searchScrollAction;
    }

    @Override public void close() {
        // nothing to do here
    }

    @Override public AdminClient admin() {
        return adminClient;
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request) {
        return indexAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request, ActionListener<IndexResponse> listener) {
        return indexAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execIndex(IndexRequest request, ActionListener<IndexResponse> listener) {
        indexAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return deleteAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        return deleteAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execDelete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        deleteAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return deleteByQueryAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        return deleteByQueryAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execDeleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        deleteByQueryAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request) {
        return getAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request, ActionListener<GetResponse> listener) {
        return getAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execGet(GetRequest request, ActionListener<GetResponse> listener) {
        getAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request) {
        return countAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request, ActionListener<CountResponse> listener) {
        return countAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execCount(CountRequest request, ActionListener<CountResponse> listener) {
        countAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request) {
        return searchAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request, ActionListener<SearchResponse> listener) {
        return searchAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execSearch(SearchRequest request, ActionListener<SearchResponse> listener) {
        searchAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return searchScrollAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        return searchScrollAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execSearchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        searchScrollAction.execute(nodesService.randomNode(), request, listener);
    }
}
