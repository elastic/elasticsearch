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

package org.elasticsearch.client.node;

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
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.TransportMoreLikeThisAction;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.terms.TermsRequest;
import org.elasticsearch.action.terms.TermsResponse;
import org.elasticsearch.action.terms.TransportTermsAction;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (shay.banon)
 */
public class NodeClient extends AbstractComponent implements InternalClient {

    private final ThreadPool threadPool;

    private final NodeAdminClient admin;

    private final TransportIndexAction indexAction;

    private final TransportDeleteAction deleteAction;

    private final TransportDeleteByQueryAction deleteByQueryAction;

    private final TransportGetAction getAction;

    private final TransportCountAction countAction;

    private final TransportSearchAction searchAction;

    private final TransportSearchScrollAction searchScrollAction;

    private final TransportTermsAction termsAction;

    private final TransportMoreLikeThisAction moreLikeThisAction;

    @Inject public NodeClient(Settings settings, ThreadPool threadPool, NodeAdminClient admin,
                              TransportIndexAction indexAction, TransportDeleteAction deleteAction,
                              TransportDeleteByQueryAction deleteByQueryAction, TransportGetAction getAction, TransportCountAction countAction,
                              TransportSearchAction searchAction, TransportSearchScrollAction searchScrollAction,
                              TransportTermsAction termsAction, TransportMoreLikeThisAction moreLikeThisAction) {
        super(settings);
        this.threadPool = threadPool;
        this.admin = admin;
        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.deleteByQueryAction = deleteByQueryAction;
        this.getAction = getAction;
        this.countAction = countAction;
        this.searchAction = searchAction;
        this.searchScrollAction = searchScrollAction;
        this.termsAction = termsAction;
        this.moreLikeThisAction = moreLikeThisAction;
    }

    @Override public ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override public void close() {
        // nothing really to do
    }

    @Override public AdminClient admin() {
        return this.admin;
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request) {
        return indexAction.execute(request);
    }

    @Override public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        indexAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return deleteAction.execute(request);
    }

    @Override public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        deleteAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return deleteByQueryAction.execute(request);
    }

    @Override public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        deleteByQueryAction.execute(request, listener);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request) {
        return getAction.execute(request);
    }

    @Override public void get(GetRequest request, ActionListener<GetResponse> listener) {
        getAction.execute(request, listener);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request) {
        return countAction.execute(request);
    }

    @Override public void count(CountRequest request, ActionListener<CountResponse> listener) {
        countAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request) {
        return searchAction.execute(request);
    }

    @Override public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        searchAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return searchScrollAction.execute(request);
    }

    @Override public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        searchScrollAction.execute(request, listener);
    }

    @Override public ActionFuture<TermsResponse> terms(TermsRequest request) {
        return termsAction.execute(request);
    }

    @Override public void terms(TermsRequest request, ActionListener<TermsResponse> listener) {
        termsAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        return moreLikeThisAction.execute(request);
    }

    @Override public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        moreLikeThisAction.execute(request, listener);
    }
}
