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

import org.elasticsearch.ElasticSearchException;
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
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.terms.TermsRequest;
import org.elasticsearch.action.terms.TermsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.action.count.ClientTransportCountAction;
import org.elasticsearch.client.transport.action.delete.ClientTransportDeleteAction;
import org.elasticsearch.client.transport.action.deletebyquery.ClientTransportDeleteByQueryAction;
import org.elasticsearch.client.transport.action.get.ClientTransportGetAction;
import org.elasticsearch.client.transport.action.index.ClientTransportIndexAction;
import org.elasticsearch.client.transport.action.mlt.ClientTransportMoreLikeThisAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchAction;
import org.elasticsearch.client.transport.action.search.ClientTransportSearchScrollAction;
import org.elasticsearch.client.transport.action.terms.ClientTransportTermsAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalTransportClient extends AbstractComponent implements InternalClient {

    private final ThreadPool threadPool;

    private final TransportClientNodesService nodesService;

    private final InternalTransportAdminClient adminClient;

    private final ClientTransportIndexAction indexAction;

    private final ClientTransportDeleteAction deleteAction;

    private final ClientTransportGetAction getAction;

    private final ClientTransportDeleteByQueryAction deleteByQueryAction;

    private final ClientTransportCountAction countAction;

    private final ClientTransportSearchAction searchAction;

    private final ClientTransportSearchScrollAction searchScrollAction;

    private final ClientTransportTermsAction termsAction;

    private final ClientTransportMoreLikeThisAction moreLikeThisAction;

    @Inject public InternalTransportClient(Settings settings, ThreadPool threadPool,
                                           TransportClientNodesService nodesService, InternalTransportAdminClient adminClient,
                                           ClientTransportIndexAction indexAction, ClientTransportDeleteAction deleteAction, ClientTransportGetAction getAction,
                                           ClientTransportDeleteByQueryAction deleteByQueryAction, ClientTransportCountAction countAction,
                                           ClientTransportSearchAction searchAction, ClientTransportSearchScrollAction searchScrollAction,
                                           ClientTransportTermsAction termsAction, ClientTransportMoreLikeThisAction moreLikeThisAction) {
        super(settings);
        this.threadPool = threadPool;
        this.nodesService = nodesService;
        this.adminClient = adminClient;

        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.getAction = getAction;
        this.deleteByQueryAction = deleteByQueryAction;
        this.countAction = countAction;
        this.searchAction = searchAction;
        this.searchScrollAction = searchScrollAction;
        this.termsAction = termsAction;
        this.moreLikeThisAction = moreLikeThisAction;
    }

    @Override public void close() {
        // nothing to do here
    }

    @Override public ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override public AdminClient admin() {
        return adminClient;
    }

    @Override public ActionFuture<IndexResponse> index(final IndexRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<IndexResponse>>() {
            @Override public ActionFuture<IndexResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return indexAction.execute(node, request);
            }
        });
    }

    @Override public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(DiscoveryNode node) throws ElasticSearchException {
                indexAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<DeleteResponse>>() {
            @Override public ActionFuture<DeleteResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return deleteAction.execute(node, request);
            }
        });
    }

    @Override public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(DiscoveryNode node) throws ElasticSearchException {
                deleteAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(final DeleteByQueryRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<DeleteByQueryResponse>>() {
            @Override public ActionFuture<DeleteByQueryResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return deleteByQueryAction.execute(node, request);
            }
        });
    }

    @Override public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(DiscoveryNode node) throws ElasticSearchException {
                deleteByQueryAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<GetResponse> get(final GetRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<GetResponse>>() {
            @Override public ActionFuture<GetResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return getAction.execute(node, request);
            }
        });
    }

    @Override public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(DiscoveryNode node) throws ElasticSearchException {
                getAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<CountResponse> count(final CountRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<CountResponse>>() {
            @Override public ActionFuture<CountResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return countAction.execute(node, request);
            }
        });
    }

    @Override public void count(final CountRequest request, final ActionListener<CountResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(DiscoveryNode node) throws ElasticSearchException {
                countAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<SearchResponse>>() {
            @Override public ActionFuture<SearchResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return searchAction.execute(node, request);
            }
        });
    }

    @Override public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(DiscoveryNode node) throws ElasticSearchException {
                searchAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<SearchResponse>>() {
            @Override public ActionFuture<SearchResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return searchScrollAction.execute(node, request);
            }
        });
    }

    @Override public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Object>() {
            @Override public Object doWithNode(DiscoveryNode node) throws ElasticSearchException {
                searchScrollAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<TermsResponse> terms(final TermsRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<TermsResponse>>() {
            @Override public ActionFuture<TermsResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return termsAction.execute(node, request);
            }
        });
    }

    @Override public void terms(final TermsRequest request, final ActionListener<TermsResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<Void>>() {
            @Override public ActionFuture<Void> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                termsAction.execute(node, request, listener);
                return null;
            }
        });
    }

    @Override public ActionFuture<SearchResponse> moreLikeThis(final MoreLikeThisRequest request) {
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<SearchResponse>>() {
            @Override public ActionFuture<SearchResponse> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return moreLikeThisAction.execute(node, request);
            }
        });
    }

    @Override public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        nodesService.execute(new TransportClientNodesService.NodeCallback<Void>() {
            @Override public Void doWithNode(DiscoveryNode node) throws ElasticSearchException {
                moreLikeThisAction.execute(node, request, listener);
                return null;
            }
        });
    }
}
