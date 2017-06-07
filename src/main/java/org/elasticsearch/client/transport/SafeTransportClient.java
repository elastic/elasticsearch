/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.client.transport;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.support.InternalSafeTransportClient;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.CachedStreams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * The safe transport client allows to create a client that is not part of the cluster, but simply connects to one
 * or more nodes directly by adding their respective addresses using {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
 * <p/>
 * <p>The safe transport client important modules used is the {@link org.elasticsearch.transport.TransportModule} which is
 * started in client mode (only connects, no bind).</p>
 * <p>
 * In multithreaded environments, the concurrency level of the transport client 
 * should be safely manageable.
 * The "safe" transport client is aware of the number of currently active action listeners.
 * If asynchronous actions are invoked, an action listener count is incremented. 
 * When responses arrive, the action listener count is decremented.
 * The maximum number of active action listeners is 30 per default. 
 * When responses do not arrive afer a specified timeout, the timeouts are counted.
 * A fatal error is issued if a maximum number of timeouts is exceeded.
 * </p>
 */
public class SafeTransportClient extends AbstractClient {

    private final Injector injector;

    private final Settings settings;

    private final Environment environment;


    private final TransportClientNodesService nodesService;

    private final InternalSafeTransportClient internalClient;


    /**
     * Constructs a new transport client with settings loaded either from the classpath or the file system (the
     * <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public SafeTransportClient() throws ElasticSearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    /**
     * Constructs a new transport client with explicit settings and settings loaded either from the classpath or the file
     * system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public SafeTransportClient(Settings settings) {
        this(settings, true);
    }

    /**
     * Constructs a new transport client with explicit settings and settings loaded either from the classpath or the file
     * system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public SafeTransportClient(Settings.Builder settings) {
        this(settings.build(), true);
    }

    /**
     * Constructs a new transport client with the provided settings and the ability to control if settings will
     * be loaded from the classpath / file system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with
     * <tt>config/</tt>).
     *
     * @param settings           The explicit settings.
     * @param loadConfigSettings <tt>true</tt> if settings should be loaded from the classpath/file system.
     * @throws ElasticSearchException
     */
    public SafeTransportClient(Settings.Builder settings, boolean loadConfigSettings) throws ElasticSearchException {
        this(settings.build(), loadConfigSettings);
    }

    /**
     * Constructs a new transport client with the provided settings and the ability to control if settings will
     * be loaded from the classpath / file system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with
     * <tt>config/</tt>).
     *
     * @param pSettings          The explicit settings.
     * @param loadConfigSettings <tt>true</tt> if settings should be loaded from the classpath/file system.
     * @throws ElasticSearchException
     */
    public SafeTransportClient(Settings pSettings, boolean loadConfigSettings) throws ElasticSearchException {
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(pSettings, loadConfigSettings);
        this.settings = settingsBuilder().put(tuple.v1())
                .put("network.server", false)
                .put("node.client", true)
                .build();
        this.environment = tuple.v2();

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new EnvironmentModule(environment));
        modules.add(new SettingsModule(settings));
        modules.add(new NetworkModule());
        modules.add(new ClusterNameModule(settings));
        modules.add(new ThreadPoolModule(settings));
        modules.add(new TransportSearchModule());
        modules.add(new TransportModule(settings));
        modules.add(new ActionModule(true));
        modules.add(new ClientTransportModule());

        injector = modules.createInjector();

        injector.getInstance(TransportService.class).start();

        nodesService = injector.getInstance(TransportClientNodesService.class);
        internalClient = injector.getInstance(InternalSafeTransportClient.class);
    }
    
    

    /**
     * Returns the current registered transport addresses to use (added using
     * {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
     */
    public ImmutableList<TransportAddress> transportAddresses() {
        return nodesService.transportAddresses();
    }

    /**
     * Returns the current connected transport nodes that this client will use.
     * <p/>
     * <p>The nodes include all the nodes that are currently alive based on the transport
     * addresses provided.
     */
    public ImmutableList<DiscoveryNode> connectedNodes() {
        return nodesService.connectedNodes();
    }

    /**
     * Returns the listed nodes in the transport client (ones added to it).
     */
    public ImmutableList<DiscoveryNode> listedNodes() {
        return nodesService.listedNodes();
    }

    /**
     * Adds a transport address that will be used to connect to.
     * <p/>
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p/>
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public SafeTransportClient addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Adds a list of transport addresses that will be used to connect to.
     * <p/>
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p/>
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public SafeTransportClient addTransportAddresses(TransportAddress... transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Removes a transport address from the list of transport addresses that are used to connect to.
     */
    public SafeTransportClient removeTransportAddress(TransportAddress transportAddress) {
        nodesService.removeTransportAddress(transportAddress);
        return this;
    }
    
    /**
     * Set the maximum number of active listeners who are active in parallel.
     * Default is 30.
     */
    public SafeTransportClient setMaximumActiveListeners(int num) {
        internalClient.setMaximumActiveListeners(num);
        return this;
    }

    /**
     * Set the time to wait before continue with a new active listener, even if there
     * is the maximum number of listeners active. Default is 60000L.
     */
    public SafeTransportClient setMillisBeforeContinue(long millis) {
        internalClient.setMillisBeforeContinue(millis);
        return this;
    }
    
    /**
     * Set the maximum number of timeouts when waiting for a free active listener slot
     * before raising a fatal exception. Default is 10.
     */
    public SafeTransportClient setMaximumTotalTimeouts(int num) {
        internalClient.setMaximumTotalTimeouts(num);
        return this;
    }
    
    /**
     * Closes the client.
     */
    @Override
    public void close() {
        internalClient.close(); // wait for all outstanding action listeners
        injector.getInstance(TransportClientNodesService.class).close();
        injector.getInstance(TransportService.class).close();
        try {
            injector.getInstance(MonitorService.class).close();
        } catch (Exception e) {
            // ignore, might not be bounded
        }

        injector.getInstance(ThreadPool.class).shutdown();
        try {
            injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            injector.getInstance(ThreadPool.class).shutdownNow();
        } catch (Exception e) {
            // ignore
        }

        CacheRecycler.clear();
        CachedStreams.clear();
        ThreadLocals.clearReferencesThreadLocals();
    }

    @Override
    public ThreadPool threadPool() {
        return internalClient.threadPool();
    }

    @Override
    public AdminClient admin() {
        return internalClient.admin();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        return internalClient.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        internalClient.execute(action, request, listener);
    }

    @Override
    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return internalClient.index(request);
    }

    @Override
    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        internalClient.index(request, listener);
    }

    @Override
    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return internalClient.update(request);
    }

    @Override
    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        internalClient.update(request, listener);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return internalClient.delete(request);
    }

    @Override
    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        internalClient.delete(request, listener);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return internalClient.bulk(request);
    }

    @Override
    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        internalClient.bulk(request, listener);
    }

    @Override
    public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return internalClient.deleteByQuery(request);
    }

    @Override
    public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        internalClient.deleteByQuery(request, listener);
    }

    @Override
    public ActionFuture<GetResponse> get(GetRequest request) {
        return internalClient.get(request);
    }

    @Override
    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        internalClient.get(request, listener);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return internalClient.multiGet(request);
    }

    @Override
    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        internalClient.multiGet(request, listener);
    }

    @Override
    public ActionFuture<CountResponse> count(CountRequest request) {
        return internalClient.count(request);
    }

    @Override
    public void count(CountRequest request, ActionListener<CountResponse> listener) {
        internalClient.count(request, listener);
    }

    @Override
    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return internalClient.search(request);
    }

    @Override
    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        internalClient.search(request, listener);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return internalClient.searchScroll(request);
    }

    @Override
    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        internalClient.searchScroll(request, listener);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return internalClient.multiSearch(request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        internalClient.multiSearch(request, listener);
    }

    @Override
    public ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        return internalClient.moreLikeThis(request);
    }

    @Override
    public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        internalClient.moreLikeThis(request, listener);
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return internalClient.percolate(request);
    }

    @Override
    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        internalClient.percolate(request, listener);
    }
}
