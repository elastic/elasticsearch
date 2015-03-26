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

package org.elasticsearch.client.transport;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
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
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.support.InternalTransportClient;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * The transport client allows to create a client that is not part of the cluster, but simply connects to one
 * or more nodes directly by adding their respective addresses using {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
 * <p/>
 * <p>The transport client important modules used is the {@link org.elasticsearch.transport.TransportModule} which is
 * started in client mode (only connects, no bind).
 */
public class TransportClient extends AbstractClient {

    private static final String CLIENT_TYPE = "transport";

    final Injector injector;

    private final Settings settings;
    private final Environment environment;
    private final PluginsService pluginsService;
    private final TransportClientNodesService nodesService;
    private final InternalTransportClient internalClient;

    /**
     * Constructs a new transport client with settings loaded either from the classpath or the file system (the
     * <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public TransportClient() throws ElasticsearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    /**
     * Constructs a new transport client with explicit settings and settings loaded either from the classpath or the file
     * system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public TransportClient(Settings settings) {
        this(settings, true);
    }

    /**
     * Constructs a new transport client with explicit settings and settings loaded either from the classpath or the file
     * system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public TransportClient(Settings.Builder settings) {
        this(settings.build(), true);
    }

    /**
     * Constructs a new transport client with the provided settings and the ability to control if settings will
     * be loaded from the classpath / file system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with
     * <tt>config/</tt>).
     *
     * @param settings           The explicit settings.
     * @param loadConfigSettings <tt>true</tt> if settings should be loaded from the classpath/file system.
     * @throws org.elasticsearch.ElasticsearchException
     */
    public TransportClient(Settings.Builder settings, boolean loadConfigSettings) throws ElasticsearchException {
        this(settings.build(), loadConfigSettings);
    }

    /**
     * Constructs a new transport client with the provided settings and the ability to control if settings will
     * be loaded from the classpath / file system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with
     * <tt>config/</tt>).
     *
     * @param pSettings          The explicit settings.
     * @param loadConfigSettings <tt>true</tt> if settings should be loaded from the classpath/file system.
     * @throws org.elasticsearch.ElasticsearchException
     */
    public TransportClient(Settings pSettings, boolean loadConfigSettings) throws ElasticsearchException {
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(pSettings, loadConfigSettings);
        Settings settings = settingsBuilder()
                .put(NettyTransport.PING_SCHEDULE, "5s") // enable by default the transport schedule ping interval
                .put(tuple.v1())
                .put("network.server", false)
                .put("node.client", true)
                .put(CLIENT_TYPE_SETTING, CLIENT_TYPE)
                .build();
        this.environment = tuple.v2();

        this.pluginsService = new PluginsService(settings, tuple.v2());
        this.settings = pluginsService.updatedSettings();

        Version version = Version.CURRENT;

        CompressorFactory.configure(this.settings);

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new Version.Module(version));
        modules.add(new PluginsModule(this.settings, pluginsService));
        modules.add(new EnvironmentModule(environment));
        modules.add(new SettingsModule(this.settings));
        modules.add(new NetworkModule());
        modules.add(new ClusterNameModule(this.settings));
        modules.add(new ThreadPoolModule(this.settings));
        modules.add(new TransportSearchModule());
        modules.add(new TransportModule(this.settings));
        modules.add(new ActionModule(true));
        modules.add(new ClientTransportModule());
        modules.add(new CircuitBreakerModule(this.settings));

        injector = modules.createInjector();

        injector.getInstance(TransportService.class).start();

        nodesService = injector.getInstance(TransportClientNodesService.class);
        internalClient = injector.getInstance(InternalTransportClient.class);
    }

    TransportClientNodesService nodeService() {
        return nodesService;
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
     * The list of filtered nodes that were not connected to, for example, due to
     * mismatch in cluster name.
     */
    public ImmutableList<DiscoveryNode> filteredNodes() {
        return nodesService.filteredNodes();
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
    public TransportClient addTransportAddress(TransportAddress transportAddress) {
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
    public TransportClient addTransportAddresses(TransportAddress... transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Removes a transport address from the list of transport addresses that are used to connect to.
     */
    public TransportClient removeTransportAddress(TransportAddress transportAddress) {
        nodesService.removeTransportAddress(transportAddress);
        return this;
    }

    /**
     * Closes the client.
     */
    @Override
    public void close() {
        injector.getInstance(TransportClientNodesService.class).close();
        injector.getInstance(TransportService.class).close();
        try {
            injector.getInstance(MonitorService.class).close();
        } catch (Exception e) {
            // ignore, might not be bounded
        }

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).close();
        }
        try {
            ThreadPool.terminate(injector.getInstance(ThreadPool.class), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignore
        }

        injector.getInstance(PageCacheRecycler.class).close();
    }

    @Override
    public Settings settings() {
        return this.settings;
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
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, Client> action, Request request) {
        return internalClient.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(Action<Request, Response, RequestBuilder, Client> action, Request request, ActionListener<Response> listener) {
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
    public ActionFuture<SuggestResponse> suggest(SuggestRequest request) {
        return internalClient.suggest(request);
    }

    @Override
    public void suggest(SuggestRequest request, ActionListener<SuggestResponse> listener) {
        internalClient.suggest(request, listener);
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
    public ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request) {
        return internalClient.termVectors(request);
    }

    @Override
    public void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        internalClient.termVectors(request, listener);
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        return internalClient.multiTermVectors(request);
    }

    @Override
    public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        internalClient.multiTermVectors(request, listener);
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return internalClient.percolate(request);
    }

    @Override
    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        internalClient.percolate(request, listener);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return internalClient.explain(request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        internalClient.explain(request, listener);
    }
}
