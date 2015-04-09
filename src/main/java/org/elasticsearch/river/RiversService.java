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

package org.elasticsearch.river;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.river.cluster.RiverClusterChangedEvent;
import org.elasticsearch.river.cluster.RiverClusterService;
import org.elasticsearch.river.cluster.RiverClusterState;
import org.elasticsearch.river.cluster.RiverClusterStateListener;
import org.elasticsearch.river.routing.RiverRouting;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 *
 */
public class RiversService extends AbstractLifecycleComponent<RiversService> {

    private final String riverIndexName;

    private Client client;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final RiversTypesRegistry typesRegistry;

    private final Injector injector;

    private final Map<RiverName, Injector> riversInjectors = Maps.newHashMap();

    private volatile ImmutableMap<RiverName, River> rivers = ImmutableMap.of();

    @Inject
    public RiversService(Settings settings, Client client, ThreadPool threadPool, ClusterService clusterService, RiversTypesRegistry typesRegistry, RiverClusterService riverClusterService, Injector injector) {
        super(settings);
        this.riverIndexName = RiverIndexName.Conf.indexName(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.typesRegistry = typesRegistry;
        this.injector = injector;
        riverClusterService.add(new ApplyRivers());
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        ImmutableSet<RiverName> indices = ImmutableSet.copyOf(this.rivers.keySet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final RiverName riverName : indices) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        closeRiver(riverName);
                    } catch (Exception e) {
                        logger.warn("failed to delete river on stop [{}]/[{}]", e, riverName.type(), riverName.name());
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    public synchronized void createRiver(RiverName riverName, Map<String, Object> settings) throws ElasticsearchException {
        if (riversInjectors.containsKey(riverName)) {
            logger.warn("ignoring river [{}][{}] creation, already exists", riverName.type(), riverName.name());
            return;
        }

        logger.info("rivers have been deprecated. Read https://www.elastic.co/blog/deprecating_rivers");
        logger.debug("creating river [{}][{}]", riverName.type(), riverName.name());

        try {
            ModulesBuilder modules = new ModulesBuilder();
            modules.add(new RiverNameModule(riverName));
            modules.add(new RiverModule(riverName, settings, this.settings, typesRegistry));
            modules.add(new RiversPluginsModule(this.settings, injector.getInstance(PluginsService.class)));

            Injector indexInjector = modules.createChildInjector(injector);
            riversInjectors.put(riverName, indexInjector);
            River river = indexInjector.getInstance(River.class);
            rivers = MapBuilder.newMapBuilder(rivers).put(riverName, river).immutableMap();


            // we need this start so there can be operations done (like creating an index) which can't be
            // done on create since Guice can't create two concurrent child injectors
            river.start();

            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            builder.startObject("node");
            builder.field("id", clusterService.localNode().id());
            builder.field("name", clusterService.localNode().name());
            builder.field("transport_address", clusterService.localNode().address().toString());
            builder.endObject();

            builder.endObject();


            client.prepareIndex(riverIndexName, riverName.name(), "_status")
                    .setConsistencyLevel(WriteConsistencyLevel.ONE)
                    .setSource(builder).execute().actionGet();
        } catch (Exception e) {
            logger.warn("failed to create river [{}][{}]", e, riverName.type(), riverName.name());

            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                builder.field("error", ExceptionsHelper.detailedMessage(e));

                builder.startObject("node");
                builder.field("id", clusterService.localNode().id());
                builder.field("name", clusterService.localNode().name());
                builder.field("transport_address", clusterService.localNode().address().toString());
                builder.endObject();
                builder.endObject();

                client.prepareIndex(riverIndexName, riverName.name(), "_status")
                        .setConsistencyLevel(WriteConsistencyLevel.ONE)
                        .setSource(builder).execute().actionGet();
            } catch (Exception e1) {
                logger.warn("failed to write failed status for river creation", e);
            }
        }
    }

    public synchronized void closeRiver(RiverName riverName) throws ElasticsearchException {
        Injector riverInjector;
        River river;
        synchronized (this) {
            riverInjector = riversInjectors.remove(riverName);
            if (riverInjector == null) {
                throw new RiverException(riverName, "missing");
            }
            logger.debug("closing river [{}][{}]", riverName.type(), riverName.name());

            Map<RiverName, River> tmpMap = Maps.newHashMap(rivers);
            river = tmpMap.remove(riverName);
            rivers = ImmutableMap.copyOf(tmpMap);
        }

        river.close();
    }

    private class ApplyRivers implements RiverClusterStateListener {
        @Override
        public void riverClusterChanged(RiverClusterChangedEvent event) {
            DiscoveryNode localNode = clusterService.localNode();
            RiverClusterState state = event.state();

            // first, go over and delete ones that either don't exists or are not allocated
            for (final RiverName riverName : rivers.keySet()) {
                RiverRouting routing = state.routing().routing(riverName);
                if (routing == null || !localNode.equals(routing.node())) {
                    // not routed at all, and not allocated here, clean it (we delete the relevant ones before)
                    closeRiver(riverName);
                }
            }

            for (final RiverRouting routing : state.routing()) {
                // not allocated
                if (routing.node() == null) {
                    logger.trace("river {} has no routing node", routing.riverName().getName());
                    continue;
                }
                // only apply changes to the local node
                if (!routing.node().equals(localNode)) {
                    logger.trace("river {} belongs to node {}", routing.riverName().getName(), routing.node());
                    continue;
                }
                // if its already created, ignore it
                if (rivers.containsKey(routing.riverName())) {
                    logger.trace("river {} is already allocated", routing.riverName().getName());
                    continue;
                }
                prepareGetMetaDocument(routing.riverName().name()).execute(new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse getResponse) {
                        if (!rivers.containsKey(routing.riverName())) {
                            if (getResponse.isExists()) {
                                // only create the river if it exists, otherwise, the indexing meta data has not been visible yet...
                                createRiver(routing.riverName(), getResponse.getSourceAsMap());
                            } else {
                                //this should never happen as we've just found the _meta document in RiversRouter
                                logger.warn("{}/{}/_meta document not found", riverIndexName, routing.riverName().getName());
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        // if its this is a failure that need to be retried, then do it
                        // this might happen if the state of the river index has not been propagated yet to this node, which
                        // should happen pretty fast since we managed to get the _meta in the RiversRouter
                        Throwable failure = ExceptionsHelper.unwrapCause(e);
                        if (isShardNotAvailableException(failure)) {
                            logger.debug("failed to get _meta from [{}]/[{}], retrying...", e, routing.riverName().type(), routing.riverName().name());
                            final ActionListener<GetResponse> listener = this;
                            try {
                                threadPool.schedule(TimeValue.timeValueSeconds(5), ThreadPool.Names.SAME, new Runnable() {
                                    @Override
                                    public void run() {
                                        prepareGetMetaDocument(routing.riverName().name()).execute(listener);
                                    }
                                });
                            } catch (EsRejectedExecutionException ex) {
                                logger.debug("Couldn't schedule river start retry, node might be shutting down", ex);
                            }
                        } else {
                            logger.warn("failed to get _meta from [{}]/[{}]", e, routing.riverName().type(), routing.riverName().name());
                        }
                    }
                });
            }
        }

        private GetRequestBuilder prepareGetMetaDocument(String riverName) {
            return client.prepareGet(riverIndexName, riverName, "_meta").setPreference("_primary").setListenerThreaded(true);
        }
    }
}
