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

package org.elasticsearch.river;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.cluster.RiverClusterChangedEvent;
import org.elasticsearch.river.cluster.RiverClusterService;
import org.elasticsearch.river.cluster.RiverClusterState;
import org.elasticsearch.river.cluster.RiverClusterStateListener;
import org.elasticsearch.river.routing.RiverRouting;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author kimchy (shay.banon)
 */
public class RiversService extends AbstractLifecycleComponent<RiversService> {

    private final String riverIndexName;

    private Client client;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final Injector injector;

    private final Map<RiverName, Injector> riversInjectors = Maps.newHashMap();

    private volatile ImmutableMap<RiverName, River> rivers = ImmutableMap.of();

    @Inject public RiversService(Settings settings, Client client, ThreadPool threadPool, ClusterService clusterService, RiverClusterService riverClusterService, Injector injector) {
        super(settings);
        this.riverIndexName = RiverIndexName.Conf.indexName(settings);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.injector = injector;
        riverClusterService.add(new ApplyRivers());
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
        ImmutableSet<RiverName> indices = ImmutableSet.copyOf(this.rivers.keySet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final RiverName riverName : indices) {
            threadPool.cached().execute(new Runnable() {
                @Override public void run() {
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

    @Override protected void doClose() throws ElasticSearchException {
    }

    public synchronized River createRiver(RiverName riverName, Map<String, Object> settings) throws ElasticSearchException {
        if (riversInjectors.containsKey(riverName)) {
            throw new RiverException(riverName, "river already exists");
        }

        logger.debug("creating river [{}][{}]", riverName.type(), riverName.name());

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new RiverNameModule(riverName));
        modules.add(new RiverModule(riverName, settings, this.settings));

        Injector indexInjector = modules.createChildInjector(injector);
        riversInjectors.put(riverName, indexInjector);
        River river = indexInjector.getInstance(River.class);
        rivers = MapBuilder.newMapBuilder(rivers).put(riverName, river).immutableMap();


        // we need this start so there can be operations done (like creating an index) which can't be
        // done on create since Guice can't create two concurrent child injectors
        river.start();
        return river;
    }

    public synchronized void closeRiver(RiverName riverName) throws ElasticSearchException {
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

        Injectors.close(injector);
    }

    private class ApplyRivers implements RiverClusterStateListener {
        @Override public void riverClusterChanged(RiverClusterChangedEvent event) {
            DiscoveryNode localNode = clusterService.localNode();
            RiverClusterState state = event.state();

            // first, go over and delete ones that either don't exists or are not allocated
            for (RiverName riverName : rivers.keySet()) {
                RiverRouting routing = state.routing().routing(riverName);
                if (routing == null || !localNode.equals(routing.node())) {
                    // not routed at all, and not allocated here, clean it (we delete the relevant ones before)
                    closeRiver(riverName);
                }
            }

            for (final RiverRouting routing : state.routing()) {
                // not allocated
                if (routing.node() == null) {
                    continue;
                }
                // only apply changes to the local node
                if (!routing.node().equals(localNode)) {
                    continue;
                }
                // if its already created, ignore it
                if (rivers.containsKey(routing.riverName())) {
                    continue;
                }
                client.prepareGet(riverIndexName, routing.riverName().name(), "_meta").execute(new ActionListener<GetResponse>() {
                    @Override public void onResponse(GetResponse getResponse) {
                        if (!rivers.containsKey(routing.riverName())) {
                            if (getResponse.exists()) {
                                // only create the river if it exists, otherwise, the indexing meta data has not been visible yet...
                                createRiver(routing.riverName(), getResponse.sourceAsMap());
                            }
                        }
                    }

                    @Override public void onFailure(Throwable e) {
                        logger.warn("failed to get _meta from [{}]/[{}]", e, routing.riverName().type(), routing.riverName().name());
                    }
                });
            }
        }
    }
}
