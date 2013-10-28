/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.monitor;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.marvel.monitor.exporter.ESExporter;
import org.elasticsearch.marvel.monitor.exporter.StatsExporter;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InternalIndicesService;
import org.elasticsearch.node.service.NodeService;

import java.util.Collection;

public class StatsExportersService extends AbstractLifecycleComponent<StatsExportersService> {

    private final InternalIndicesService indicesService;
    private final NodeService nodeService;
    private final ClusterService clusterService;
    private final Client client;

    private volatile ExportingWorker exp;
    private volatile Thread thread;
    private final TimeValue interval;

    private Collection<StatsExporter> exporters;

    @Inject
    public StatsExportersService(Settings settings, IndicesService indicesService,
                                 NodeService nodeService, ClusterService clusterService,
                                 Client client,
                                 Discovery discovery) {
        super(settings);
        this.indicesService = (InternalIndicesService) indicesService;
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        this.interval = componentSettings.getAsTime("interval", TimeValue.timeValueSeconds(5));
        this.client = client;

        StatsExporter esExporter = new ESExporter(settings.getComponentSettings(ESExporter.class), discovery);
        this.exporters = ImmutableSet.of(esExporter);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        for (StatsExporter e : exporters)
            e.start();

        this.exp = new ExportingWorker();
        this.thread = new Thread(exp, EsExecutors.threadName(settings, "monitor"));
        this.thread.setDaemon(true);
        this.thread.start();
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        this.exp.closed = true;
        this.thread.interrupt();
        for (StatsExporter e : exporters)
            e.stop();
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        for (StatsExporter e : exporters)
            e.close();
    }

    class ExportingWorker implements Runnable {

        volatile boolean closed;

        @Override
        public void run() {
            while (!closed) {
                // sleep first to allow node to complete initialization before collecting the first start
                try {
                    Thread.sleep(interval.millis());
                } catch (InterruptedException e) {
                    // ignore, if closed, good....
                }

                // do the actual export..., go over the actual exporters list and...
                try {
                    logger.debug("Collecting node stats");
                    NodeStats nodeStats = nodeService.stats();

                    logger.debug("Exporting node stats");
                    for (StatsExporter e : exporters) {
                        try {
                            e.exportNodeStats(nodeStats);
                        } catch (Throwable t) {
                            logger.error("StatsExporter [{}] has thrown an exception:", t, e.name());
                        }
                    }

                    logger.debug("Collecting shard stats");
                    ShardStats[] shardStatsArray = indicesService.shardStats(CommonStatsFlags.ALL);

                    logger.debug("Exporting shards stats");
                    for (StatsExporter e : exporters) {
                        try {
                            e.exportShardStats(shardStatsArray);
                        } catch (Throwable t) {
                            logger.error("StatsExporter [{}] has thrown an exception:", t, e.name());
                        }
                    }


                    if (clusterService.state().nodes().localNodeMaster()) {
                        logger.debug("local node is master, exporting aggregated stats");
                        IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats().all().get();
                        for (StatsExporter e : exporters) {
                            try {
                                e.exportIndicesStats(indicesStatsResponse);
                            } catch (Throwable t) {
                                logger.error("StatsExporter [{}] has thrown an exception:", t, e.name());
                            }


                        }

                    }


                } catch (Throwable t) {
                    logger.error("Background thread had an uncaught exception:", t);
                }

            }
        }
    }
}
