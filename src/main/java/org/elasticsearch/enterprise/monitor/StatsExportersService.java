/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.enterprise.monitor;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.enterprise.monitor.exporter.ESExporter;
import org.elasticsearch.enterprise.monitor.exporter.StatsExporter;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InternalIndicesService;
import org.elasticsearch.node.service.NodeService;

import java.util.Collection;

public class StatsExportersService extends AbstractLifecycleComponent<StatsExportersService> {

    private final InternalIndicesService indicesService;
    private final NodeService nodeService;

    private volatile ExportingWorker exp;
    private volatile Thread thread;
    private final TimeValue interval;

    private Collection<StatsExporter> exporters;

    @Inject
    public StatsExportersService(Settings settings, IndicesService indicesService,
                                 NodeService nodeService, Discovery discovery) {
        super(settings);
        this.indicesService = (InternalIndicesService) indicesService;
        this.nodeService = nodeService;
        this.interval = componentSettings.getAsTime("interval", TimeValue.timeValueSeconds(5));

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
                // sleep first to allow node to complete initialization before collectiont the first start
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
                            logger.error("StatsExporter {} has thrown an exception:", t, e.name());
                        }
                    }

                    logger.debug("Collecting shard stats");
                    ShardStats[] shardStatsArray = indicesService.shardStats(CommonStatsFlags.ALL);

                    logger.debug("Exporting shards stats");
                    for (StatsExporter e : exporters) {
                        try {
                            e.exportShardStats(shardStatsArray);
                        } catch (Throwable t) {
                            logger.error("StatsExporter {} has thrown an exception:", t, e.name());
                        }
                    }
                } catch (Throwable t) {
                    logger.error("Background thread had an uncaught exception:", t);
                }

            }
        }
    }
}
