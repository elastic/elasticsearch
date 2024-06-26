/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.ingest.geoip.enterprise.EnterpriseGeoIpTaskParams.ENTERPRISE_GEOIP_DOWNLOADER;

public class EnterpriseGeoIpDownloaderTaskExecutor extends PersistentTasksExecutor<PersistentTaskParams> {
    private static final Logger logger = LogManager.getLogger(GeoIpDownloader.class);

    private final PersistentTasksService persistentTasksService;

    protected EnterpriseGeoIpDownloaderTaskExecutor(
        Client client,
        HttpClient httpClient,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        super(ENTERPRISE_GEOIP_DOWNLOADER, threadPool.generic());
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, PersistentTaskParams params, PersistentTaskState state) {
        logger.info("Running enterprise downloader");
    }
}
