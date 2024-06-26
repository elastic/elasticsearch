/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.ingest.geoip.enterprise.EnterpriseGeoIpTaskParams;

@SuppressWarnings("rawtypes")
public class LicensedEnterpriseGeoIpDownloaderTaskExecutor extends PersistentTasksExecutor
    implements
        ClusterStateListener {
    private static final String TASK_NAME = "enterprise-geoip-downloader";
    private static final Logger logger = LogManager.getLogger(LicensedEnterpriseGeoIpDownloaderTaskExecutor.class);

    private final PersistentTasksService persistentTasksService;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private final LicensedFeature.Momentary feature;

    protected LicensedEnterpriseGeoIpDownloaderTaskExecutor(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState
    ) {
        super(TASK_NAME, threadPool.generic());
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.clusterService = clusterService;
        this.feature = LicensedFeature.momentary(null, XPackField.ENTERPRISE_GEOIP_DOWNLOADER, License.OperationMode.PLATINUM);
        this.licenseState = licenseState;
    }

//    @Override
//    protected void nodeOperation(AllocatedPersistentTask task, EnterpriseGeoIpTaskParams params, PersistentTaskState state) {
//        logger.info("Running enterprise downloader");
//    }

    @UpdateForV9 // use MINUS_ONE once that means no timeout
    private static final TimeValue MASTER_TIMEOUT = TimeValue.MAX_VALUE;

    public void init() {
        clusterService.addListener(this);
//        if (feature.check(licenseState)) {
//            logger.info("Have a valid license, starting enterprise downloader");
//            startTask();
//        } else {
//            logger.info("Do not have a valid license, not starting enterprise downloader");
//        }
    }

    private void startTask() {
        persistentTasksService.sendStartRequest(
            TASK_NAME,
            TASK_NAME,
            new EnterpriseGeoIpTaskParams(),
            MASTER_TIMEOUT,
            ActionListener.wrap(r -> logger.debug("Started geoip downloader task"), e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create geoip downloader task", e);
                }
            })
        );
    }

    private void stopTask() {
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = ActionListener.wrap(
            r -> logger.debug("Stopped geoip downloader task"),
            e -> {
                Throwable t = e instanceof RemoteTransportException ? ExceptionsHelper.unwrapCause(e) : e;
                if (t instanceof ResourceNotFoundException == false) {
                    logger.error("failed to remove geoip downloader task", e);
                }
            }
        );
        persistentTasksService.sendRemoveRequest(TASK_NAME, MASTER_TIMEOUT, listener);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (feature.check(licenseState)) {
            logger.info("License is now valid, starting enterprise geoip downloader");
            startTask();
        } else {
            logger.info("License is no longer valid, stopping enterprise geoip downloader");
            stopTask();
        }
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, PersistentTaskParams params, PersistentTaskState state) {
        logger.info("Running enterprise downloader");
    }
}
