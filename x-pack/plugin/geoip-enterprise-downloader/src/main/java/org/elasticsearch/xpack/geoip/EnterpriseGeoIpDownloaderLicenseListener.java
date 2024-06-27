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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.ingest.geoip.enterprise.EnterpriseGeoIpTaskParams;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.core.XPackField;

import static org.elasticsearch.ingest.geoip.enterprise.EnterpriseGeoIpTaskParams.ENTERPRISE_GEOIP_DOWNLOADER;

public class EnterpriseGeoIpDownloaderLicenseListener implements LicenseStateListener {
    private static final Logger logger = LogManager.getLogger(EnterpriseGeoIpDownloaderLicenseListener.class);

    private final PersistentTasksService persistentTasksService;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private final LicensedFeature.Momentary feature;

    protected EnterpriseGeoIpDownloaderLicenseListener(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState
    ) {
        this.persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        this.clusterService = clusterService;
        // TODO maybe a static feature is more conventional? i dunno!
        this.feature = LicensedFeature.momentary(null, XPackField.ENTERPRISE_GEOIP_DOWNLOADER, License.OperationMode.PLATINUM);
        this.licenseState = licenseState;
    }

    @UpdateForV9 // use MINUS_ONE once that means no timeout
    private static final TimeValue MASTER_TIMEOUT = TimeValue.MAX_VALUE;
    private volatile boolean licenseStateListenerRegistered;

    public void init() {
        // TODO alternatively we could have the equivalent of this code in EnterpriseDownloaderPlugin itself... :shrug:
        listenForLicenseStateChanges();
    }

    void listenForLicenseStateChanges() {
        assert licenseStateListenerRegistered == false;
        licenseState.addListener(this);
        licenseStateListenerRegistered = true;
    }

    @Override
    public void licenseStateChanged() {
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            // we should only start/stop task from single node, master is the best as it will go through it anyway
            return;
        }
        assert licenseStateListenerRegistered;
        // TODO remove dev-time only logging
        // TODO we send a start even if it was already started...
        // TODO we send a stop even if there was never a start...
        if (feature.checkWithoutTracking(licenseState)) {
            logger.debug("License is valid, ensuring enterprise geoip downloader is started");
            startTask();
        } else {
            logger.debug("License is not valid, ensuring enterprise geoip downloader is stopped");
            stopTask();
        }
    }

    private void startTask() {
        persistentTasksService.sendStartRequest(
            ENTERPRISE_GEOIP_DOWNLOADER,
            ENTERPRISE_GEOIP_DOWNLOADER,
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
        persistentTasksService.sendRemoveRequest(ENTERPRISE_GEOIP_DOWNLOADER, MASTER_TIMEOUT, listener);
    }
}
