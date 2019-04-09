/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

public class InvalidLicenseEnforcer implements LicenseStateListener {

    private static final Logger logger = LogManager.getLogger(InvalidLicenseEnforcer.class);

    private final ThreadPool threadPool;
    private final XPackLicenseState licenseState;
    private final DatafeedManager datafeedManager;
    private final AutodetectProcessManager autodetectProcessManager;

    private volatile boolean licenseStateListenerRegistered;

    InvalidLicenseEnforcer(XPackLicenseState licenseState, ThreadPool threadPool,
                           DatafeedManager datafeedManager, AutodetectProcessManager autodetectProcessManager) {
        this.threadPool = threadPool;
        this.licenseState = licenseState;
        this.datafeedManager = datafeedManager;
        this.autodetectProcessManager = autodetectProcessManager;
    }

    void listenForLicenseStateChanges() {
        /*
         * Registering this as a listener can not be done in the constructor because otherwise it would be unsafe publication of this. That
         * is, it would expose this to another thread before the constructor had finished. Therefore, we have a dedicated method to register
         * the listener that is invoked after the constructor has returned.
         */
        assert licenseStateListenerRegistered == false;
        licenseState.addListener(this);
        licenseStateListenerRegistered = true;
    }

    @Override
    public void licenseStateChanged() {
        assert licenseStateListenerRegistered;
        if (licenseState.isMachineLearningAllowed() == false) {
            // if the license has expired, close jobs and datafeeds
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("cannot close all jobs", e);
                }

                @Override
                protected void doRun() throws Exception {
                    datafeedManager.stopAllDatafeedsOnThisNode("invalid license");
                    autodetectProcessManager.closeAllJobsOnThisNode("invalid license");
                }
            });
        }
    }

}
