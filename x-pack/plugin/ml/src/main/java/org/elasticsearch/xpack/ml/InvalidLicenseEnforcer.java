/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

public class InvalidLicenseEnforcer {

    private static final Logger logger = LogManager.getLogger(InvalidLicenseEnforcer.class);

    private final ThreadPool threadPool;
    private final XPackLicenseState licenseState;
    private final DatafeedManager datafeedManager;
    private final AutodetectProcessManager autodetectProcessManager;

    InvalidLicenseEnforcer(XPackLicenseState licenseState, ThreadPool threadPool,
                           DatafeedManager datafeedManager, AutodetectProcessManager autodetectProcessManager) {
        this.threadPool = threadPool;
        this.licenseState = licenseState;
        this.datafeedManager = datafeedManager;
        this.autodetectProcessManager = autodetectProcessManager;
        licenseState.addListener(this::closeJobsAndDatafeedsIfLicenseExpired);
    }

    private void closeJobsAndDatafeedsIfLicenseExpired() {
        if (licenseState.isMachineLearningAllowed() == false) {
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
