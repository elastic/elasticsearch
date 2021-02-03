/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cloud.gce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

public class GceModule extends AbstractModule {
    // pkg private so tests can override with mock
    static Class<? extends GceInstancesService> computeServiceImpl = GceInstancesServiceImpl.class;

    protected final Settings settings;
    protected final Logger logger = LogManager.getLogger(GceModule.class);

    public GceModule(Settings settings) {
        this.settings = settings;
    }

    public static Class<? extends GceInstancesService> getComputeServiceImpl() {
        return computeServiceImpl;
    }

    @Override
    protected void configure() {
        logger.debug("configure GceModule (bind compute service)");
        bind(GceInstancesService.class).to(computeServiceImpl).asEagerSingleton();
    }
}
