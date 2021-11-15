/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.plugins.TracingPlugin;

public class APMTracer extends AbstractLifecycleComponent implements TracingPlugin.Tracer {

    private static final Logger logger = LogManager.getLogger(APMTracer.class);

    public APMTracer() {}

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public void trace(String something) {
        logger.info("tracing {}", something);
    }
}
