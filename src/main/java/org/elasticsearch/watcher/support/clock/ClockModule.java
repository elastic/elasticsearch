/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.clock;

import org.elasticsearch.common.inject.AbstractModule;

/**
 *
 */
public class ClockModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Clock.class).toInstance(SystemClock.INSTANCE);
    }

}
