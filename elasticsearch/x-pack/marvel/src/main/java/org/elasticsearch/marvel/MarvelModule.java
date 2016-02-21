/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.cleaner.CleanerService;

public class MarvelModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MarvelSettings.class).asEagerSingleton();
        bind(AgentService.class).asEagerSingleton();
        bind(CleanerService.class).asEagerSingleton();
    }
}
