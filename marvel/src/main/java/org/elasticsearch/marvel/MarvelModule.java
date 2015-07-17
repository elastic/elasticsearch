/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.collector.CollectorModule;
import org.elasticsearch.marvel.agent.exporter.ExporterModule;
import org.elasticsearch.marvel.license.LicenseModule;

public class MarvelModule extends AbstractModule implements SpawnModules {

    @Override
    protected void configure() {
        bind(AgentService.class).in(Scopes.SINGLETON);
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new LicenseModule(), new CollectorModule(), new ExporterModule());
    }
}
