/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.rest.action.ingest.IngestRestFilter;

import java.util.function.BiFunction;

/**
 * Registry for processor factories
 * @see Processor.Factory
 */
public class IngestModule extends AbstractModule {

    private final ProcessorsRegistry processorsRegistry;

    public IngestModule() {
        this.processorsRegistry = new ProcessorsRegistry();
    }

    @Override
    protected void configure() {
        binder().bind(IngestRestFilter.class).asEagerSingleton();
        bind(ProcessorsRegistry.class).toInstance(processorsRegistry);
        binder().bind(IngestBootstrapper.class).asEagerSingleton();
    }

    /**
     * Adds a processor factory under a specific type name.
     */
    public void addProcessor(String type, BiFunction<Environment, TemplateService, Processor.Factory<?>> processorFactoryProvider) {
        processorsRegistry.addProcessor(type, processorFactoryProvider);
    }
}
