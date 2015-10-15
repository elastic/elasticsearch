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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.grok.GrokProcessor;
import org.elasticsearch.ingest.processor.simple.SimpleProcessor;
import org.elasticsearch.plugin.ingest.rest.IngestRestFilter;

import java.util.HashMap;
import java.util.Map;

public class IngestModule extends AbstractModule {

    private final Map<String, Class<? extends Processor.Builder.Factory>> processors = new HashMap<>();

    @Override
    protected void configure() {
        binder().bind(IngestRestFilter.class).asEagerSingleton();
        binder().bind(PipelineExecutionService.class).asEagerSingleton();
        binder().bind(PipelineStore.class).asEagerSingleton();
        binder().bind(PipelineStoreClient.class).asEagerSingleton();

        registerProcessor(SimpleProcessor.TYPE, SimpleProcessor.Builder.Factory.class);
        registerProcessor(GrokProcessor.TYPE, GrokProcessor.Builder.Factory.class);

        MapBinder<String, Processor.Builder.Factory> mapBinder = MapBinder.newMapBinder(binder(), String.class, Processor.Builder.Factory.class);
        for (Map.Entry<String, Class<? extends Processor.Builder.Factory>> entry : processors.entrySet()) {
            mapBinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }

    public void registerProcessor(String processorType, Class<? extends Processor.Builder.Factory> processorFactory) {
        processors.put(processorType, processorFactory);
    }

}
