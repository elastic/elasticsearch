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
import org.elasticsearch.ingest.processor.add.AddProcessor;
import org.elasticsearch.ingest.processor.convert.ConvertProcessor;
import org.elasticsearch.ingest.processor.date.DateProcessor;
import org.elasticsearch.ingest.processor.geoip.GeoIpProcessor;
import org.elasticsearch.ingest.processor.grok.GrokProcessor;
import org.elasticsearch.ingest.processor.gsub.GsubProcessor;
import org.elasticsearch.ingest.processor.join.JoinProcessor;
import org.elasticsearch.ingest.processor.lowercase.LowercaseProcessor;
import org.elasticsearch.ingest.processor.remove.RemoveProcessor;
import org.elasticsearch.ingest.processor.rename.RenameProcessor;
import org.elasticsearch.ingest.processor.split.SplitProcessor;
import org.elasticsearch.ingest.processor.trim.TrimProcessor;
import org.elasticsearch.ingest.processor.uppercase.UppercaseProcessor;
import org.elasticsearch.plugin.ingest.rest.IngestRestFilter;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulateExecutionService;

import java.util.HashMap;
import java.util.Map;

public class IngestModule extends AbstractModule {

    private final Map<String, Processor.Factory> processors = new HashMap<>();

    @Override
    protected void configure() {
        binder().bind(IngestRestFilter.class).asEagerSingleton();
        binder().bind(PipelineExecutionService.class).asEagerSingleton();
        binder().bind(PipelineStore.class).asEagerSingleton();
        binder().bind(SimulateExecutionService.class).asEagerSingleton();

        addProcessor(GeoIpProcessor.TYPE, new GeoIpProcessor.Factory());
        addProcessor(GrokProcessor.TYPE, new GrokProcessor.Factory());
        addProcessor(DateProcessor.TYPE, new DateProcessor.Factory());
        addProcessor(AddProcessor.TYPE, new AddProcessor.Factory());
        addProcessor(RenameProcessor.TYPE, new RenameProcessor.Factory());
        addProcessor(RemoveProcessor.TYPE, new RemoveProcessor.Factory());
        addProcessor(SplitProcessor.TYPE, new SplitProcessor.Factory());
        addProcessor(JoinProcessor.TYPE, new JoinProcessor.Factory());
        addProcessor(UppercaseProcessor.TYPE, new UppercaseProcessor.Factory());
        addProcessor(LowercaseProcessor.TYPE, new LowercaseProcessor.Factory());
        addProcessor(TrimProcessor.TYPE, new TrimProcessor.Factory());
        addProcessor(ConvertProcessor.TYPE, new ConvertProcessor.Factory());
        addProcessor(GsubProcessor.TYPE, new GsubProcessor.Factory());

        MapBinder<String, Processor.Factory> mapBinder = MapBinder.newMapBinder(binder(), String.class, Processor.Factory.class);
        for (Map.Entry<String, Processor.Factory> entry : processors.entrySet()) {
            mapBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }
    }

    /**
     * Adds a processor factory under a specific type name.
     */
    public void addProcessor(String type, Processor.Factory factory) {
        processors.put(type, factory);
    }

}
