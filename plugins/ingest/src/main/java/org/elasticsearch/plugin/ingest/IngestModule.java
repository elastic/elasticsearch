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
import org.elasticsearch.ingest.processor.set.SetProcessor;
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

import java.util.HashMap;
import java.util.Map;

public class IngestModule extends AbstractModule {

    private final Map<String, ProcessorFactoryProvider> processorFactoryProviders = new HashMap<>();

    @Override
    protected void configure() {
        binder().bind(IngestRestFilter.class).asEagerSingleton();
        binder().bind(IngestBootstrapper.class).asEagerSingleton();

        addProcessor(GeoIpProcessor.TYPE, (environment, templateService) -> new GeoIpProcessor.Factory(environment.configFile()));
        addProcessor(GrokProcessor.TYPE, (environment, templateService) -> new GrokProcessor.Factory(environment.configFile()));
        addProcessor(DateProcessor.TYPE, (environment, templateService) -> new DateProcessor.Factory());
        addProcessor(SetProcessor.TYPE, (environment, templateService) -> new SetProcessor.Factory(templateService));
        addProcessor(RenameProcessor.TYPE, (environment, templateService) -> new RenameProcessor.Factory());
        addProcessor(RemoveProcessor.TYPE, (environment, templateService) -> new RemoveProcessor.Factory(templateService));
        addProcessor(SplitProcessor.TYPE, (environment, templateService) -> new SplitProcessor.Factory());
        addProcessor(JoinProcessor.TYPE, (environment, templateService) -> new JoinProcessor.Factory());
        addProcessor(UppercaseProcessor.TYPE, (environment, templateService) -> new UppercaseProcessor.Factory());
        addProcessor(LowercaseProcessor.TYPE, (environment, mustacheFactory) -> new LowercaseProcessor.Factory());
        addProcessor(TrimProcessor.TYPE, (environment, templateService) -> new TrimProcessor.Factory());
        addProcessor(ConvertProcessor.TYPE, (environment, templateService) -> new ConvertProcessor.Factory());
        addProcessor(GsubProcessor.TYPE, (environment, templateService) -> new GsubProcessor.Factory());

        MapBinder<String, ProcessorFactoryProvider> mapBinder = MapBinder.newMapBinder(binder(), String.class, ProcessorFactoryProvider.class);
        for (Map.Entry<String, ProcessorFactoryProvider> entry : processorFactoryProviders.entrySet()) {
            mapBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }
    }

    /**
     * Adds a processor factory under a specific type name.
     */
    public void addProcessor(String type, ProcessorFactoryProvider processorFactoryProvider) {
        processorFactoryProviders.put(type, processorFactoryProvider);
    }

}
