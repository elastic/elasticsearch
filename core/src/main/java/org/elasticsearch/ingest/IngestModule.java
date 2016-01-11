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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.ingest.processor.AppendProcessor;
import org.elasticsearch.ingest.processor.ConvertProcessor;
import org.elasticsearch.ingest.processor.DateProcessor;
import org.elasticsearch.ingest.processor.FailProcessor;
import org.elasticsearch.ingest.processor.GsubProcessor;
import org.elasticsearch.ingest.processor.JoinProcessor;
import org.elasticsearch.ingest.processor.LowercaseProcessor;
import org.elasticsearch.ingest.processor.RemoveProcessor;
import org.elasticsearch.ingest.processor.RenameProcessor;
import org.elasticsearch.ingest.processor.SetProcessor;
import org.elasticsearch.ingest.processor.SplitProcessor;
import org.elasticsearch.ingest.processor.TrimProcessor;
import org.elasticsearch.ingest.processor.UppercaseProcessor;

import java.util.function.BiFunction;

/**
 * Registry for processor factories
 * @see Processor.Factory
 */
public class IngestModule extends AbstractModule {

    private final ProcessorsRegistry processorsRegistry;

    public IngestModule() {
        this.processorsRegistry = new ProcessorsRegistry();
        registerProcessor(DateProcessor.TYPE, (environment, templateService) -> new DateProcessor.Factory());
        registerProcessor(SetProcessor.TYPE, (environment, templateService) -> new SetProcessor.Factory(templateService));
        registerProcessor(AppendProcessor.TYPE, (environment, templateService) -> new AppendProcessor.Factory(templateService));
        registerProcessor(RenameProcessor.TYPE, (environment, templateService) -> new RenameProcessor.Factory());
        registerProcessor(RemoveProcessor.TYPE, (environment, templateService) -> new RemoveProcessor.Factory(templateService));
        registerProcessor(SplitProcessor.TYPE, (environment, templateService) -> new SplitProcessor.Factory());
        registerProcessor(JoinProcessor.TYPE, (environment, templateService) -> new JoinProcessor.Factory());
        registerProcessor(UppercaseProcessor.TYPE, (environment, templateService) -> new UppercaseProcessor.Factory());
        registerProcessor(LowercaseProcessor.TYPE, (environment, templateService) -> new LowercaseProcessor.Factory());
        registerProcessor(TrimProcessor.TYPE, (environment, templateService) -> new TrimProcessor.Factory());
        registerProcessor(ConvertProcessor.TYPE, (environment, templateService) -> new ConvertProcessor.Factory());
        registerProcessor(GsubProcessor.TYPE, (environment, templateService) -> new GsubProcessor.Factory());
        registerProcessor(FailProcessor.TYPE, (environment, templateService) -> new FailProcessor.Factory(templateService));
    }

    @Override
    protected void configure() {
        bind(ProcessorsRegistry.class).toInstance(processorsRegistry);
        binder().bind(IngestBootstrapper.class).asEagerSingleton();
    }

    /**
     * Adds a processor factory under a specific type name.
     */
    public void registerProcessor(String type, BiFunction<Environment, TemplateService, Processor.Factory<?>> processorFactoryProvider) {
        processorsRegistry.registerProcessor(type, processorFactoryProvider);
    }

    public static boolean isIngestEnabled(Settings settings) {
        return settings.getAsBoolean("node.ingest", true);
    }
}
