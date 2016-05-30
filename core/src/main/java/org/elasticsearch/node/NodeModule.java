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

package org.elasticsearch.node;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.ingest.ProcessorsRegistry;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.ingest.processor.AppendProcessor;
import org.elasticsearch.ingest.processor.ConvertProcessor;
import org.elasticsearch.ingest.processor.DateProcessor;
import org.elasticsearch.ingest.processor.DateIndexNameProcessor;
import org.elasticsearch.ingest.processor.FailProcessor;
import org.elasticsearch.ingest.processor.ForEachProcessor;
import org.elasticsearch.ingest.processor.GsubProcessor;
import org.elasticsearch.ingest.processor.JoinProcessor;
import org.elasticsearch.ingest.processor.LowercaseProcessor;
import org.elasticsearch.ingest.processor.RemoveProcessor;
import org.elasticsearch.ingest.processor.RenameProcessor;
import org.elasticsearch.ingest.processor.SetProcessor;
import org.elasticsearch.ingest.processor.SortProcessor;
import org.elasticsearch.ingest.processor.SplitProcessor;
import org.elasticsearch.ingest.processor.TrimProcessor;
import org.elasticsearch.ingest.processor.UppercaseProcessor;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.service.NodeService;

import java.util.function.BiFunction;

/**
 *
 */
public class NodeModule extends AbstractModule {

    private final Node node;
    private final MonitorService monitorService;
    private final ProcessorsRegistry.Builder processorsRegistryBuilder;

    // pkg private so tests can mock
    Class<? extends PageCacheRecycler> pageCacheRecyclerImpl = PageCacheRecycler.class;
    Class<? extends BigArrays> bigArraysImpl = BigArrays.class;

    public NodeModule(Node node, MonitorService monitorService) {
        this.node = node;
        this.monitorService = monitorService;
        this.processorsRegistryBuilder = new ProcessorsRegistry.Builder();

        registerProcessor(DateProcessor.TYPE, (templateService, registry) -> new DateProcessor.Factory());
        registerProcessor(SetProcessor.TYPE, (templateService, registry) -> new SetProcessor.Factory(templateService));
        registerProcessor(AppendProcessor.TYPE, (templateService, registry) -> new AppendProcessor.Factory(templateService));
        registerProcessor(RenameProcessor.TYPE, (templateService, registry) -> new RenameProcessor.Factory());
        registerProcessor(RemoveProcessor.TYPE, (templateService, registry) -> new RemoveProcessor.Factory(templateService));
        registerProcessor(SplitProcessor.TYPE, (templateService, registry) -> new SplitProcessor.Factory());
        registerProcessor(JoinProcessor.TYPE, (templateService, registry) -> new JoinProcessor.Factory());
        registerProcessor(UppercaseProcessor.TYPE, (templateService, registry) -> new UppercaseProcessor.Factory());
        registerProcessor(LowercaseProcessor.TYPE, (templateService, registry) -> new LowercaseProcessor.Factory());
        registerProcessor(TrimProcessor.TYPE, (templateService, registry) -> new TrimProcessor.Factory());
        registerProcessor(ConvertProcessor.TYPE, (templateService, registry) -> new ConvertProcessor.Factory());
        registerProcessor(GsubProcessor.TYPE, (templateService, registry) -> new GsubProcessor.Factory());
        registerProcessor(FailProcessor.TYPE, (templateService, registry) -> new FailProcessor.Factory(templateService));
        registerProcessor(ForEachProcessor.TYPE, (templateService, registry) -> new ForEachProcessor.Factory(registry));
        registerProcessor(DateIndexNameProcessor.TYPE, (templateService, registry) -> new DateIndexNameProcessor.Factory());
        registerProcessor(SortProcessor.TYPE, (templateService, registry) -> new SortProcessor.Factory());
    }

    @Override
    protected void configure() {
        if (pageCacheRecyclerImpl == PageCacheRecycler.class) {
            bind(PageCacheRecycler.class).asEagerSingleton();
        } else {
            bind(PageCacheRecycler.class).to(pageCacheRecyclerImpl).asEagerSingleton();
        }
        if (bigArraysImpl == BigArrays.class) {
            bind(BigArrays.class).asEagerSingleton();
        } else {
            bind(BigArrays.class).to(bigArraysImpl).asEagerSingleton();
        }

        bind(Node.class).toInstance(node);
        bind(MonitorService.class).toInstance(monitorService);
        bind(NodeService.class).asEagerSingleton();
        bind(ProcessorsRegistry.Builder.class).toInstance(processorsRegistryBuilder);
    }

    /**
     * Returns the node
     */
    public Node getNode() {
        return node;
    }

    /**
     * Adds a processor factory under a specific type name.
     */
    public void registerProcessor(String type, BiFunction<TemplateService, ProcessorsRegistry, Processor.Factory<?>> provider) {
        processorsRegistryBuilder.registerProcessor(type, provider);
    }
}
