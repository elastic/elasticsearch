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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.ingest.ProcessorsRegistry;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.service.NodeService;

import java.util.function.Function;

/**
 *
 */
public class NodeModule extends AbstractModule {

    private final Node node;
    private final MonitorService monitorService;
    private final ProcessorsRegistry.Builder processorsRegistryBuilder;

    // pkg private so tests can mock
    Class<? extends BigArrays> bigArraysImpl = BigArrays.class;

    public NodeModule(Node node, MonitorService monitorService) {
        this.node = node;
        this.monitorService = monitorService;
        this.processorsRegistryBuilder = new ProcessorsRegistry.Builder();
    }

    @Override
    protected void configure() {
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
    public void registerProcessor(String type, Function<ProcessorsRegistry, Processor.Factory<?>> provider) {
        processorsRegistryBuilder.registerProcessor(type, provider);
    }
}
