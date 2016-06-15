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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.script.ScriptService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class ProcessorsRegistry implements Closeable {

    private final Map<String, Processor.Factory> processorFactories;
    private final TemplateService templateService;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    private ProcessorsRegistry(ScriptService scriptService, ClusterService clusterService,
                               Map<String, Function<ProcessorsRegistry, Processor.Factory<?>>> providers) {
        this.templateService = new InternalTemplateService(scriptService);
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        for (Map.Entry<String, Function<ProcessorsRegistry, Processor.Factory<?>>> entry : providers.entrySet()) {
            processorFactories.put(entry.getKey(), entry.getValue().apply(this));
        }
        this.processorFactories = Collections.unmodifiableMap(processorFactories);
    }

    public TemplateService getTemplateService() {
        return templateService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public Processor.Factory getProcessorFactory(String name) {
        return processorFactories.get(name);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = new ArrayList<>();
        for (Processor.Factory factory : processorFactories.values()) {
            if (factory instanceof Closeable) {
                closeables.add((Closeable) factory);
            }
        }
        IOUtils.close(closeables);
    }

    // For testing:
    Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    public static final class Builder {

        private final Map<String, Function<ProcessorsRegistry, Processor.Factory<?>>> providers = new HashMap<>();

        /**
         * Adds a processor factory under a specific name.
         */
        public void registerProcessor(String name, Function<ProcessorsRegistry, Processor.Factory<?>> provider) {
            Function<ProcessorsRegistry, Processor.Factory<?>> previous = this.providers.putIfAbsent(name, provider);
            if (previous != null) {
                throw new IllegalArgumentException("Processor factory already registered for name [" + name + "]");
            }
        }

        public ProcessorsRegistry build(ScriptService scriptService, ClusterService clusterService) {
            return new ProcessorsRegistry(scriptService, clusterService, providers);
        }

    }
}
