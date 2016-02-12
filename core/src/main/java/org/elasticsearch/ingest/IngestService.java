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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * Holder class for several ingest related services.
 */
public class IngestService implements Closeable {

    private final PipelineStore pipelineStore;
    private final PipelineExecutionService pipelineExecutionService;
    private final ProcessorsRegistry.Builder processorsRegistryBuilder;

    public IngestService(Settings settings, ThreadPool threadPool, ProcessorsRegistry.Builder processorsRegistryBuilder) {
        this.processorsRegistryBuilder = processorsRegistryBuilder;
        this.pipelineStore = new PipelineStore(settings);
        this.pipelineExecutionService = new PipelineExecutionService(pipelineStore, threadPool);
    }

    public PipelineStore getPipelineStore() {
        return pipelineStore;
    }

    public PipelineExecutionService getPipelineExecutionService() {
        return pipelineExecutionService;
    }

    public void setScriptService(ScriptService scriptService) {
        pipelineStore.buildProcessorFactoryRegistry(processorsRegistryBuilder, scriptService);
    }

    @Override
    public void close() throws IOException {
        pipelineStore.close();
    }

}
