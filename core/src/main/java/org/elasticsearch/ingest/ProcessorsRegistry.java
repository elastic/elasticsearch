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

    public ProcessorsRegistry(Map<String, Processor.Factory> processors) {
        this.processorFactories = Collections.unmodifiableMap(processors);
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
}
