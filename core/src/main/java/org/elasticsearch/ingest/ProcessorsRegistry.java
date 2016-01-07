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

import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class ProcessorsRegistry {

    private final Map<String, BiFunction<Environment, TemplateService, Processor.Factory<?>>> processorFactoryProviders = new HashMap<>();

    /**
     * Adds a processor factory under a specific name.
     */
    public void registerProcessor(String name, BiFunction<Environment, TemplateService, Processor.Factory<?>> processorFactoryProvider) {
        BiFunction<Environment, TemplateService, Processor.Factory<?>> provider = processorFactoryProviders.putIfAbsent(name, processorFactoryProvider);
        if (provider != null) {
            throw new IllegalArgumentException("Processor factory already registered for name [" + name + "]");
        }
    }

    public Set<Map.Entry<String, BiFunction<Environment, TemplateService, Processor.Factory<?>>>> entrySet() {
        return processorFactoryProviders.entrySet();
    }
}
