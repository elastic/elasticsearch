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

import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A pipeline is a list of {@link Processor} instances grouped under a unique id.
 */
public final class Pipeline {

    private final String id;
    private final String description;
    private final List<Processor> processors;

    private Pipeline(String id, String description, List<Processor> processors) {
        this.id = id;
        this.description = description;
        this.processors = processors;
    }

    /**
     * Modifies the data of a document to be indexed based on the processor this pipeline holds
     */
    public void execute(Data data) {
        for (Processor processor : processors) {
            processor.execute(data);
        }
    }

    /**
     * The unique id of this pipeline
     */
    public String getId() {
        return id;
    }

    /**
     * An optional description of what this pipeline is doing to the data gets processed by this pipeline.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Unmodifiable list containing each processor that operates on the data.
     */
    public List<Processor> getProcessors() {
        return processors;
    }

    public final static class Builder {

        private final String id;
        private String description;
        private List<Processor> processors = new ArrayList<>();

        public Builder(String id) {
            this.id = id;
        }

        public void fromMap(Map<String, Object> config, Map<String, Processor.Builder.Factory> processorRegistry) throws IOException {
            description = (String) config.get("description");
            @SuppressWarnings("unchecked")
            List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) config.get("processors");
            if (processors != null ) {
                for (Map<String, Map<String, Object>> processor : processors) {
                    for (Map.Entry<String, Map<String, Object>> entry : processor.entrySet()) {
                        Processor.Builder builder = processorRegistry.get(entry.getKey()).create();
                        if (builder != null) {
                            builder.fromMap(entry.getValue());
                            this.processors.add(builder.build());
                        } else {
                            throw new IllegalArgumentException("No processor type exist with name [" + entry.getKey() + "]");
                        }
                    }
                }
            }
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public void addProcessors(Processor.Builder... processors) throws IOException {
            for (Processor.Builder processor : processors) {
                this.processors.add(processor.build());
            }
        }

        public Pipeline build() {
            return new Pipeline(id, description, Collections.unmodifiableList(processors));
        }
    }
}
