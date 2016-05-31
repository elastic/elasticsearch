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

package org.elasticsearch.ingest.core;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.ProcessorsRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * A pipeline is a list of {@link Processor} instances grouped under a unique id.
 */
public final class Pipeline {

    final static String DESCRIPTION_KEY = "description";
    final static String PROCESSORS_KEY = "processors";
    final static String ON_FAILURE_KEY = "on_failure";

    private final String id;
    private final String description;
    private final CompoundProcessor compoundProcessor;

    public Pipeline(String id, String description, CompoundProcessor compoundProcessor) {
        this.id = id;
        this.description = description;
        this.compoundProcessor = compoundProcessor;
    }

    /**
     * Modifies the data of a document to be indexed based on the processor this pipeline holds
     */
    public void execute(IngestDocument ingestDocument) throws Exception {
        compoundProcessor.execute(ingestDocument);
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
     * Get the underlying {@link CompoundProcessor} containing the Pipeline's processors
     */
    public CompoundProcessor getCompoundProcessor() {
        return compoundProcessor;
    }

    /**
     * Unmodifiable list containing each processor that operates on the data.
     */
    public List<Processor> getProcessors() {
        return compoundProcessor.getProcessors();
    }

    /**
     * Unmodifiable list containing each on_failure processor that operates on the data in case of
     * exception thrown in pipeline processors
     */
    public List<Processor> getOnFailureProcessors() {
        return compoundProcessor.getOnFailureProcessors();
    }

    /**
     * Flattens the normal and on failure processors into a single list. The original order is lost.
     * This can be useful for pipeline validation purposes.
     */
    public List<Processor> flattenAllProcessors() {
        return compoundProcessor.flattenProcessors();
    }

    public final static class Factory {

        public Pipeline create(String id, Map<String, Object> config, ProcessorsRegistry processorRegistry) throws Exception {
            String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
            List<Map<String, Map<String, Object>>> processorConfigs = ConfigurationUtils.readList(null, null, config, PROCESSORS_KEY);
            List<Processor> processors = ConfigurationUtils.readProcessorConfigs(processorConfigs, processorRegistry);
            List<Map<String, Map<String, Object>>> onFailureProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, ON_FAILURE_KEY);
            List<Processor> onFailureProcessors = ConfigurationUtils.readProcessorConfigs(onFailureProcessorConfigs, processorRegistry);
            if (config.isEmpty() == false) {
                throw new ElasticsearchParseException("pipeline [" + id + "] doesn't support one or more provided configuration parameters " + Arrays.toString(config.keySet().toArray()));
            }
            CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.unmodifiableList(processors), Collections.unmodifiableList(onFailureProcessors));
            return new Pipeline(id, description, compoundProcessor);
        }

    }
}
