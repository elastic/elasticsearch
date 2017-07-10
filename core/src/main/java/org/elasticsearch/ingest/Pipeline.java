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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A pipeline is a list of {@link Processor} instances grouped under a unique id.
 */
public final class Pipeline {

    static final String DESCRIPTION_KEY = "description";
    static final String PROCESSORS_KEY = "processors";
    static final String VERSION_KEY = "version";
    static final String ON_FAILURE_KEY = "on_failure";
    private static final CompoundProcessor EMPTY_PROCESSOR = new CompoundProcessor();

    private final String id;
    @Nullable
    private final String description;
    @Nullable
    private final Integer version;
    private final CompoundProcessor compoundProcessor;
    private final Optional<IngestScript> script;

    public Pipeline(String id, @Nullable String description, @Nullable Integer version, CompoundProcessor compoundProcessor) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.compoundProcessor = compoundProcessor;
        this.script = Optional.empty();
    }

    public Pipeline(String id, @Nullable String description, @Nullable Integer version, IngestScript script) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.script = Optional.of(script);
        this.compoundProcessor = EMPTY_PROCESSOR;
    }

    /**
     * Modifies the data of a document to be indexed based on the processor this pipeline holds
     */
    public void execute(IngestDocument ingestDocument) throws Exception {
        if (script.isPresent()) {
            script.get().execute(ingestDocument.getSourceAndMetadata());
        } else {
            compoundProcessor.execute(ingestDocument);
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
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * An optional version stored with the pipeline so that it can be used to determine if the pipeline should be updated / replaced.
     *
     * @return {@code null} if not supplied.
     */
    @Nullable
    public Integer getVersion() {
        return version;
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

    public static final class Factory {

        private void assertConfigIsEmpty(Map<String, Object> config, String pipelineId) {
            if (config.isEmpty() == false) {
                throw new ElasticsearchParseException("pipeline [" + pipelineId +
                    "] doesn't support one or more provided configuration parameters " + Arrays.toString(config.keySet().toArray()));
            }
        }
        public Pipeline create(String id, Map<String, Object> config, Map<String, Processor.Factory> processorFactories,
                               ScriptService scriptService) throws Exception {
            String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
            Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
            Script script = ConfigurationUtils.readOptionalScript(null, null,
                config, Script.SCRIPT_PARSE_FIELD.getPreferredName());
            if (script != null && (config.containsKey(PROCESSORS_KEY) || config.containsKey(ON_FAILURE_KEY))) {
                throw new ElasticsearchParseException("pipeline [" + id + "]: cannot have both `script` and `processors` defined");
            } else if (script != null) {
                // Skip processor extraction
                IngestScript.Factory factory = scriptService.compile(script, IngestScript.CONTEXT);
                IngestScript ingestScript = factory.newInstance(script.getParams());
                assertConfigIsEmpty(config, id);
                return new Pipeline(id, description, version, ingestScript);
            }
            List<Map<String, Map<String, Object>>> processorConfigs = ConfigurationUtils.readOptionalList(null,
                null,config, PROCESSORS_KEY);
            List<Processor> processors = ConfigurationUtils.readProcessorConfigs(processorConfigs, processorFactories);
            List<Map<String, Map<String, Object>>> onFailureProcessorConfigs =
                    ConfigurationUtils.readOptionalList(null, null, config, ON_FAILURE_KEY);
            List<Processor> onFailureProcessors = ConfigurationUtils.readProcessorConfigs(onFailureProcessorConfigs, processorFactories);
            assertConfigIsEmpty(config, id);
            if (onFailureProcessorConfigs != null && onFailureProcessors.isEmpty()) {
                throw new ElasticsearchParseException("pipeline [" + id + "] cannot have an empty on_failure option defined");
            }
            CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.unmodifiableList(processors),
                    Collections.unmodifiableList(onFailureProcessors));
            return new Pipeline(id, description, version, compoundProcessor);
        }

    }
}
