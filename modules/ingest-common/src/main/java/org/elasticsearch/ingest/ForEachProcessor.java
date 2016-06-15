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

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.core.ConfigurationUtils.readList;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;

/**
 * A processor that for each value in a list executes a one or more processors.
 *
 * This can be useful in cases to do string operations on json array of strings,
 * or remove a field from objects inside a json array.
 */
public final class ForEachProcessor extends AbstractProcessor {

    public static final String TYPE = "foreach";

    private final String field;
    private final List<Processor> processors;

    ForEachProcessor(String tag, String field, List<Processor> processors) {
        super(tag);
        this.field = field;
        this.processors = processors;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        List<Object> values = ingestDocument.getFieldValue(field, List.class);
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object value : values) {
            Map<String, Object> innerSource = new HashMap<>(ingestDocument.getSourceAndMetadata());
            innerSource.put("_value", value); // scalar value to access the list item being evaluated
            IngestDocument innerIngestDocument = new IngestDocument(innerSource, ingestDocument.getIngestMetadata());
            for (Processor processor : processors) {
                processor.execute(innerIngestDocument);
            }
            newValues.add(innerSource.get("_value"));
        }
        ingestDocument.setFieldValue(field, newValues);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    List<Processor> getProcessors() {
        return processors;
    }

    public static final class Factory extends AbstractProcessorFactory<ForEachProcessor> {

        private final ProcessorsRegistry processorRegistry;

        public Factory(ProcessorsRegistry processorRegistry) {
            this.processorRegistry = processorRegistry;
        }

        @Override
        protected ForEachProcessor doCreate(String tag, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            List<Map<String, Map<String, Object>>> processorConfigs = readList(TYPE, tag, config, "processors");
            List<Processor> processors = ConfigurationUtils.readProcessorConfigs(processorConfigs, processorRegistry);
            return new ForEachProcessor(tag, field, Collections.unmodifiableList(processors));
        }
    }
}
