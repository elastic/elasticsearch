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

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readMap;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * A processor that for each value in a list executes a one or more processors.
 *
 * This can be useful in cases to do string operations on json array of strings,
 * or remove a field from objects inside a json array.
 *
 * Note that this processor is experimental.
 */
public final class ForEachProcessor extends AbstractProcessor {

    public static final String TYPE = "foreach";

    private final String field;
    private final Processor processor;

    ForEachProcessor(String tag, String field, Processor processor) {
        super(tag);
        this.field = field;
        this.processor = processor;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        List values = ingestDocument.getFieldValue(field, List.class);
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object value : values) {
            Object previousValue = ingestDocument.getIngestMetadata().put("_value", value);
            try {
                processor.execute(ingestDocument);
            } finally {
                newValues.add(ingestDocument.getIngestMetadata().put("_value", previousValue));
            }
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

    Processor getProcessor() {
        return processor;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public ForEachProcessor create(Map<String, Processor.Factory> factories, String tag,
                                       Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            Map<String, Map<String, Object>> processorConfig = readMap(TYPE, tag, config, "processor");
            Set<Map.Entry<String, Map<String, Object>>> entries = processorConfig.entrySet();
            if (entries.size() != 1) {
                throw newConfigurationException(TYPE, tag, "processor", "Must specify exactly one processor type");
            }
            Map.Entry<String, Map<String, Object>> entry = entries.iterator().next();
            Processor processor = ConfigurationUtils.readProcessor(factories, entry.getKey(), entry.getValue());
            return new ForEachProcessor(tag, field, processor);
        }
    }
}
