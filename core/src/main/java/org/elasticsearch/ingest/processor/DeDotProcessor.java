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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.IngestDocument;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Processor that replaces dots in document field names with a
 * specified separator.
 */
public class DeDotProcessor extends AbstractProcessor {

    public static final String TYPE = "dedot";
    static final String DEFAULT_SEPARATOR = "_";

    private final String separator;

    DeDotProcessor(String tag, String separator) {
        super(tag);
        this.separator = separator;
    }

    public String getSeparator() {
        return separator;
    }

    @Override
    public void execute(IngestDocument document) {
        deDot(document.getSourceAndMetadata());
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Recursively iterates through Maps and Lists in search of map entries with
     * keys containing dots. The dots in these fields are replaced with {@link #separator}.
     *
     * @param obj The current object in context to be checked for dots in its fields.
     */
    private void deDot(Object obj) {
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map) obj;
            Iterator<Map.Entry<String, Object>> it = doc.entrySet().iterator();
            Map<String, Object> deDottedFields = new HashMap<>();
            while (it.hasNext()) {
                Map.Entry<String, Object> entry = it.next();
                deDot(entry.getValue());
                String fieldName = entry.getKey();
                if (fieldName.contains(".")) {
                    String deDottedFieldName = fieldName.replaceAll("\\.", separator);
                    deDottedFields.put(deDottedFieldName, entry.getValue());
                    it.remove();
                }
            }
            doc.putAll(deDottedFields);
        } else if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List) obj;
            for (Object value : list) {
                deDot(value);
            }
        }
    }

    public static class Factory extends AbstractProcessorFactory<DeDotProcessor> {

        @Override
        public DeDotProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String separator = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "separator");
            if (separator == null) {
                separator = DEFAULT_SEPARATOR;
            }
            return new DeDotProcessor(processorTag, separator);
        }
    }
}

