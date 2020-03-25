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

import java.util.Map;

public final class DotExpanderProcessor extends AbstractProcessor {

    static final String TYPE = "dot_expander";

    private final String path;
    private final String field;

    DotExpanderProcessor(String tag, String path, String field) {
        super(tag);
        this.path = path;
        this.field = field;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String path;
        Map<String, Object> map;
        if (this.path != null) {
            path = this.path + "." + field;
            map = ingestDocument.getFieldValue(this.path, Map.class);
        } else {
            path = field;
            map = ingestDocument.getSourceAndMetadata();
        }

        if (map.containsKey(field)) {
            if (ingestDocument.hasField(path)) {
                Object value = map.remove(field);
                ingestDocument.appendFieldValue(path, value);
            } else {
                // check whether we actually can expand the field in question into an object field.
                // part of the path may already exist and if part of it would be a value field (string, integer etc.)
                // then we can't override it with an object field and we should fail with a good reason.
                // IngestDocument#setFieldValue(...) would fail too, but the error isn't very understandable
                for (int index = path.indexOf('.'); index != -1; index = path.indexOf('.', index + 1)) {
                    String partialPath = path.substring(0, index);
                    if (ingestDocument.hasField(partialPath)) {
                        Object val = ingestDocument.getFieldValue(partialPath, Object.class);
                        if ((val instanceof Map) == false) {
                            throw new IllegalArgumentException("cannot expend [" + path + "], because [" + partialPath +
                                "] is not an object field, but a value field");
                        }
                    } else {
                        break;
                    }
                }
                Object value = map.remove(field);
                ingestDocument.setFieldValue(path, value);
            }
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getPath() {
        return path;
    }

    String getField() {
        return field;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag,
                                Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            if (field.contains(".") == false) {
                throw ConfigurationUtils.newConfigurationException(ConfigurationUtils.TAG_KEY, tag, "field",
                        "field does not contain a dot");
            }
            if (field.indexOf('.') == 0 || field.lastIndexOf('.') == field.length() - 1) {
                throw ConfigurationUtils.newConfigurationException(ConfigurationUtils.TAG_KEY, tag, "field",
                        "Field can't start or end with a dot");
            }
            int firstIndex = -1;
            for (int index = field.indexOf('.'); index != -1; index = field.indexOf('.', index + 1)) {
                if (index - firstIndex == 1) {
                    throw ConfigurationUtils.newConfigurationException(ConfigurationUtils.TAG_KEY, tag, "field",
                            "No space between dots");
                }
                firstIndex = index;
            }

            String path = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "path");
            return new DotExpanderProcessor(tag, path, field);
        }
    }
}
