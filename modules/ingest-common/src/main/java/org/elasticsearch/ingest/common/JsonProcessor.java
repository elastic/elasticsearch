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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that serializes a string-valued field into a
 * map of maps.
 */
public final class JsonProcessor extends AbstractProcessor {

    public static final String TYPE = "json";

    private final String field;
    private final String targetField;
    private final boolean addToRoot;

    JsonProcessor(String tag, String field, String targetField, boolean addToRoot) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.addToRoot = addToRoot;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    boolean isAddToRoot() {
        return addToRoot;
    }

    public static Object apply(Object fieldValue) {
        BytesReference bytesRef = fieldValue == null ? new BytesArray("null") : new BytesArray(fieldValue.toString());
        try (InputStream stream = bytesRef.streamInput();
             XContentParser parser = JsonXContent.jsonXContent
                 .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
            XContentParser.Token token = parser.nextToken();
            Object value = null;
            if (token == XContentParser.Token.VALUE_NULL) {
                value = null;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                value = parser.text();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.numberValue();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                value = parser.booleanValue();
            } else if (token == XContentParser.Token.START_OBJECT) {
                value = parser.map();
            } else if (token == XContentParser.Token.START_ARRAY) {
                value = parser.list();
            } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                throw new IllegalArgumentException("cannot read binary value");
            }
            return value;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void apply(Map<String, Object> ctx, String fieldName) {
        Object value = apply(ctx.get(fieldName));
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;
                ctx.putAll(map);
        } else {
            throw new IllegalArgumentException("cannot add non-map fields to root of document");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        if (addToRoot) {
           apply(document.getSourceAndMetadata(), field);
        } else {
            document.setFieldValue(targetField, apply(document.getFieldValue(field, Object.class)));
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public JsonProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                    Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_field");
            boolean addToRoot = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "add_to_root", false);

            if (addToRoot && targetField != null) {
                throw newConfigurationException(TYPE, processorTag, "target_field",
                    "Cannot set a target field while also setting `add_to_root` to true");
            }

            if (targetField == null) {
                targetField = field;
            }

            return new JsonProcessor(processorTag, field, targetField, addToRoot);
        }
    }
}

