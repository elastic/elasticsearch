/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    private final boolean allowDuplicateKeys;

    JsonProcessor(String tag, String description, String field, String targetField, boolean addToRoot, boolean allowDuplicateKeys) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.addToRoot = addToRoot;
        this.allowDuplicateKeys = allowDuplicateKeys;
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

    public static Object apply(Object fieldValue, boolean allowDuplicateKeys) {
        BytesReference bytesRef = fieldValue == null ? new BytesArray("null") : new BytesArray(fieldValue.toString());
        try (InputStream stream = bytesRef.streamInput();
             XContentParser parser = JsonXContent.jsonXContent
                 .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
            parser.allowDuplicateKeys(allowDuplicateKeys);
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

    public static void apply(Map<String, Object> ctx, String fieldName, boolean allowDuplicateKeys) {
        Object value = apply(ctx.get(fieldName), allowDuplicateKeys);
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
            apply(document.getSourceAndMetadata(), field, allowDuplicateKeys);
        } else {
            document.setFieldValue(targetField, apply(document.getFieldValue(field, Object.class), allowDuplicateKeys));
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
                                    String description, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_field");
            boolean addToRoot = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "add_to_root", false);
            boolean allowDuplicateKeys = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "allow_duplicate_keys", false);

            if (addToRoot && targetField != null) {
                throw newConfigurationException(TYPE, processorTag, "target_field",
                    "Cannot set a target field while also setting `add_to_root` to true");
            }

            if (targetField == null) {
                targetField = field;
            }

            return new JsonProcessor(processorTag, description, field, targetField, addToRoot, allowDuplicateKeys);
        }
    }
}

