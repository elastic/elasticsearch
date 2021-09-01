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
import java.util.Locale;
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
    private final ConflictStrategy addToRootConflictStrategy;
    private final boolean allowDuplicateKeys;

    JsonProcessor(String tag, String description, String field, String targetField, boolean addToRoot,
                  ConflictStrategy addToRootConflictStrategy, boolean allowDuplicateKeys) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.addToRoot = addToRoot;
        this.addToRootConflictStrategy = addToRootConflictStrategy;
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

    public ConflictStrategy getAddToRootConflictStrategy() {
        return addToRootConflictStrategy;
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

    public static void apply(Map<String, Object> ctx, String fieldName, boolean allowDuplicateKeys, ConflictStrategy conflictStrategy) {
        Object value = apply(ctx.get(fieldName), allowDuplicateKeys);
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            if (conflictStrategy == ConflictStrategy.MERGE) {
                recursiveMerge(ctx, map);
            } else {
                ctx.putAll(map);
            }
        } else {
            throw new IllegalArgumentException("cannot add non-map fields to root of document");
        }
    }

    public static void recursiveMerge(Map<String, Object> target, Map<String, Object> from) {
        for (String key : from.keySet()) {
            if (target.containsKey(key)) {
                Object targetValue = target.get(key);
                Object fromValue = from.get(key);
                if (targetValue instanceof Map && fromValue instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> targetMap = (Map<String, Object>) targetValue;
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fromMap = (Map<String, Object>) fromValue;
                    recursiveMerge(targetMap, fromMap);
                } else {
                    target.put(key, fromValue);
                }
            } else {
                target.put(key, from.get(key));
            }
        }
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        if (addToRoot) {
            apply(document.getSourceAndMetadata(), field, allowDuplicateKeys, addToRootConflictStrategy);
        } else {
            document.setFieldValue(targetField, apply(document.getFieldValue(field, Object.class), allowDuplicateKeys));
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public enum ConflictStrategy {
        REPLACE,
        MERGE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ConflictStrategy fromString(String conflictStrategy) {
            return ConflictStrategy.valueOf(conflictStrategy.toUpperCase(Locale.ROOT));
        }
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public JsonProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                    String description, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_field");
            boolean addToRoot = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "add_to_root", false);
            boolean allowDuplicateKeys = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "allow_duplicate_keys", false);
            String conflictStrategyString = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config,
                "add_to_root_conflict_strategy");
            boolean hasConflictStrategy = conflictStrategyString != null;
            if (conflictStrategyString == null) {
                conflictStrategyString = ConflictStrategy.REPLACE.name();
            }
            ConflictStrategy addToRootConflictStrategy;
            try {
                addToRootConflictStrategy = ConflictStrategy.fromString(conflictStrategyString);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, "add_to_root_conflict_strategy", "conflict strategy [" +
                    conflictStrategyString + "] not supported, cannot convert field.");
            }

            if (addToRoot && targetField != null) {
                throw newConfigurationException(TYPE, processorTag, "target_field",
                    "Cannot set a target field while also setting `add_to_root` to true");
            }
            if (addToRoot == false && hasConflictStrategy) {
                throw newConfigurationException(TYPE, processorTag, "add_to_root_conflict_strategy",
                    "Cannot set `add_to_root_conflict_strategy` if `add_to_root` is false");
            }

            if (targetField == null) {
                targetField = field;
            }

            return new JsonProcessor(processorTag, description, field, targetField, addToRoot, addToRootConflictStrategy,
                allowDuplicateKeys);
        }
    }
}

