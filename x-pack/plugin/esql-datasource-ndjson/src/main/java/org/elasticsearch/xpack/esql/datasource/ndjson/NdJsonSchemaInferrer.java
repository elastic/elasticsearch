/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Infers schema from NDJSON files by reading the first N lines.
 * - Flattens nested objects using dot notation
 * - Detects arrays as multi-value fields
 * - Marks fields as nullable when null values are encountered
 *
 * Types: KEYWORD, LONG, DOUBLE, BOOLEAN, NULL. Number types are large to be on the safe side if the sample
 * only shows small values.
 *
 * Missing:
 * - Support for union types (e.g., string or integer)
 * - Support for nested arrays (does it exist in ESQL?)
 * - Support for complex types (e.g., date, ip)
 */
public class NdJsonSchemaInferrer {

    private static final Logger logger = LogManager.getLogger(NdJsonSchemaInferrer.class);
    private static final int DEFAULT_SAMPLE_SIZE = 100;

    /**
     * Infers schema from an NDJSON input stream.
     */
    public static List<Attribute> inferSchema(InputStream inputStream) throws IOException {
        return inferSchema(inputStream, DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Infers schema from an NDJSON input stream, reading up to maxLines.
     */
    public static List<Attribute> inferSchema(InputStream inputStream, int maxLines) throws IOException {
        FieldInfo root = new FieldInfo();
        JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
        try {
            int lineCount = 0;
            while (lineCount < maxLines) {
                try {
                    if (parser.nextToken() == null) {
                        break; // End of stream
                    }
                } catch (JsonParseException e) {
                    logger.warn("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, inputStream);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                    continue;
                }

                try {
                    inferObjectSchema(parser, root);
                    lineCount++;
                } catch (JsonParseException e) {
                    logger.warn("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, inputStream);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                }
            }
        } finally {
            parser.close();
        }

        // Convert FieldInfo map to Attribute list
        List<Attribute> attributes = new ArrayList<>();
        buildSchema(root, null, attributes);
        return attributes;
    }

    private static void inferObjectSchema(JsonParser parser, FieldInfo object) throws IOException {
        JsonToken token = parser.currentToken();
        if (token != JsonToken.START_OBJECT) {
            throw new NdJsonParseException(parser, "Expected JSON object");
        }
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token != JsonToken.FIELD_NAME) {
                throw new NdJsonParseException(parser, "Expected field name in object");
            }
            var child = object.getChild(parser.getCurrentName());
            parser.nextToken();
            inferValueSchema(parser, child);
        }
    }

    private static void inferValueSchema(JsonParser parser, FieldInfo field) throws IOException {
        switch (parser.currentToken()) {
            case START_ARRAY -> {
                field.isArray = true;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    inferValueSchema(parser, field);
                }
            }
            // Keep in sync with NdJsonPageIterator.Decoder
            case START_OBJECT -> inferObjectSchema(parser, field);
            case VALUE_STRING -> {
                try {
                    Instant.parse(parser.getText());
                    field.addType(DataType.DATETIME);
                } catch (DateTimeParseException e) {
                    field.addType(DataType.KEYWORD);
                }
            }
            case VALUE_NUMBER_INT -> {
                long value = parser.getLongValue();
                if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                    field.addType(DataType.INTEGER);
                } else {
                    field.addType(DataType.LONG);
                }
            } // conservative size
            case VALUE_NUMBER_FLOAT -> field.addType(DataType.DOUBLE); // conservative size
            case VALUE_TRUE, VALUE_FALSE -> field.addType(DataType.BOOLEAN);
            case VALUE_NULL -> field.addType(DataType.NULL);
            // Ignore all other events
        }
    }

    /** Build the list of Attribute by recursively traversing the FieldInfo tree */
    private static void buildSchema(FieldInfo field, String parentName, List<Attribute> attributes) {
        for (Map.Entry<String, FieldInfo> entry : field.children.entrySet()) {
            // TODO: disallow dots in names (or replace them) as it may cause issues when decoding
            var name = entry.getKey();
            var info = entry.getValue();
            if (parentName != null) {
                name = parentName + "." + name;
            }

            DataType dataType = info.resolveType();
            if (dataType != DataType.UNSUPPORTED) {
                // Unsupported is used for nested object properties
                attributes.add(attribute(name, dataType, info.types.contains(DataType.NULL)));
            }

            if (info.children != null) {
                buildSchema(info, name, attributes);
            }
        }
    }

    public static Attribute attribute(String name, DataType type, boolean nullable) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, type, Map.of(), false, null),
            nullable ? Nullability.TRUE : Nullability.UNKNOWN,
            null,
            false
        );
    }

    /**
     * Field type information collected during schema inference.
     */
    private static class FieldInfo {
        final EnumSet<DataType> types = EnumSet.noneOf(DataType.class);
        boolean isArray = false;
        Map<String, FieldInfo> children = null;

        FieldInfo getChild(String name) {
            // TODO: limit depth
            if (children == null) {
                children = new LinkedHashMap<>();
            }
            return children.computeIfAbsent(name, (_name) -> new FieldInfo());
        }

        void addType(DataType type) {
            types.add(type);
        }

        DataType resolveType() {
            if (types.isEmpty()) {
                // Can happen with parent and always-empty array
                return DataType.UNSUPPORTED;
            }
            if (types.size() == 1) {
                return types.iterator().next();
            }
            // Multiple types - for now, use the widest type
            // TODO: Create MultiTypeEsField for proper union type support
            if (types.contains(DataType.DATETIME)) {
                return DataType.DATETIME;
            }
            if (types.contains(DataType.KEYWORD)) {
                return DataType.KEYWORD;
            }
            if (types.contains(DataType.DOUBLE)) {
                return DataType.DOUBLE;
            }
            if (types.contains(DataType.LONG)) {
                return DataType.LONG;
            }
            if (types.contains(DataType.INTEGER)) {
                return DataType.INTEGER;
            }
            return types.iterator().next();
        }
    }
}
