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

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Infers schema from NDJSON files by reading the first N lines.
 * - Flattens nested objects using dot notation
 * - Detects arrays as multi-value fields
 * - Marks fields as nullable when null or missing values are encountered
 *
 * Types: KEYWORD, INTEGER, LONG, DOUBLE, BOOLEAN, DATETIME.
 */
public class NdJsonSchemaInferrer {

    // Known issue: missing field in structures in nested arrays will not be marked as nullable.
    // In this example, "events.page" will not be nullable:
    // {"events": [{"type": "click", "page": 1}, {"type": "view", "page": 2}]}
    // {"events": [{"type": "click", "page": 3}, {"type": "view"}]}
    //
    // Accurately detecting this would require a more costly null/missing algorithm, and nulls are
    // not supported in arrays anyway.

    // The default format for date fields in ES is "strict_date_optional_time||epoch_millis".
    // Use the string part of this default for schema inference (we cannot assume that a number
    // is a date)
    public static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");

    private static final Logger logger = LogManager.getLogger(NdJsonSchemaInferrer.class);

    private static final EnumSet<DataType> NUMBER_TYPES = EnumSet.of(DataType.DOUBLE, DataType.LONG, DataType.INTEGER);

    // Fields that we've actually seen in the current json document
    private final BitSet fieldsSeen = new BitSet();
    private final List<FieldInfo> fields = new ArrayList<>();
    private int lineCount = 0;

    private NdJsonSchemaInferrer() {}

    /**
     * Infers schema from an NDJSON input stream, reading up to maxLines.
     */
    public static List<Attribute> inferSchema(InputStream inputStream, int maxLines) throws IOException {
        return new NdJsonSchemaInferrer().doInferSchema(inputStream, maxLines);
    }

    private List<Attribute> doInferSchema(InputStream inputStream, int maxLines) throws IOException {
        FieldInfo root = new FieldInfo(null);
        JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
        try {
            while (lineCount < maxLines) {
                try {
                    if (parser.nextToken() == null) {
                        break; // End of stream
                    }
                } catch (JsonParseException e) {
                    // Schema inference is a best-effort sampling pass: malformed lines here are
                    // safe to skip because every such line will be re-encountered during the
                    // actual slice read (see NdJsonPageIterator), where the configured
                    // ErrorPolicy decides whether to log/fail. Logging at debug avoids noisy
                    // duplicate reports of the same issue.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, inputStream);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                    continue;
                }

                try {
                    inferObjectSchema(parser, root);
                    lineCount++;
                } catch (JsonParseException e) {
                    // See comment above: deferred to the slice read for policy-driven handling.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, inputStream);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                }

                // Mark fields we haven't seen in this round as nullable
                for (int i = 0; i < fields.size(); i++) {
                    if (fieldsSeen.get(i) == false) {
                        fields.get(i).nullable = true;
                    }
                }
                fieldsSeen.clear();

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
                String text = parser.getText();
                // All-digit strings (e.g. book ids) must not be inferred as years / partial dates.
                if (field.types.contains(DataType.KEYWORD) == false && isLikelyDateString(text) && DATE_FORMATTER.tryParse(text) != null) {
                    field.addType(DataType.DATETIME);
                } else {
                    field.addType(DataType.KEYWORD);
                }
            }
            case VALUE_NUMBER_INT -> {
                switch (parser.getNumberType()) {
                    case INT:
                        field.addType(DataType.INTEGER);
                        return;
                    case LONG:
                        field.addType(DataType.LONG);
                        return;
                    case BIG_INTEGER: {
                        field.addType(DataType.DOUBLE);
                        var location = parser.getTokenLocation();
                        logger.debug(
                            "Big integers are not supported, falling back to double [{}, line: {}, column: {}]",
                            parser.getText(),
                            location.getLineNr(),
                            location.getColumnNr()
                        );
                    }
                }
            } // conservative size
            case VALUE_NUMBER_FLOAT -> field.addType(DataType.DOUBLE); // conservative size
            case VALUE_TRUE, VALUE_FALSE -> field.addType(DataType.BOOLEAN);
            case VALUE_NULL -> field.nullable = true;
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
                attributes.add(attribute(name, dataType, info.nullable));
            }

            if (info.children != null) {
                buildSchema(info, name, attributes);
            }
        }
    }

    public static Attribute attribute(String name, DataType type, boolean nullable) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullable ? Nullability.TRUE : Nullability.UNKNOWN, null, false);
    }

    /**
     * Field type information collected during schema inference.
     */
    private class FieldInfo {
        final EnumSet<DataType> types = EnumSet.noneOf(DataType.class);
        boolean isArray = false;
        boolean nullable = false;
        Map<String, FieldInfo> children = null;
        final int idx;
        final String name;

        FieldInfo(String name) {
            this.name = name;
            this.idx = fields.size();
            fields.add(this);
            if (lineCount > 0) {
                // Field appearing after the first lines.
                nullable = true;
            }
        }

        FieldInfo getChild(String name) {
            // TODO: limit depth
            if (children == null) {
                children = new LinkedHashMap<>();
            }
            return children.computeIfAbsent(name, (n) -> new FieldInfo(n));
        }

        void addType(DataType type) {
            types.add(type);
            fieldsSeen.set(idx);
        }

        DataType resolveType() {
            if (types.isEmpty()) {
                // Can happen with parent and always-empty array
                return DataType.UNSUPPORTED;
            }

            // Note: DATETIME and BOOLEAN will only be selected if they're the only type
            if (types.size() == 1) {
                return types.iterator().next();
            }

            // Multiple types - use the widest type
            // Nullability is handled separately and not part of type resolution
            if (types.contains(DataType.KEYWORD)) {
                return DataType.KEYWORD;
            }

            if (hasOnly(types, NUMBER_TYPES)) {
                if (types.contains(DataType.DOUBLE)) {
                    return DataType.DOUBLE;
                }
                if (types.contains(DataType.LONG)) {
                    return DataType.LONG;
                }
                if (types.contains(DataType.INTEGER)) {
                    return DataType.INTEGER;
                }
            }

            // Widest type
            return DataType.KEYWORD;
        }
    }

    private static <E extends Enum<E>> boolean hasOnly(EnumSet<E> values, EnumSet<E> from) {
        if (values.isEmpty()) {
            return false;
        }
        var copy = EnumSet.copyOf(values);
        copy.removeAll(from);
        return copy.isEmpty();
    }

    /**
     * {@code strict_date_optional_time} accepts year-only forms that collide with numeric identifiers
     * (e.g. book numbers). Skip date inference for all-ASCII-digit tokens.
     */
    private static boolean isLikelyDateString(String text) {
        if (text.isEmpty()) {
            return false;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < '0' || c > '9') {
                return true;
            }
        }
        return false;
    }
}
