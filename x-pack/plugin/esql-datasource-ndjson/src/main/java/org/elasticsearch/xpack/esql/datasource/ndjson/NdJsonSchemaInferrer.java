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
import com.fasterxml.jackson.core.exc.StreamConstraintsException;

import org.elasticsearch.cluster.metadata.DatasetMapping.Subobjects;
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
 *
 * <p>Column names are always flat dotted names, whichever way the file spells them: the two spellings of a dotted
 * column are one path through the field tree ({@link #childFor}), so mixing them across records infers one column.
 * {@link Subobjects} decides only whether a scalar and an object at the same name are a conflict (one shape wins) or
 * two independent columns.
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
    public static final DateFormatter STRICT_DATE_OPTIONAL_TIME = DateFormatter.forPattern("strict_date_optional_time");

    private static final Logger logger = LogManager.getLogger(NdJsonSchemaInferrer.class);

    private static final EnumSet<DataType> NUMBER_TYPES = EnumSet.of(DataType.DOUBLE, DataType.LONG, DataType.INTEGER);

    // Fields that we've actually seen in the current json document
    private final BitSet fieldsSeen = new BitSet();
    private final List<FieldInfo> fields = new ArrayList<>();
    private int lineCount = 0;

    private final DateFormatter dateFormatter;

    /** How a dotted field name is read; see {@link #acceptsScalar} and {@link #acceptsObject} for what it decides. */
    private final Subobjects subobjects;

    private NdJsonSchemaInferrer(DateFormatter dateFormatter, Subobjects subobjects) {
        this.dateFormatter = dateFormatter != null ? dateFormatter : STRICT_DATE_OPTIONAL_TIME;
        this.subobjects = subobjects;
    }

    /**
     * Infers schema from an NDJSON input stream, reading up to maxLines.
     * When {@code datetimeFormatter} is null, falls back to {@link #STRICT_DATE_OPTIONAL_TIME}.
     */
    public static List<Attribute> inferSchema(InputStream inputStream, int maxLines, DateFormatter datetimeFormatter, Subobjects subobjects)
        throws IOException {
        return new NdJsonSchemaInferrer(datetimeFormatter, subobjects).doInferSchema(inputStream, maxLines);
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
                } catch (JsonParseException | StreamConstraintsException e) {
                    // Schema inference is a best-effort sampling pass: malformed lines here are
                    // safe to skip because every such line will be re-encountered during the
                    // actual slice read (see NdJsonPageIterator), where the configured
                    // ErrorPolicy decides whether to log/fail. Logging at debug avoids noisy
                    // duplicate reports of the same issue. A StreamConstraintsException (an
                    // over-long number or field name, nesting past the depth cap) is the same
                    // scanner-level whole-line failure and defers to the slice read identically;
                    // failing inference on it would deny the read's error_mode a say.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, inputStream);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                    continue;
                }

                try {
                    inferObjectSchema(parser, root);
                    lineCount++;
                } catch (JsonParseException | StreamConstraintsException e) {
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

    private void inferObjectSchema(JsonParser parser, FieldInfo object) throws IOException {
        JsonToken token = parser.currentToken();
        if (token != JsonToken.START_OBJECT) {
            throw new NdJsonParseException(parser, "Expected JSON object");
        }
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token != JsonToken.FIELD_NAME) {
                throw new NdJsonParseException(parser, "Expected field name in object");
            }
            var child = childFor(object, parser.getCurrentName());
            parser.nextToken();
            if (child == null) {
                parser.skipChildren();
            } else {
                inferValueSchema(parser, child);
            }
        }
    }

    /**
     * The node a field name addresses within {@code object}. A dotted name is a path, so both spellings of a dotted
     * column ({@code {"a.b":1}} and {@code {"a":{"b":1}}}) land on one node and a file that mixes them infers one
     * column rather than two attributes with the same name.
     *
     * <p>Returns {@code null} when the name has no representable node: under {@link Subobjects#ENABLED} a segment that
     * already resolved to a scalar is a leaf and cannot be descended into, and the value is ignored the same way the
     * nested spelling of that conflict is.
     */
    private FieldInfo childFor(FieldInfo object, String fieldName) {
        if (NdJsonUtils.isFieldPath(fieldName) == false) {
            return object.getChild(fieldName);
        }
        FieldInfo node = object;
        int start = 0;
        int dot;
        while ((dot = fieldName.indexOf('.', start)) >= 0) {
            node = node.getChild(fieldName.substring(start, dot));
            if (acceptsObject(node) == false) {
                return null;
            }
            start = dot + 1;
        }
        return node.getChild(fieldName.substring(start));
    }

    /**
     * Whether a scalar value may be recorded on {@code field}. Under {@link Subobjects#ENABLED} a node that already has
     * children is an object, and a scalar on it is the shape conflict the decoder reports at read time. Under
     * {@link Subobjects#DISABLED} a scalar {@code a} and a flattened {@code a.b} are two independent columns, so the
     * node carries both.
     */
    private boolean acceptsScalar(FieldInfo field) {
        return subobjects == Subobjects.DISABLED || field.children == null;
    }

    /** The mirror of {@link #acceptsScalar}: whether an object value may descend into {@code field}. */
    private boolean acceptsObject(FieldInfo field) {
        return subobjects == Subobjects.DISABLED || field.types.isEmpty();
    }

    private void inferValueSchema(JsonParser parser, FieldInfo field) throws IOException {
        switch (parser.currentToken()) {
            case START_ARRAY -> {
                field.isArray = true;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    inferValueSchema(parser, field);
                }
            }
            // Keep in sync with NdJsonPageDecoder.BlockDecoder.decodeValue. Under Subobjects.ENABLED a field seen as
            // both a scalar and an object across sampled records resolves to whichever shape was observed first
            // (mirrors core ES dynamic mapping's first-writer-wins); the other shape is ignored here for
            // schema-inference purposes so buildSchema never emits both a scalar attribute and nested children for the
            // same name. The decoder applies ErrorPolicy to the actual conflicting value at read time. Under
            // Subobjects.DISABLED the two shapes are not in conflict: the object flattens to dotted columns that are
            // siblings of the scalar, so both are inferred.
            case START_OBJECT -> {
                if (acceptsObject(field) == false) {
                    parser.skipChildren();
                } else {
                    inferObjectSchema(parser, field);
                }
            }
            case VALUE_STRING -> {
                if (acceptsScalar(field)) {
                    String text = parser.getText();
                    if (field.types.contains(DataType.KEYWORD) == false && isDateTimeString(text)) {
                        field.addType(DataType.DATETIME);
                    } else {
                        field.addType(DataType.KEYWORD);
                    }
                }
            }
            case VALUE_NUMBER_INT -> {
                if (acceptsScalar(field)) {
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
                }
            } // conservative size
            case VALUE_NUMBER_FLOAT -> {
                if (acceptsScalar(field)) {
                    field.addType(DataType.DOUBLE); // conservative size
                }
            }
            case VALUE_TRUE, VALUE_FALSE -> {
                if (acceptsScalar(field)) {
                    field.addType(DataType.BOOLEAN);
                }
            }
            case VALUE_NULL -> field.nullable = true;
            // Ignore all other events
        }
    }

    /** Build the list of Attribute by recursively traversing the FieldInfo tree */
    private static void buildSchema(FieldInfo field, String parentName, List<Attribute> attributes) {
        if (field.children == null) {
            // No children were ever observed. Happens for the root when every sampled line was
            // malformed (so {@link FieldInfo#getChild} was never called), or legitimately for
            // leaf fields during recursion. Nothing to contribute to the schema either way.
            return;
        }
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
     * Check if a string parses as a datetime. We filter out 4-digit years accepted by strict_date_optional_time
     * and other Iso8601 parsers where {@code MONTH_OF_YEAR} is optional. These are the only 4-digit values they
     * accept, and we don't want to treat an all-4-digit column as DATETIME.
     */
    private boolean isDateTimeString(String text) {
        if (dateFormatter == STRICT_DATE_OPTIONAL_TIME) {
            if (text.length() == 4 && text.chars().allMatch(Character::isDigit)) {
                return false;
            }
        }
        return dateFormatter.tryParse(text) != null;
    }
}
