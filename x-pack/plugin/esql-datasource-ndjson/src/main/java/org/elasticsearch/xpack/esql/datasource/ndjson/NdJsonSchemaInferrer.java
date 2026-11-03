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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.EnumSet;
import java.util.Iterator;
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
    public static final DateFormatter STRICT_DATE_OPTIONAL_TIME = DateFormatter.forPattern("strict_date_optional_time");

    private static final Logger logger = LogManager.getLogger(NdJsonSchemaInferrer.class);

    private static final EnumSet<DataType> NUMBER_TYPES = EnumSet.of(DataType.DOUBLE, DataType.LONG, DataType.INTEGER);

    // Fields that we've actually seen in the current json document
    private final BitSet fieldsSeen = new BitSet();
    private final List<FieldInfo> fields = new ArrayList<>();
    private int lineCount = 0;

    /**
     * Allowance for one {@link FieldInfo}: the object, its {@link EnumSet}, and its slot in {@link #fields} and in the
     * parent's children map. The field name is charged on top. Not a measured deep size.
     */
    static final long FIELD_INFO_BYTES = 256L;

    /** Label the inference charges are made under, so a trip names the work that was refused. */
    static final String BREAKER_LABEL = "ndjson_schema_inference";

    private final int maxFields;
    private final DateFormatter dateFormatter;
    private final CircuitBreaker breaker;
    private long reservedBytes = 0;

    private NdJsonSchemaInferrer(int maxFields, DateFormatter dateFormatter, CircuitBreaker breaker) {
        this.maxFields = maxFields;
        this.dateFormatter = dateFormatter != null ? dateFormatter : STRICT_DATE_OPTIONAL_TIME;
        this.breaker = breaker;
    }

    /**
     * Infers schema from an NDJSON input stream, reading up to maxLines.
     * When {@code datetimeFormatter} is null, falls back to {@link #STRICT_DATE_OPTIONAL_TIME}.
     * <p>
     * The field tree and the column list built from it are charged to {@code breaker} while they exist, and released
     * before returning, so the breaker bounds a schema while it is built. The returned attributes belong to the caller,
     * and whether they stay charged after that is the caller's choice. A flattened nested field
     * is named by its whole dotted path, so the column list can be orders of magnitude larger than the input that
     * produced it. A {@code CircuitBreakingException} propagates unchanged and stops inference. It is not a malformed
     * line, so it must never be caught as one.
     * <p>
     * More than {@code maxFields} fields, objects and leaves alike, fails inference with an {@code ExternalClientException} (400) naming
     * {@code schema_max_fields}. Like the breaker, that is not a malformed line: it stops inference at once. Fields are
     * counted as a line is parsed, so the line that crosses the cap is read to its end first, and if it turns out to be
     * malformed it is skipped like any other and its fields are discarded.
     */
    public static List<Attribute> inferSchema(
        InputStream inputStream,
        int maxLines,
        int maxFields,
        DateFormatter datetimeFormatter,
        CircuitBreaker breaker
    ) throws IOException {
        NdJsonSchemaInferrer inferrer = new NdJsonSchemaInferrer(maxFields, datetimeFormatter, breaker);
        try {
            return inferrer.doInferSchema(inputStream, maxLines);
        } finally {
            inferrer.breaker.addWithoutBreaking(-inferrer.reservedBytes);
        }
    }

    private void charge(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
        reservedBytes += bytes;
    }

    private List<Attribute> doInferSchema(InputStream inputStream, int maxLines) throws IOException {
        FieldInfo root = new FieldInfo(null, null);
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

                int lineStart = fields.size();
                try {
                    inferObjectSchema(parser, root);
                    lineCount++;
                } catch (JsonParseException e) {
                    // See comment above: deferred to the slice read for policy-driven handling.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    discardFieldsFrom(lineStart);
                    inputStream = NdJsonUtils.moveToNextLine(parser, inputStream);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                } catch (FieldCapExceeded e) {
                    // Fields are created while the line is still being parsed, so the line that crossed the cap may
                    // yet turn out to be malformed. Only a well-formed line fails inference; a malformed one is
                    // skipped like any other, without its fields counting toward the cap.
                    if (restOfRecordParses(parser)) {
                        // A deterministic limit, so a 400; typed rather than an IllegalArgumentException so that every
                        // rail keeps the message, and the same refusal CSV, TSV and Parquet give.
                        throw ExternalClientException.schemaTooWide(fieldCapMessage(maxFields));
                    }
                    logger.debug("Malformed NDJSON at line {} past the field cap", lineCount);
                    discardFieldsFrom(lineStart);
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
        buildSchema(root, attributes);
        return attributes;
    }

    /**
     * Removes the fields created since {@code start}, children before their parents, and releases their charges. A
     * malformed line's partial record must not leave columns behind.
     */
    private void discardFieldsFrom(int start) {
        for (int i = fields.size() - 1; i >= start; i--) {
            FieldInfo field = fields.remove(i);
            field.parent.children.remove(field.name);
            if (field.parent.children.isEmpty()) {
                // getChild only creates the map to add a child, so an empty one was created by this line.
                field.parent.children = null;
            }
            long bytes = fieldBytes(field.name);
            breaker.addWithoutBreaking(-bytes);
            reservedBytes -= bytes;
        }
    }

    /**
     * Reads the rest of the current record without building fields, to tell whether the line that crossed the field cap
     * is well formed. A record cut short at the end of the stream counts as malformed.
     */
    private static boolean restOfRecordParses(JsonParser parser) throws IOException {
        try {
            while (parser.getParsingContext().inRoot() == false) {
                if (parser.nextToken() == null) {
                    return false;
                }
            }
            return true;
        } catch (JsonParseException e) {
            return false;
        }
    }

    /**
     * The refusal for a schema over {@code maxFields}. Below the ceiling the user can raise the cap; at the ceiling
     * raising it is rejected too, so say the file is wider than any schema inference supports instead.
     */
    static String fieldCapMessage(int maxFields) {
        if (maxFields >= ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS) {
            return LoggerMessageFormat.format(
                "NDJSON schema inference found more than [{}] fields, the most [{}] allows; declare the dataset's "
                    + "columns with [dynamic: false] to skip inference",
                maxFields,
                NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS
            );
        }
        return LoggerMessageFormat.format(
            "NDJSON schema inference found more than [{}] fields; raise [esql.external.schema_max_fields] or the dataset's [{}] "
                + "to infer a wider schema",
            maxFields,
            NdJsonFormatReader.CONFIG_SCHEMA_MAX_FIELDS
        );
    }

    private static long fieldBytes(String name) {
        return FIELD_INFO_BYTES + HeapEstimates.stringBytes(name);
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
            var child = object.getChild(parser.getCurrentName());
            parser.nextToken();
            inferValueSchema(parser, child);
        }
    }

    private void inferValueSchema(JsonParser parser, FieldInfo field) throws IOException {
        switch (parser.currentToken()) {
            case START_ARRAY -> {
                field.isArray = true;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    inferValueSchema(parser, field);
                }
            }
            // Keep in sync with NdJsonPageDecoder.BlockDecoder.decodeValue. A field seen as both a
            // scalar and an object across sampled records resolves to whichever shape was observed
            // first (mirrors core ES dynamic mapping's first-writer-wins); the other shape is ignored
            // here for schema-inference purposes so buildSchema never emits both a scalar attribute
            // and nested children for the same name (elastic/esql-planning#1028). The decoder applies
            // ErrorPolicy to the actual conflicting value at read time.
            case START_OBJECT -> {
                if (field.types.isEmpty() == false) {
                    parser.skipChildren();
                } else {
                    inferObjectSchema(parser, field);
                }
            }
            case VALUE_STRING -> {
                if (field.children == null) {
                    String text = parser.getText();
                    if (field.types.contains(DataType.KEYWORD) == false && isDateTimeString(text)) {
                        field.addType(DataType.DATETIME);
                    } else {
                        field.addType(DataType.KEYWORD);
                    }
                }
            }
            case VALUE_NUMBER_INT -> {
                if (field.children == null) {
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
                if (field.children == null) {
                    field.addType(DataType.DOUBLE); // conservative size
                }
            }
            case VALUE_TRUE, VALUE_FALSE -> {
                if (field.children == null) {
                    field.addType(DataType.BOOLEAN);
                }
            }
            case VALUE_NULL -> field.nullable = true;
            // Ignore all other events
        }
    }

    /**
     * Build the list of Attribute by walking the FieldInfo tree depth first. The walk keeps its own stack rather than
     * recursing and spells the dotted path in one shared buffer rather than building a string per ancestor. Only a
     * column's own name is materialized, and it is charged from its length before it is built, so a refusal never
     * follows the allocation it was meant to prevent. Each stack frame is covered by its node's {@link #FIELD_INFO_BYTES}.
     */
    private void buildSchema(FieldInfo root, List<Attribute> attributes) {
        if (root.children == null) {
            // No children were ever observed. Happens when every sampled line was malformed (so
            // {@link FieldInfo#getChild} was never called). Nothing to contribute to the schema.
            return;
        }
        StringBuilder path = new StringBuilder();
        int chargedPathLength = 0;
        Deque<SchemaFrame> stack = new ArrayDeque<>();
        stack.push(new SchemaFrame(root.children.entrySet().iterator(), NO_PARENT_PATH));
        while (stack.isEmpty() == false) {
            SchemaFrame frame = stack.peek();
            if (frame.children().hasNext() == false) {
                stack.pop();
                continue;
            }
            Map.Entry<String, FieldInfo> entry = frame.children().next();
            String name = entry.getKey();
            FieldInfo info = entry.getValue();
            int pathLength = frame.pathLength() == NO_PARENT_PATH ? name.length() : frame.pathLength() + 1 + name.length();
            if (pathLength > chargedPathLength) {
                // Two bytes per character also covers the builder's doubling growth for Latin-1 names.
                charge((pathLength - chargedPathLength) * (long) Character.BYTES);
                chargedPathLength = pathLength;
            }
            path.setLength(Math.max(frame.pathLength(), 0));
            if (frame.pathLength() != NO_PARENT_PATH) {
                path.append('.');
            }
            path.append(name);

            DataType dataType = info.resolveType();
            if (dataType != DataType.UNSUPPORTED) {
                // Unsupported is used for nested object properties
                charge(HeapEstimates.columnBytes(pathLength));
                attributes.add(attribute(path.toString(), dataType, info.nullable));
            }

            if (info.children != null) {
                stack.push(new SchemaFrame(info.children.entrySet().iterator(), pathLength));
            }
        }
    }

    /**
     * Path length of the root's children's parent. Distinct from 0 because an empty segment is a real parent whose
     * children are still separated from it by a dot.
     */
    private static final int NO_PARENT_PATH = -1;

    /** One level of {@link #buildSchema}'s walk: the children still to visit and the length of their parent's path. */
    private record SchemaFrame(Iterator<Map.Entry<String, FieldInfo>> children, int pathLength) {}

    public static Attribute attribute(String name, DataType type, boolean nullable) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullable ? Nullability.TRUE : Nullability.FALSE, null, false);
    }

    /**
     * Thrown when a line would create more than {@code maxFields} fields. Not a client error yet: {@link #doInferSchema}
     * first checks whether the line is malformed, which defers to the slice read instead. Carries no stack trace, since
     * it never leaves this class.
     */
    private static final class FieldCapExceeded extends RuntimeException {
        FieldCapExceeded() {
            super(null, null, false, false);
        }
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
        final FieldInfo parent;
        final String name;

        FieldInfo(FieldInfo parent, String name) {
            // fields holds the root too, so this admits exactly maxFields fields below it.
            if (fields.size() > maxFields) {
                throw new FieldCapExceeded();
            }
            charge(fieldBytes(name));
            this.parent = parent;
            this.name = name;
            this.idx = fields.size();
            fields.add(this);
            if (lineCount > 0) {
                // Field appearing after the first lines.
                nullable = true;
            }
        }

        FieldInfo getChild(String name) {
            if (children == null) {
                children = new LinkedHashMap<>();
            }
            return children.computeIfAbsent(name, (n) -> new FieldInfo(this, n));
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
