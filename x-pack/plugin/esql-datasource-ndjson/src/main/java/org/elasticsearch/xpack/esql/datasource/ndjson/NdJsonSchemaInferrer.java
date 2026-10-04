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
import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;
import org.elasticsearch.xpack.esql.datasources.spi.TemporalInference;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.TemporalAccessor;
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
 * Types: KEYWORD, INTEGER, LONG, DOUBLE, BOOLEAN, DATETIME, DATE_NANOS.
 *
 * <p>A timestamp is inferred DATE_NANOS only when it carries a non-zero sub-millisecond component,
 * which DATETIME would silently drop; see {@link TemporalInference}.
 *
 * <p>Column names are always flat dotted names, whichever way the file spells them: the two spellings of a dotted
 * column are one path through the field tree ({@link #childFor}), so mixing them across records infers one column.
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
     * and whether they stay charged after that is the caller's choice: the multi-file gather and the schema interner
     * charge what they keep, while a single-file resolve and a data-node read keep it uncharged. A flattened nested field
     * is named by its whole dotted path, so the column list can be orders of magnitude larger than the input that
     * produced it. A {@code CircuitBreakingException} propagates unchanged and stops inference. It is not a malformed
     * line, so it must never be caught as one.
     * <p>
     * More than {@code maxFields} fields, objects and leaves alike, fails inference with a client error naming
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
        NdJsonUtils.LineTerminatorTrackingStream tracking = new NdJsonUtils.LineTerminatorTrackingStream(inputStream);
        JsonParser parser = NdJsonUtils.JSON_FACTORY.createParser(tracking);
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
                    // failing inference on it would deny the read's error_mode a say. A record that
                    // names one field twice (NdJsonUtils.JSON_FACTORY enables Jackson's duplicate
                    // detection) arrives here as a JsonParseException and defers for the same reason:
                    // the fields it introduced are discarded, so it contributes no columns to the sample,
                    // and the slice read is where it either fails the query or drops with a warning.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, tracking);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                    continue;
                }

                int lineStart = fields.size();
                try {
                    inferObjectSchema(parser, root);
                    lineCount++;
                } catch (JsonParseException | StreamConstraintsException e) {
                    // See comment above: deferred to the slice read for policy-driven handling.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    discardFieldsFrom(lineStart);
                    inputStream = NdJsonUtils.moveToNextLine(parser, tracking);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                } catch (FieldCapExceeded e) {
                    // Fields are created while the line is still being parsed, so the line that crossed the cap may
                    // yet turn out to be malformed. Only a well-formed line fails inference; a malformed one is
                    // skipped like any other, without its fields counting toward the cap.
                    if (restOfRecordParses(parser)) {
                        throw new IllegalArgumentException(fieldCapMessage(maxFields));
                    }
                    logger.debug("Malformed NDJSON at line {} past the field cap", lineCount);
                    discardFieldsFrom(lineStart);
                    inputStream = NdJsonUtils.moveToNextLine(parser, tracking);
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
        } catch (JsonParseException | StreamConstraintsException e) {
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
            "NDJSON schema inference found more than [{}] fields; raise [{}] in the dataset settings or the "
                + "WITH clause to infer a wider schema",
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
            var child = childFor(object, parser.getCurrentName());
            parser.nextToken();
            inferValueSchema(parser, child);
        }
    }

    /**
     * The node a field name addresses within {@code object}. A dotted name is a path, so both spellings of a dotted
     * column ({@code {"a.b":1}} and {@code {"a":{"b":1}}}) land on one node and a file that mixes them infers one
     * column rather than two attributes with the same name.
     */
    private static FieldInfo childFor(FieldInfo object, String fieldName) {
        if (NdJsonUtils.isFieldPath(fieldName) == false) {
            return object.getChild(fieldName);
        }
        FieldInfo node = object;
        int start = 0;
        int dot;
        while ((dot = fieldName.indexOf('.', start)) >= 0) {
            node = node.getChild(fieldName.substring(start, dot));
            start = dot + 1;
        }
        return node.getChild(fieldName.substring(start));
    }

    private void inferValueSchema(JsonParser parser, FieldInfo field) throws IOException {
        switch (parser.currentToken()) {
            case START_ARRAY -> {
                field.isArray = true;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    inferValueSchema(parser, field);
                }
            }
            case START_OBJECT -> inferObjectSchema(parser, field);
            case VALUE_STRING -> inferStringType(field, parser.getText());
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

    /**
     * Build the list of Attribute by walking the FieldInfo tree depth first. A dotted key is split into one node per
     * segment, which the parser's nesting cap does not bound, so the walk keeps its own stack rather than recursing and
     * spells the dotted path in one shared buffer rather than building a string per ancestor. Only a column's own name
     * is materialized, and it is charged from its length before it is built, so a refusal never follows the allocation
     * it was meant to prevent. Each stack frame is covered by its node's {@link #FIELD_INFO_BYTES}.
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
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullable ? Nullability.TRUE : Nullability.UNKNOWN, null, false);
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
            return resolveObservedTypes(types);
        }
    }

    /**
     * The single type that represents everything observed for one field.
     * <p>
     * The rule is {@link TypeWidening}'s, folded over the observed set: this rail decides which types
     * it saw, not what they combine to, and the combining is the same question reconciliation answers
     * when two files disagree. Folding in any order is safe because the lattice is a join-semilattice,
     * which matters here — a JSON field's types arrive in whatever order the file happens to list them.
     * <p>
     * An empty set means the field was only ever an object or an always-empty array, which is not a
     * scalar column at all; that is this method's answer to give because the lattice has no bottom
     * element to represent "nothing observed".
     */
    static DataType resolveObservedTypes(EnumSet<DataType> observed) {
        if (observed.isEmpty()) {
            // Can happen with parent and always-empty array
            return DataType.UNSUPPORTED;
        }
        DataType resolved = null;
        for (DataType type : observed) {
            resolved = resolved == null ? type : TypeWidening.join(resolved, type);
        }
        return resolved;
    }

    /**
     * Types one string value.
     * <p>
     * Kept out of {@link #inferValueSchema} deliberately. That method carries the per-value token
     * switch for every field of every sampled line, and it is small enough for the JIT to inline;
     * growing it with this body measurably slowed the whole switch, including the string field that
     * never reaches the date parse at all.
     * <p>
     * The KEYWORD short-circuit is what keeps a string field cheap: once a field is known to hold
     * strings, no later value pays a date parse. Without it every sampled value of a keyword column
     * would be parsed as a date and the result thrown away.
     */
    private void inferStringType(FieldInfo field, String text) {
        if (field.types.contains(DataType.KEYWORD)) {
            field.addType(DataType.KEYWORD);
            return;
        }
        TemporalAccessor parsed = tryParseDateTime(text);
        field.addType(parsed == null ? DataType.KEYWORD : forcesDateNanos(parsed) ? DataType.DATE_NANOS : DataType.DATETIME);
    }

    /**
     * Parses a string as a datetime, returning the parse result so the caller can tell millisecond
     * timestamps from nanosecond ones without paying a second parse. Returns null when the string is
     * not a datetime at all. We filter out 4-digit years accepted by strict_date_optional_time
     * and other Iso8601 parsers where {@code MONTH_OF_YEAR} is optional. These are the only 4-digit values they
     * accept, and we don't want to treat an all-4-digit column as DATETIME.
     */
    private TemporalAccessor tryParseDateTime(String text) {
        if (dateFormatter == STRICT_DATE_OPTIONAL_TIME) {
            if (text.length() == 4 && text.chars().allMatch(Character::isDigit)) {
                return null;
            }
        }
        return dateFormatter.tryParse(text);
    }

    /**
     * Whether a parsed timestamp must be read as {@code date_nanos} to survive intact.
     * <p>
     * Only asked on the default ISO rail, mirroring the 4-digit-year filter above: when the file
     * declares its own {@code datetime_format} the user has expressed intent about how their
     * timestamps are written, and declaring the schema is the way to ask for nanoseconds. It also
     * keeps us from flipping a column onto a decode rail that the custom pattern may not parse.
     */
    private boolean forcesDateNanos(TemporalAccessor parsed) {
        return dateFormatter == STRICT_DATE_OPTIONAL_TIME && TemporalInference.forcesDateNanos(parsed);
    }
}
