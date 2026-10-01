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

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.TemporalInference;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.TemporalAccessor;
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

    private final DateFormatter dateFormatter;
    /** Set once at the start of {@link #doInferSchema}; {@link FieldInfo#addType} reports into it directly. */
    private List<Widening> widenings;

    private NdJsonSchemaInferrer(DateFormatter dateFormatter) {
        this.dateFormatter = dateFormatter != null ? dateFormatter : STRICT_DATE_OPTIONAL_TIME;
    }

    /**
     * One within-file widen worth reporting: a field whose inferred type moved because its sampled
     * values disagree — a fold to {@link DataType#KEYWORD}, or a {@code long}/{@code double} merge.
     * Mirrors {@code CsvSchemaInferrer.Widening}, feeding the same warning channel
     * ({@code NdJsonFormatReader}) and the same {@link org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata#widenedColumns()}.
     *
     * @param row 1-based record number, within the inference sample, that carried {@code value}
     */
    public record Widening(String columnName, DataType fromType, DataType toType, String value, int row) {}

    /**
     * Infers schema from an NDJSON input stream, reading up to maxLines.
     * When {@code datetimeFormatter} is null, falls back to {@link #STRICT_DATE_OPTIONAL_TIME}.
     */
    public static List<Attribute> inferSchema(InputStream inputStream, int maxLines, DateFormatter datetimeFormatter) throws IOException {
        return inferSchema(inputStream, maxLines, datetimeFormatter, new ArrayList<>());
    }

    /** As above, reporting every within-sample widen worth surfacing to the user into {@code widenings}. */
    public static List<Attribute> inferSchema(
        InputStream inputStream,
        int maxLines,
        DateFormatter datetimeFormatter,
        List<Widening> widenings
    ) throws IOException {
        return new NdJsonSchemaInferrer(datetimeFormatter).doInferSchema(inputStream, maxLines, widenings);
    }

    private List<Attribute> doInferSchema(InputStream inputStream, int maxLines, List<Widening> widenings) throws IOException {
        this.widenings = widenings;
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
                    // it contributes no columns to the sample, and the slice read is where it either
                    // fails the query or drops with a warning.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
                    inputStream = NdJsonUtils.moveToNextLine(parser, tracking);
                    parser = NdJsonUtils.JSON_FACTORY.createParser(inputStream);
                    continue;
                }

                try {
                    inferObjectSchema(parser, root);
                    lineCount++;
                } catch (JsonParseException | StreamConstraintsException e) {
                    // See comment above: deferred to the slice read for policy-driven handling.
                    logger.debug("Malformed NDJSON at line {}: {}", lineCount, e);
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
                        field.addType(DataType.INTEGER, lineCount + 1, parser.getText());
                        return;
                    case LONG:
                        field.addType(DataType.LONG, lineCount + 1, parser.getText());
                        return;
                    case BIG_INTEGER: {
                        field.addType(DataType.DOUBLE, lineCount + 1, parser.getText());
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
            case VALUE_NUMBER_FLOAT -> field.addType(DataType.DOUBLE, lineCount + 1, parser.getText()); // conservative size
            case VALUE_TRUE, VALUE_FALSE -> field.addType(DataType.BOOLEAN, lineCount + 1, parser.getText());
            case VALUE_NULL -> field.nullable = true;
            // Ignore all other events
        }
    }

    /** Build the list of Attribute by recursively traversing the FieldInfo tree. */
    private static void buildSchema(FieldInfo field, String parentName, List<Attribute> attributes) {
        if (field.children == null) {
            // No children were ever observed. Happens for the root when every sampled line was
            // malformed (so {@link FieldInfo#getChild} was never called), or legitimately for
            // leaf fields during recursion. Nothing to contribute to the schema either way.
            return;
        }
        for (Map.Entry<String, FieldInfo> entry : field.children.entrySet()) {
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
        /** Dotted path from the root, or {@code null} for the root itself; labels a reported {@link Widening}. */
        final String fullName;
        /**
         * The single type that represents every value seen for this field so far, folded in arrival order —
         * unlike {@link #types} (an unordered set, resolved only at the end by {@link #resolveType}), this is
         * updated incrementally so {@link #addType} can tell exactly which value moved it and to what, the
         * same question {@code CsvSchemaInferrer.narrowCandidate} answers for CSV. {@code null} until the
         * first non-null value.
         */
        DataType runningType;

        FieldInfo(String name, String fullName) {
            this.name = name;
            this.fullName = fullName;
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
            return children.computeIfAbsent(name, (n) -> new FieldInfo(n, fullName == null ? n : fullName + "." + n));
        }

        /**
         * Records one sampled value's type, updating the running fold and reporting a {@link Widening} the
         * moment it moves to {@link DataType#KEYWORD} or completes a {@code long}/{@code double} merge —
         * gated exactly like {@code SchemaReconciliation}'s cross-file emitters
         * ({@code emitKeywordFallbackWarnings} / {@code emitPrecisionLossWarnings}): a lossless promotion
         * (e.g. {@code integer -> long}) stays silent. The first value a field ever sees never widens
         * anything (there is nothing yet to move away from), matching
         * {@code CsvSchemaInferrer.narrowCandidate}'s treatment of an unconfirmed column.
         * <p>
         * A type already contributing to the fold changes nothing if seen again — {@code join} is
         * idempotent — so re-seeing it is skipped before the join, both as an optimization and because
         * {@code updated != previous} below only ever holds on a genuinely new type: {@code previous}
         * already absorbed every type seen so far, so joining it with one of those again is a no-op.
         * <p>
         * The long/double merge check is deliberately membership-based ({@code hadBothLongAndDouble},
         * computed from {@link #types} before this call's type is added) rather than keyed off whether
         * {@code updated} itself changed: a field already resolved to {@code DOUBLE} from a genuine
         * decimal, that later sees a value that is <em>also</em> exactly long-representable, has
         * {@code updated == previous == DOUBLE} — the join is a no-op — even though this is exactly the
         * cross-file-equivalent case ({@code DOUBLE} unified from both {@code LONG} and {@code DOUBLE}
         * contributors). Checking {@code previous}/{@code type} against the two rungs directly (as a
         * transition-only check once did) misses that order. {@code fromType} reports {@code LONG} (this
         * value's own shape) rather than {@code previous} whenever {@code previous} already equals
         * {@code updated}, since reporting {@code fromType == toType == DOUBLE} would say nothing useful.
         */
        void addType(DataType type, int row, String value) {
            fieldsSeen.set(idx);
            boolean hadBothLongAndDouble = types.contains(DataType.LONG) && types.contains(DataType.DOUBLE);
            if (types.add(type) == false) {
                return;
            }
            DataType previous = runningType;
            DataType updated = previous == null ? type : TypeWidening.join(previous, type);
            if (previous != null) {
                boolean becameKeyword = updated == DataType.KEYWORD && updated != previous;
                boolean becameLongDoubleMerge = updated == DataType.DOUBLE
                    && hadBothLongAndDouble == false
                    && types.contains(DataType.LONG)
                    && types.contains(DataType.DOUBLE);
                if (becameKeyword) {
                    widenings.add(new Widening(fullName, previous, updated, value, row));
                } else if (becameLongDoubleMerge) {
                    DataType fromType = previous == updated ? type : previous;
                    widenings.add(new Widening(fullName, fromType, updated, value, row));
                }
            }
            runningType = updated;
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
            field.addType(DataType.KEYWORD, lineCount + 1, text);
            return;
        }
        TemporalAccessor parsed = tryParseDateTime(text);
        DataType type = parsed == null ? DataType.KEYWORD : forcesDateNanos(parsed) ? DataType.DATE_NANOS : DataType.DATETIME;
        field.addType(type, lineCount + 1, text);
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
