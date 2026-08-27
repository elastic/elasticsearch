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

    private NdJsonSchemaInferrer(DateFormatter dateFormatter) {
        this.dateFormatter = dateFormatter != null ? dateFormatter : STRICT_DATE_OPTIONAL_TIME;
    }

    /**
     * Infers schema from an NDJSON input stream, reading up to maxLines.
     * When {@code datetimeFormatter} is null, falls back to {@link #STRICT_DATE_OPTIONAL_TIME}.
     */
    public static List<Attribute> inferSchema(InputStream inputStream, int maxLines, DateFormatter datetimeFormatter) throws IOException {
        return new NdJsonSchemaInferrer(datetimeFormatter).doInferSchema(inputStream, maxLines);
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
                    inferStringType(field, parser.getText());
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
            resolved = resolved == null ? type : TypeWidening.join(resolved, type, TypeWidening.Policy.INFERENCE);
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
