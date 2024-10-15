/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public enum DataType {
    /**
     * Fields of this type are unsupported by any functions and are always
     * rendered as {@code null} in the response.
     */
    UNSUPPORTED(builder().typeName("UNSUPPORTED").unknownSize()),
    /**
     * Fields that are always {@code null}, usually created with constant
     * {@code null} values.
     */
    NULL(builder().esType("null").estimatedSize(0)),
    /**
     * Fields that can either be {@code true} or {@code false}.
     */
    BOOLEAN(builder().esType("boolean").estimatedSize(1)),

    /**
     * 64-bit signed numbers labeled as metric counters in time-series indices.
     * Although stored internally as numeric fields, they represent cumulative
     * metrics and must not be treated as regular numeric fields. Therefore,
     * we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate
     * aggregation, and casting to their parent numeric type.
     */
    COUNTER_LONG(builder().esType("counter_long").estimatedSize(Long.BYTES).docValues().counter()),
    /**
     * 32-bit signed numbers labeled as metric counters in time-series indices.
     * Although stored internally as numeric fields, they represent cumulative
     * metrics and must not be treated as regular numeric fields. Therefore,
     * we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate
     * aggregation, and casting to their parent numeric type.
     */
    COUNTER_INTEGER(builder().esType("counter_integer").estimatedSize(Integer.BYTES).docValues().counter()),
    /**
     * 64-bit floating point numbers labeled as metric counters in time-series indices.
     * Although stored internally as numeric fields, they represent cumulative
     * metrics and must not be treated as regular numeric fields. Therefore,
     * we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate
     * aggregation, and casting to their parent numeric type.
     */
    COUNTER_DOUBLE(builder().esType("counter_double").estimatedSize(Double.BYTES).docValues().counter()),

    /**
     * 64-bit signed numbers loaded as a java {@code long}.
     */
    LONG(builder().esType("long").estimatedSize(Long.BYTES).wholeNumber().docValues().counter(COUNTER_LONG)),
    /**
     * 32-bit signed numbers loaded as a java {@code int}.
     */
    INTEGER(builder().esType("integer").estimatedSize(Integer.BYTES).wholeNumber().docValues().counter(COUNTER_INTEGER)),
    /**
     * 64-bit unsigned numbers packed into a java {@code long}.
     */
    UNSIGNED_LONG(builder().esType("unsigned_long").estimatedSize(Long.BYTES).wholeNumber().docValues()),
    /**
     * 64-bit floating point number loaded as a java {@code double}.
     */
    DOUBLE(builder().esType("double").estimatedSize(Double.BYTES).rationalNumber().docValues().counter(COUNTER_DOUBLE)),

    /**
     * 16-bit signed numbers widened on load to {@link #INTEGER}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    SHORT(builder().esType("short").estimatedSize(Short.BYTES).wholeNumber().docValues().widenSmallNumeric(INTEGER)),
    /**
     * 8-bit signed numbers widened on load to {@link #INTEGER}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    BYTE(builder().esType("byte").estimatedSize(Byte.BYTES).wholeNumber().docValues().widenSmallNumeric(INTEGER)),
    /**
     * 32-bit floating point numbers widened on load to {@link #DOUBLE}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    FLOAT(builder().esType("float").estimatedSize(Float.BYTES).rationalNumber().docValues().widenSmallNumeric(DOUBLE)),
    /**
     * 16-bit floating point numbers widened on load to {@link #DOUBLE}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    HALF_FLOAT(builder().esType("half_float").estimatedSize(Float.BYTES).rationalNumber().docValues().widenSmallNumeric(DOUBLE)),
    /**
     * Signed 64-bit fixed point numbers converted on load to a {@link #DOUBLE}.
     * Values of this type never escape type resolution and functions, operators,
     * and results should never encounter one.
     */
    SCALED_FLOAT(builder().esType("scaled_float").estimatedSize(Long.BYTES).rationalNumber().docValues().widenSmallNumeric(DOUBLE)),

    /**
     * String fields that are analyzed when the document is received but never
     * cut into more than one token. ESQL always loads these after-analysis.
     * Generally ESQL uses {@code keyword} fields as raw strings. So things like
     * {@code TO_STRING} will make a {@code keyword} field.
     */
    KEYWORD(builder().esType("keyword").unknownSize().docValues()),
    /**
     * String fields that are analyzed when the document is received and may be
     * cut into more than one token. Generally ESQL only sees {@code text} fields
     * when loaded from the index and ESQL will load these fields
     * <strong>without</strong> analysis. The {@code MATCH} operator can be used
     * to query these fields with analysis.
     */
    TEXT(builder().esType("text").unknownSize()),
    /**
     * Millisecond precision date, stored as a 64-bit signed number.
     */
    DATETIME(builder().esType("date").typeName("DATETIME").estimatedSize(Long.BYTES).docValues()),
    /**
     * Nanosecond precision date, stored as a 64-bit signed number.
     */
    DATE_NANOS(builder().esType("date_nanos").estimatedSize(Long.BYTES).docValues()),
    /**
     * IP addresses. IPv4 address are always
     * <a href="https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5">embedded</a>
     * in IPv6. These flow through the compute engine as fixed length, 16 byte
     * {@link BytesRef}s.
     */
    IP(builder().esType("ip").estimatedSize(16).docValues()),
    /**
     * A version encoded in a way that sorts using semver.
     */
    // 8.15.2-SNAPSHOT is 15 bytes, most are shorter, some can be longer
    VERSION(builder().esType("version").estimatedSize(15).docValues()),
    OBJECT(builder().esType("object").unknownSize()),
    SOURCE(builder().esType(SourceFieldMapper.NAME).unknownSize()),
    DATE_PERIOD(builder().typeName("DATE_PERIOD").estimatedSize(3 * Integer.BYTES)),
    TIME_DURATION(builder().typeName("TIME_DURATION").estimatedSize(Integer.BYTES + Long.BYTES)),
    // WKB for points is typically 21 bytes.
    GEO_POINT(builder().esType("geo_point").estimatedSize(21).docValues()),
    CARTESIAN_POINT(builder().esType("cartesian_point").estimatedSize(21).docValues()),
    // wild estimate for size, based on some test data (airport_city_boundaries)
    CARTESIAN_SHAPE(builder().esType("cartesian_shape").estimatedSize(200).docValues()),
    GEO_SHAPE(builder().esType("geo_shape").estimatedSize(200).docValues()),

    /**
     * Fields with this type represent a Lucene doc id. This field is a bit magic in that:
     * <ul>
     *     <li>One copy of it is always added at the start of every query</li>
     *     <li>It is implicitly dropped before being returned to the user</li>
     *     <li>It is not "target-able" by any functions</li>
     *     <li>Users shouldn't know it's there at all</li>
     *     <li>It is used as an input for things that interact with Lucene like
     *         loading field values</li>
     * </ul>
     */
    DOC_DATA_TYPE(builder().esType("_doc").estimatedSize(Integer.BYTES * 3)),
    /**
     * Fields with this type represent values from the {@link TimeSeriesIdFieldMapper}.
     * Every document in {@link IndexMode#TIME_SERIES} index will have a single value
     * for this field and the segments themselves are sorted on this value.
     */
    TSID_DATA_TYPE(builder().esType("_tsid").unknownSize().docValues()),
    /**
     * Fields with this type are the partial result of running a non-time-series aggregation
     * inside alongside time-series aggregations. These fields are not parsable from the
     * mapping and should be hidden from users.
     */
    PARTIAL_AGG(builder().esType("partial_agg").unknownSize());

    /**
     * Types that are actively being built. These types are not returned
     * from Elasticsearch if their associated {@link FeatureFlag} is disabled.
     * They aren't included in generated documentation. And the tests don't
     * check that sending them to a function produces a sane error message.
     */
    public static final Map<DataType, FeatureFlag> UNDER_CONSTRUCTION = Map.ofEntries(
        Map.entry(DATE_NANOS, EsqlCorePlugin.DATE_NANOS_FEATURE_FLAG)
    );

    private final String typeName;

    private final String name;

    private final String esType;

    private final Optional<Integer> estimatedSize;

    /**
     * True if the type represents a "whole number", as in, does <strong>not</strong> have a decimal part.
     */
    private final boolean isWholeNumber;

    /**
     * True if the type represents a "rational number", as in, <strong>does</strong> have a decimal part.
     */
    private final boolean isRationalNumber;

    /**
     * True if the type supports doc values by default
     */
    private final boolean docValues;

    /**
     * {@code true} if this is a TSDB counter, {@code false} otherwise.
     */
    private final boolean isCounter;

    /**
     * If this is a "small" numeric type this contains the type ESQL will
     * widen it into, otherwise this is {@code null}.
     */
    private final DataType widenSmallNumeric;

    /**
     * If this is a representable numeric this will be the counter "version"
     * of this numeric, otherwise this is {@code null}.
     */
    private final DataType counter;

    DataType(Builder builder) {
        String typeString = builder.typeName != null ? builder.typeName : builder.esType;
        assert builder.estimatedSize != null : "Missing size for type " + typeString;
        this.typeName = typeString.toLowerCase(Locale.ROOT);
        this.name = typeString.toUpperCase(Locale.ROOT);
        this.esType = builder.esType;
        this.estimatedSize = builder.estimatedSize;
        this.isWholeNumber = builder.isWholeNumber;
        this.isRationalNumber = builder.isRationalNumber;
        this.docValues = builder.docValues;
        this.isCounter = builder.isCounter;
        this.widenSmallNumeric = builder.widenSmallNumeric;
        this.counter = builder.counter;
    }

    private static final Collection<DataType> TYPES = Arrays.stream(values())
        .filter(d -> d != DOC_DATA_TYPE && d != TSID_DATA_TYPE)
        .sorted(Comparator.comparing(DataType::typeName))
        .toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static final Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        // TODO: Why don't we use the names ES uses as the esType field for these?
        // ES calls this 'point', but ESQL calls it 'cartesian_point'
        map.put("point", DataType.CARTESIAN_POINT);
        map.put("shape", DataType.CARTESIAN_SHAPE);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private static final Map<String, DataType> NAME_OR_ALIAS_TO_TYPE;
    static {
        Map<String, DataType> map = DataType.types().stream().collect(toMap(DataType::typeName, Function.identity()));
        map.put("bool", BOOLEAN);
        map.put("int", INTEGER);
        map.put("string", KEYWORD);
        NAME_OR_ALIAS_TO_TYPE = Collections.unmodifiableMap(map);
    }

    public static Collection<DataType> types() {
        return TYPES;
    }

    /**
     * Resolve a type from a name. This name is sometimes user supplied,
     * like in the case of {@code ::<typename>} and is sometimes the name
     * used over the wire, like in {@link #readFrom(String)}.
     */
    public static DataType fromTypeName(String name) {
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.ROOT));
    }

    public static DataType fromEs(String name) {
        DataType type = ES_TO_TYPE.get(name);
        if (type == null) {
            return UNSUPPORTED;
        }
        FeatureFlag underConstruction = UNDER_CONSTRUCTION.get(type);
        if (underConstruction != null && underConstruction.isEnabled() == false) {
            return UNSUPPORTED;
        }
        return type;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof BigInteger) {
            return UNSIGNED_LONG;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof Byte) {
            return BYTE;
        }
        if (value instanceof Short) {
            return SHORT;
        }
        if (value instanceof ZonedDateTime) {
            return DATETIME;
        }
        if (value instanceof String || value instanceof Character || value instanceof BytesRef) {
            return KEYWORD;
        }

        return null;
    }

    public static boolean isUnsupported(DataType from) {
        return from == UNSUPPORTED;
    }

    public static boolean isString(DataType t) {
        return t == KEYWORD || t == TEXT;
    }

    public static boolean isPrimitiveAndSupported(DataType t) {
        return isPrimitive(t) && t != UNSUPPORTED;
    }

    public static boolean isPrimitive(DataType t) {
        return t != OBJECT;
    }

    public static boolean isNull(DataType t) {
        return t == NULL;
    }

    public static boolean isNullOrNumeric(DataType t) {
        return t.isNumeric() || isNull(t);
    }

    public static boolean isDateTime(DataType type) {
        return type == DATETIME;
    }

    public static boolean isNullOrTimeDuration(DataType t) {
        return t == TIME_DURATION || isNull(t);
    }

    public static boolean isNullOrDatePeriod(DataType t) {
        return t == DATE_PERIOD || isNull(t);
    }

    public static boolean isTemporalAmount(DataType t) {
        return t == DATE_PERIOD || t == TIME_DURATION;
    }

    public static boolean isNullOrTemporalAmount(DataType t) {
        return isTemporalAmount(t) || isNull(t);
    }

    public static boolean isDateTimeOrTemporal(DataType t) {
        return isDateTime(t) || isTemporalAmount(t);
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL) || (isString(left) && isString(right)) || (left.isNumeric() && right.isNumeric());
        }
    }

    /**
     * Supported types that can be contained in a block.
     */
    public static boolean isRepresentable(DataType t) {
        return t != OBJECT
            && t != UNSUPPORTED
            && t != DATE_PERIOD
            && t != TIME_DURATION
            && t != BYTE
            && t != SHORT
            && t != FLOAT
            && t != SCALED_FLOAT
            && t != SOURCE
            && t != HALF_FLOAT
            && t != PARTIAL_AGG
            && t.isCounter() == false;
    }

    public static boolean isCounter(DataType t) {
        return t == COUNTER_DOUBLE || t == COUNTER_INTEGER || t == COUNTER_LONG;
    }

    public static boolean isSpatialPoint(DataType t) {
        return t == GEO_POINT || t == CARTESIAN_POINT;
    }

    public static boolean isSpatialGeo(DataType t) {
        return t == GEO_POINT || t == GEO_SHAPE;
    }

    public static boolean isSpatial(DataType t) {
        return t == GEO_POINT || t == CARTESIAN_POINT || t == GEO_SHAPE || t == CARTESIAN_SHAPE;
    }

    public static boolean isSortable(DataType t) {
        return false == (t == SOURCE || isCounter(t) || isSpatial(t));
    }

    public String nameUpper() {
        return name;
    }

    public String typeName() {
        return typeName;
    }

    public String esType() {
        return esType;
    }

    /**
     * Return the Elasticsearch field name of this type if there is one,
     * otherwise return the ESQL specific name.
     */
    public String esNameIfPossible() {
        return esType != null ? esType : typeName;
    }

    /**
     * The name we give to types on the response.
     */
    public String outputType() {
        return esType == null ? "unsupported" : esType;
    }

    /**
     * True if the type represents a "whole number", as in, does <strong>not</strong> have a decimal part.
     */
    public boolean isWholeNumber() {
        return isWholeNumber;
    }

    /**
     * True if the type represents a "rational number", as in, <strong>does</strong> have a decimal part.
     */
    public boolean isRationalNumber() {
        return isRationalNumber;
    }

    /**
     * Does this data type represent <strong>any</strong> number?
     */
    public boolean isNumeric() {
        return isWholeNumber || isRationalNumber;
    }

    /**
     * @return the estimated size, in bytes, of this data type.  If there's no reasonable way to estimate the size,
     *         the optional will be empty.
     */
    public Optional<Integer> estimatedSize() {
        return estimatedSize;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    /**
     * {@code true} if this is a TSDB counter, {@code false} otherwise.
     */
    public boolean isCounter() {
        return isCounter;
    }

    /**
     * If this is a "small" numeric type this contains the type ESQL will
     * widen it into, otherwise this returns {@code this}.
     */
    public DataType widenSmallNumeric() {
        return widenSmallNumeric == null ? this : widenSmallNumeric;
    }

    /**
     * If this is a representable numeric this will be the counter "version"
     * of this numeric, otherwise this is {@code null}.
     */
    public DataType counter() {
        return counter;
    }

    public void writeTo(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(typeName);
    }

    public static DataType readFrom(StreamInput in) throws IOException {
        // TODO: Use our normal enum serialization pattern
        return readFrom(((PlanStreamInput) in).readCachedString());
    }

    /**
     * Resolve a {@link DataType} from a name read from a {@link StreamInput}.
     * @throws IOException on an unknown dataType
     */
    public static DataType readFrom(String name) throws IOException {
        if (name.equalsIgnoreCase(DataType.DOC_DATA_TYPE.nameUpper())) {
            /*
             * DOC is not declared in fromTypeName because fromTypeName is
             * exposed to users for things like `::<typename>` and we don't
             * want folks to be able to convert to `DOC`.
             */
            return DataType.DOC_DATA_TYPE;
        }
        DataType dataType = DataType.fromTypeName(name);
        if (dataType == null) {
            throw new IOException("Unknown DataType for type name: " + name);
        }
        return dataType;
    }

    public static Set<String> namesAndAliases() {
        return NAME_OR_ALIAS_TO_TYPE.keySet();
    }

    public static DataType fromNameOrAlias(String typeName) {
        DataType type = NAME_OR_ALIAS_TO_TYPE.get(typeName.toLowerCase(Locale.ROOT));
        return type != null ? type : UNSUPPORTED;
    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * Named parameters with default values. It's just easier to do this with
     * a builder in java....
     */
    private static class Builder {
        private String esType;

        private String typeName;

        private Optional<Integer> estimatedSize;

        /**
         * True if the type represents a "whole number", as in, does <strong>not</strong> have a decimal part.
         */
        private boolean isWholeNumber;

        /**
         * True if the type represents a "rational number", as in, <strong>does</strong> have a decimal part.
         */
        private boolean isRationalNumber;

        /**
         * True if the type supports doc values by default
         */
        private boolean docValues;

        /**
         * {@code true} if this is a TSDB counter, {@code false} otherwise.
         */
        private boolean isCounter;

        /**
         * If this is a "small" numeric type this contains the type ESQL will
         * widen it into, otherwise this is {@code null}. "Small" numeric types
         * aren't supported by ESQL proper and are "widened" on load.
         */
        private DataType widenSmallNumeric;

        /**
         * If this is a representable numeric this will be the counter "version"
         * of this numeric, otherwise this is {@code null}.
         */
        private DataType counter;

        Builder() {}

        Builder esType(String esType) {
            this.esType = esType;
            return this;
        }

        Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        Builder estimatedSize(int size) {
            this.estimatedSize = Optional.of(size);
            return this;
        }

        Builder unknownSize() {
            this.estimatedSize = Optional.empty();
            return this;
        }

        Builder wholeNumber() {
            this.isWholeNumber = true;
            return this;
        }

        Builder rationalNumber() {
            this.isRationalNumber = true;
            return this;
        }

        Builder docValues() {
            this.docValues = true;
            return this;
        }

        Builder counter() {
            this.isCounter = true;
            return this;
        }

        /**
         * If this is a "small" numeric type this contains the type ESQL will
         * widen it into, otherwise this is {@code null}. "Small" numeric types
         * aren't supported by ESQL proper and are "widened" on load.
         */
        Builder widenSmallNumeric(DataType widenSmallNumeric) {
            this.widenSmallNumeric = widenSmallNumeric;
            return this;
        }

        Builder counter(DataType counter) {
            assert counter.isCounter;
            this.counter = counter;
            return this;
        }
    }
}
