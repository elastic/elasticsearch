/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.index.mapper.RangeFieldMapper.ESQL_LONG_RANGES;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.areTypesCompatible;

/**
 * This enum represents data types the ES|QL query processing layer is able to
 * interact with in some way. This includes fully representable types (e.g.
 * {@link DataType#LONG}, numeric types which we promote (e.g. {@link DataType#SHORT})
 * or fold into other types (e.g. {@link DataType#DATE_PERIOD}) early in the
 * processing pipeline, and types which the language doesn't support, but require
 * special handling anyway (e.g. {@link DataType#OBJECT})
 *
 * <h2>Behavior of new, previously unsupported data types</h2>
 *
 * Data types that have support in ES indices, but are not yet supported in ES|QL, are
 * treated as {@link #UNSUPPORTED} by ES|QL. Fields of that type are filled with
 * {@code null} values, and no functions support them.
 * In query plans, these fields amount to
 * {@link org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute}s.
 * <p>
 * When such a type gets support in ES|QL, query plans cannot contain it
 * unless all nodes in the cluster (and remote clusters participating in the query)
 * support it to avoid serialization errors and semantically invalid results.
 * This is an example of version-aware query planning,
 * see {@link org.elasticsearch.xpack.esql.session.Versioned}.
 *
 * <h2>Process for adding a new data type</h2>
 *
 * We assume that the data type is already supported in ES indices, but not in
 * ES|QL. Types that aren't yet enabled in ES will require some adjustments to
 * the process, but should generally be a bit simpler as there are no existing
 * queries using this type that could cause backwards compatibility issues.
 * <p>
 * Note: it is not expected that all the following steps be done in a single PR.
 * Use capabilities to gate tests as you go, and use as many PRs as you think
 * appropriate. New data types are complex, and smaller PRs will make reviews
 * easier.
 * <ul>
 *     <li>
 *         Create a new data type and mark it as under construction using
 *         {@link Builder#underConstruction(TransportVersion)}. This makes the type available on
 *         SNAPSHOT builds, only, prevents some tests from running and prevents documentation
 *         for the new type to be built.</li>
 *     <li>
 *         New tests using the type will require a new {@code EsqlCapabilities} entry,
 *         otherwise bwc tests will fail (even in SNAPSHOT builds) because old nodes don't
 *         know about the new type. This capability needs to be SNAPSHOT-only as long as
 *         the type is under construction</li>
 *     <li>
 *         Create a new CSV test file for the new type. You'll either need to
 *         create a new data file as well, or add values of the new type to
 *         and existing data file. See CsvTestDataLoader for creating a new data
 *         set.</li>
 *     <li>
 *         In the new CSV test file, start adding basic functionality tests.
 *         These should include reading and returning values, both from indexed data
 *         and from the ROW command.  It should also include functions that support
 *         "every" type, such as Case or MvFirst.</li>
 *     <li>
 *         Add the new type to the CsvTestUtils#Type enum, if it isn't already
 *         there. You also need to modify CsvAssert to support reading values
 *         of the new type.</li>
 *     <li>
 *         At this point, the CSV tests should fail with a sensible ES|QL error
 *         message. Make sure they're failing in ES|QL, not in the test
 *         framework.</li>
 *     <li>
 *         Add the new data type to this enum. This will cause a bunch of
 *         compile errors for switch statements throughout the code.  Resolve those
 *         as appropriate. That is the main way in which the new type will be tied
 *         into the framework.</li>
 *     <li>
 *         Add typed data generators to TestCaseSupplier, and make sure all
 *         functions that support the new type have tests for it.</li>
 *     <li>
 *         Work to support things all types should do. Equality and the
 *         "typeless" MV functions (MvFirst, MvLast, and MvCount) should work for
 *         most types. Case and Coalesce should also support all types.
 *         If the type has a natural ordering, make sure to test
 *         sorting and the other binary comparisons. Make sure these functions all
 *         have CSV tests that run against indexed data.</li>
 *     <li>
 *         Add conversion functions as appropriate.  Almost all types should
 *         support ToString, and should have a "ToType" function that accepts a
 *         string.  There may be other logical conversions depending on the nature
 *         of the type. Make sure to add the conversion function to the
 *         TYPE_TO_CONVERSION_FUNCTION map in EsqlDataTypeConverter. Make sure the
 *         conversion functions have CSV tests that run against indexed data.</li>
 *     <li>
 *         Support the new type in aggregations that are type independent.
 *         This includes Values, Count, and Count Distinct. Make sure there are
 *         CSV tests against indexed data for these.</li>
 *     <li>
 *         Support other functions and aggregations as appropriate, making sure
 *         to included CSV tests.</li>
 *     <li>
 *         Consider how the type will interact with other types. For example,
 *         if the new type is numeric, it may be good for it to be comparable with
 *         other numbers. Supporting this may require new logic in
 *         EsqlDataTypeConverter#commonType, individual function type checking, the
 *         verifier rules, or other places. We suggest starting with CSV tests and
 *         seeing where they fail.</li>
 *     <li>
 *         Ensure the new type doesn't break {@code FROM idx | KEEP *} queries by
 *         updating AllSupportedFieldsTestCase. Make sure to run this test in bwc
 *         configurations in release mode.
 *         </li>
 * </ul>
 * There are some additional steps that should be taken when getting ready for a release:
 * <ul>
 *     <li>
 *         Ensure the capabilities for this type are always enabled.</li>
 *     <li>
 *         Mark the type with a new transport version via
 *         {@link Builder#supportedSince(TransportVersion, TransportVersion)}.
 *         This will enable the type on non-SNAPSHOT builds as long as all nodes in the cluster
 *         (and remote clusters) support it.
 *         Use the under-construction transport version for the {@code createdVersion} here so that
 *         existing tests continue to pass.
 *         </li>
 *     <li>
 *         Fix new test failures related to declared function types.</li>
 *     <li>
 *         Update the expectations in AllSupportedFieldsTestCase and make sure it
 *         passes in release builds.</li>
 *     <li>
 *         Make sure to run the full test suite locally via gradle to generate
 *         the function type tables and helper files with the new type. Ensure all
 *         the functions that support the type have appropriate docs for it.</li>
 *     <li>
 *         If appropriate, remove the type from the ESQL limitations list of
 *         unsupported types.</li>
 * </ul>
 */
public enum DataType implements Writeable {
    /**
     * Fields of this type are unsupported by any functions and are always
     * rendered as {@code null} in the response.
     */
    UNSUPPORTED(builder().typeName("UNSUPPORTED").estimatedSize(1024).supportedOnAllNodes()),
    /**
     * Fields that are always {@code null}, usually created with constant
     * {@code null} values.
     */
    NULL(builder().esType("null").estimatedSize(0).supportedOnAllNodes()),
    /**
     * Fields that can either be {@code true} or {@code false}.
     */
    BOOLEAN(builder().esType("boolean").estimatedSize(1).supportedOnAllNodes()),

    /**
     * 64-bit signed numbers labeled as metric counters in time-series indices.
     * Although stored internally as numeric fields, they represent cumulative
     * metrics and must not be treated as regular numeric fields. Therefore,
     * we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate
     * aggregation, and casting to their parent numeric type.
     */
    COUNTER_LONG(builder().esType("counter_long").estimatedSize(Long.BYTES).docValues().counter().supportedOnAllNodes()),
    /**
     * 32-bit signed numbers labeled as metric counters in time-series indices.
     * Although stored internally as numeric fields, they represent cumulative
     * metrics and must not be treated as regular numeric fields. Therefore,
     * we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate
     * aggregation, and casting to their parent numeric type.
     */
    COUNTER_INTEGER(builder().esType("counter_integer").estimatedSize(Integer.BYTES).docValues().counter().supportedOnAllNodes()),
    /**
     * 64-bit floating point numbers labeled as metric counters in time-series indices.
     * Although stored internally as numeric fields, they represent cumulative
     * metrics and must not be treated as regular numeric fields. Therefore,
     * we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate
     * aggregation, and casting to their parent numeric type.
     */
    COUNTER_DOUBLE(builder().esType("counter_double").estimatedSize(Double.BYTES).docValues().counter().supportedOnAllNodes()),

    /**
     * 64-bit signed numbers loaded as a java {@code long}.
     */
    LONG(builder().esType("long").estimatedSize(Long.BYTES).wholeNumber().docValues().counter(COUNTER_LONG).supportedOnAllNodes()),
    /**
     * 32-bit signed numbers loaded as a java {@code int}.
     */
    INTEGER(
        builder().esType("integer").estimatedSize(Integer.BYTES).wholeNumber().docValues().counter(COUNTER_INTEGER).supportedOnAllNodes()
    ),
    /**
     * 64-bit unsigned numbers packed into a java {@code long}.
     */
    UNSIGNED_LONG(builder().esType("unsigned_long").estimatedSize(Long.BYTES).wholeNumber().docValues().supportedOnAllNodes()),
    /**
     * 64-bit floating point number loaded as a java {@code double}.
     */
    DOUBLE(
        builder().esType("double").estimatedSize(Double.BYTES).rationalNumber().docValues().counter(COUNTER_DOUBLE).supportedOnAllNodes()
    ),

    /**
     * 16-bit signed numbers widened on load to {@link #INTEGER}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    SHORT(builder().esType("short").estimatedSize(Short.BYTES).wholeNumber().docValues().widenSmallNumeric(INTEGER).supportedOnAllNodes()),
    /**
     * 8-bit signed numbers widened on load to {@link #INTEGER}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    BYTE(builder().esType("byte").estimatedSize(Byte.BYTES).wholeNumber().docValues().widenSmallNumeric(INTEGER).supportedOnAllNodes()),
    /**
     * 32-bit floating point numbers widened on load to {@link #DOUBLE}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    FLOAT(
        builder().esType("float").estimatedSize(Float.BYTES).rationalNumber().docValues().widenSmallNumeric(DOUBLE).supportedOnAllNodes()
    ),
    /**
     * 16-bit floating point numbers widened on load to {@link #DOUBLE}.
     * Values of this type never escape type resolution and functions,
     * operators, and results should never encounter one.
     */
    HALF_FLOAT(
        builder().esType("half_float")
            .estimatedSize(Float.BYTES)
            .rationalNumber()
            .docValues()
            .widenSmallNumeric(DOUBLE)
            .supportedOnAllNodes()
    ),
    /**
     * Signed 64-bit fixed point numbers converted on load to a {@link #DOUBLE}.
     * Values of this type never escape type resolution and functions, operators,
     * and results should never encounter one.
     */
    SCALED_FLOAT(
        builder().esType("scaled_float")
            .estimatedSize(Long.BYTES)
            .rationalNumber()
            .docValues()
            .widenSmallNumeric(DOUBLE)
            .supportedOnAllNodes()
    ),

    /**
     * String fields that are analyzed when the document is received but never
     * cut into more than one token. ESQL always loads these after-analysis.
     * Generally ESQL uses {@code keyword} fields as raw strings. So things like
     * {@code TO_STRING} will make a {@code keyword} field.
     */
    KEYWORD(builder().esType("keyword").estimatedSize(50).docValues().supportedOnAllNodes()),
    /**
     * String fields that are analyzed when the document is received and may be
     * cut into more than one token. Generally ESQL only sees {@code text} fields
     * when loaded from the index and ESQL will load these fields
     * <strong>without</strong> analysis. The {@code MATCH} operator can be used
     * to query these fields with analysis.
     */
    TEXT(builder().esType("text").estimatedSize(1024).supportedOnAllNodes()),
    /**
     * Millisecond precision date, stored as a 64-bit signed number.
     */
    DATETIME(builder().esType("date").typeName("DATETIME").estimatedSize(Long.BYTES).docValues().supportedOnAllNodes()),
    /**
     * Nanosecond precision date, stored as a 64-bit signed number.
     */
    DATE_NANOS(builder().esType("date_nanos").estimatedSize(Long.BYTES).docValues().supportedOnAllNodes()),
    /**
     * Represents a half-inclusive range between two dates.
     */
    DATE_RANGE(builder().esType("date_range").estimatedSize(2 * Long.BYTES).docValues().underConstruction(ESQL_LONG_RANGES)),
    /**
     * IP addresses. IPv4 address are always
     * <a href="https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5">embedded</a>
     * in IPv6. These flow through the compute engine as fixed length, 16 byte
     * {@link BytesRef}s.
     */
    IP(builder().esType("ip").estimatedSize(16).docValues().supportedOnAllNodes()),
    /**
     * A version encoded in a way that sorts using semver.
     */
    // 8.15.2-SNAPSHOT is 15 bytes, most are shorter, some can be longer
    VERSION(builder().esType("version").estimatedSize(15).docValues().supportedOnAllNodes()),
    OBJECT(builder().esType("object").estimatedSize(1024).supportedOnAllNodes()),
    SOURCE(builder().esType(SourceFieldMapper.NAME).estimatedSize(10 * 1024).supportedOnAllNodes()),
    DATE_PERIOD(builder().typeName("DATE_PERIOD").estimatedSize(3 * Integer.BYTES).supportedOnAllNodes()),
    TIME_DURATION(builder().typeName("TIME_DURATION").estimatedSize(Integer.BYTES + Long.BYTES).supportedOnAllNodes()),
    // WKB for points is typically 21 bytes.
    GEO_POINT(builder().esType("geo_point").estimatedSize(21).docValues().supportedOnAllNodes()),
    CARTESIAN_POINT(builder().esType("cartesian_point").estimatedSize(21).docValues().supportedOnAllNodes()),
    // wild estimate for size, based on some test data (airport_city_boundaries)
    CARTESIAN_SHAPE(builder().esType("cartesian_shape").estimatedSize(200).docValues().supportedOnAllNodes()),
    GEO_SHAPE(builder().esType("geo_shape").estimatedSize(200).docValues().supportedOnAllNodes()),
    // We use INDEX_SOURCE because it's already on Serverless at the time of writing, and it's not in stateful versions before 9.2.0.
    // NOTE: If INDEX_SOURCE somehow gets backported to a version that doesn't actually support these types, we'll be missing validation for
    // mixed/multi clusters with remotes that don't support these types. This is low-ish risk because these types require specific
    // geo functions to turn up in the query, and those types aren't available before 9.2.0 either.
    GEOHASH(
        builder().esType("geohash")
            .typeName("GEOHASH")
            .estimatedSize(Long.BYTES)
            .supportedSince(DataTypesTransportVersions.INDEX_SOURCE, DataTypesTransportVersions.INDEX_SOURCE)
    ),
    GEOTILE(
        builder().esType("geotile")
            .typeName("GEOTILE")
            .estimatedSize(Long.BYTES)
            .supportedSince(DataTypesTransportVersions.INDEX_SOURCE, DataTypesTransportVersions.INDEX_SOURCE)
    ),
    GEOHEX(
        builder().esType("geohex")
            .typeName("GEOHEX")
            .estimatedSize(Long.BYTES)
            .supportedSince(DataTypesTransportVersions.INDEX_SOURCE, DataTypesTransportVersions.INDEX_SOURCE)
    ),

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
    DOC_DATA_TYPE(builder().esType("_doc").estimatedSize(Integer.BYTES * 3).supportedOnAllNodes()),
    /**
     * Fields with this type represent values from the {@link TimeSeriesIdFieldMapper}.
     * Every document in {@link IndexMode#TIME_SERIES} index will have a single value
     * for this field and the segments themselves are sorted on this value.
     */
    // We use INDEX_SOURCE because it's already on Serverless at the time of writing, and it's not in stateful versions before 9.2.0.
    // NOTE: If INDEX_SOURCE somehow gets backported to a version that doesn't actually support _tsid, we'll be missing validation for
    // mixed/multi clusters with remotes that don't support these types. This is low-ish risk because _tsid requires specifically being
    // used in `FROM idx METADATA _tsid` or in the `TS` command, which both weren't available before 9.2.0.
    TSID_DATA_TYPE(
        builder().esType("_tsid")
            .estimatedSize(Long.BYTES * 2)
            .docValues()
            .supportedSince(DataTypesTransportVersions.INDEX_SOURCE, DataTypesTransportVersions.INDEX_SOURCE)
    ),
    AGGREGATE_METRIC_DOUBLE(
        builder().esType("aggregate_metric_double")
            .estimatedSize(Double.BYTES * 3 + Integer.BYTES)
            .supportedSince(
                DataTypesTransportVersions.COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED,
                DataTypesTransportVersions.ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION
            )
    ),

    EXPONENTIAL_HISTOGRAM(
        builder().esType("exponential_histogram")
            .estimatedSize(16 * 160)// guess 160 buckets (OTEL default for positive values only histograms) with 16 bytes per bucket
            .docValues()
            .supportedSince(
                DataTypesTransportVersions.TEXT_SIMILARITY_RANK_DOC_EXPLAIN_CHUNKS_VERSION,
                DataTypesTransportVersions.ESQL_EXPONENTIAL_HISTOGRAM_SUPPORTED_VERSION
            )
    ),

    TDIGEST(
        builder().esType("tdigest")
            .estimatedSize(16 * 160)// guess 160 buckets (OTEL default for positive values only histograms) with 16 bytes per bucket
            .docValues()
            .supportedSince(DataTypesTransportVersions.ESQL_SERIALIZEABLE_TDIGEST, DataTypesTransportVersions.ESQL_TDIGEST_TECH_PREVIEW)

    ),

    /**
     * Data type for representing histogram data without an associated data structure
     */
    HISTOGRAM(
        builder().esType("histogram")
            .estimatedSize(16 * 160)// guess 160 buckets (OTEL default for positive values only histograms) with 16 bytes per bucket
            .docValues()
            .supportedSince(DataTypesTransportVersions.ESQL_HISTOGRAM_DATATYPE, DataTypesTransportVersions.ESQL_HISTOGRAM_DATATYPE_RELEASE)
    ),

    /**
     * Fields with this type are dense vectors, represented as an array of float values.
     */
    DENSE_VECTOR(
        builder().esType("dense_vector")
            .estimatedSize(4096)
            .supportedSince(
                DataTypesTransportVersions.ML_INFERENCE_SAGEMAKER_CHAT_COMPLETION,
                DataTypesTransportVersions.ESQL_DENSE_VECTOR_CREATED_VERSION
            )
    );

    public static final Set<DataType> UNDER_CONSTRUCTION = Arrays.stream(DataType.values())
        .filter(t -> t.supportedVersion().underConstruction())
        .collect(Collectors.toSet());

    private final String typeName;

    private final String name;

    private final String esType;

    private final int estimatedSize;

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

    /**
     * Version that first created this data type.
     */
    private final SupportedVersion supportedVersion;

    DataType(Builder builder) {
        String typeString = builder.typeName != null ? builder.typeName : builder.esType;
        this.typeName = typeString.toLowerCase(Locale.ROOT);
        this.name = typeString.toUpperCase(Locale.ROOT);
        this.esType = builder.esType;
        this.estimatedSize = Objects.requireNonNull(builder.estimatedSize, "estimated size is required");
        this.isWholeNumber = builder.isWholeNumber;
        this.isRationalNumber = builder.isRationalNumber;
        this.docValues = builder.docValues;
        this.isCounter = builder.isCounter;
        this.widenSmallNumeric = builder.widenSmallNumeric;
        this.counter = builder.counter;
        assert (builder.supportedVersion != null) : "version from when a data type is supported is required";
        this.supportedVersion = builder.supportedVersion;
    }

    private static final Collection<DataType> TYPES = Arrays.stream(values())
        .filter(d -> d != DOC_DATA_TYPE)
        .sorted(Comparator.comparing(DataType::typeName))
        .toList();

    private static final Collection<DataType> STRING_TYPES = DataType.types().stream().filter(DataType::isString).toList();

    private static final Map<String, DataType> NAME_TO_TYPE;

    private static final Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        // TODO: Why don't we use the names ES uses as the esType field for these?
        // ES calls this 'point', but ESQL calls it 'cartesian_point'
        map.put("point", DataType.CARTESIAN_POINT);
        map.put("shape", DataType.CARTESIAN_SHAPE);
        // semantic_text is returned as text by field_caps, but unit tests will retrieve it from the mapping
        // so we need to map it here as well
        map.put("semantic_text", DataType.TEXT);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
        // DATETIME has different esType and typeName, add an entry in NAME_TO_TYPE with date as key
        map = TYPES.stream().collect(toMap(DataType::typeName, t -> t));
        map.put("date", DataType.DATETIME);
        NAME_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private static final Map<String, DataType> NAME_OR_ALIAS_TO_TYPE;
    static {
        Map<String, DataType> map = DataType.types().stream().collect(toMap(DataType::typeName, Function.identity()));
        map.put("bool", BOOLEAN);
        map.put("int", INTEGER);
        map.put("string", KEYWORD);
        map.put("date", DataType.DATETIME);
        NAME_OR_ALIAS_TO_TYPE = Collections.unmodifiableMap(map);
    }

    public static Collection<DataType> types() {
        return TYPES;
    }

    public static Collection<DataType> stringTypes() {
        return STRING_TYPES;
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
        // Ensure that under construction types are not used in non-snapshot builds.
        if (type == null || type.supportedVersion().supportedLocally() == false) {
            return UNSUPPORTED;
        }
        return type;
    }

    public static DataType fromJava(Object value) {
        switch (value) {
            case null -> {
                return NULL;
            }
            case Integer i -> {
                return INTEGER;
            }
            case Long l -> {
                return LONG;
            }
            case BigInteger bigInteger -> {
                return UNSIGNED_LONG;
            }
            case Boolean b -> {
                return BOOLEAN;
            }
            case Double v -> {
                return DOUBLE;
            }
            case Float v -> {
                return FLOAT;
            }
            case Byte b -> {
                return BYTE;
            }
            case Short i -> {
                return SHORT;
            }
            case ZonedDateTime zonedDateTime -> {
                return DATETIME;
            }
            case List<?> list -> {
                if (list.isEmpty()) {
                    return null;
                }
                return fromJava(list.getFirst());
            }
            default -> {
                if (value instanceof String || value instanceof Character || value instanceof BytesRef) {
                    return KEYWORD;
                }
                return null;
            }
        }

    }

    /**
     * Infers the ES|QL DataType from a Java Class.
     * This method mirrors the logic of {@link #fromJava(Object)} but operates on {@code Class<?>} types,
     * handling both primitive and wrapper classes equivalently.
     *
     * @param classType The Java Class to infer the DataType from.
     * @return The corresponding ES|QL DataType, or {@code null} if no direct mapping is found or can be reliably inferred.
     */
    public static DataType fromJavaType(Class<?> classType) {
        if (classType == null || classType == Void.class) {
            return NULL;
        }

        if (classType == int.class || classType == Integer.class) {
            return INTEGER;
        } else if (classType == long.class || classType == Long.class) {
            return LONG;
        } else if (classType == BigInteger.class) {
            return UNSIGNED_LONG;
        } else if (classType == boolean.class || classType == Boolean.class) {
            return BOOLEAN;
        } else if (classType == double.class || classType == Double.class) {
            return DOUBLE;
        } else if (classType == float.class || classType == Float.class) {
            return FLOAT;
        } else if (classType == byte.class || classType == Byte.class) {
            return BYTE;
        } else if (classType == short.class || classType == Short.class) {
            return SHORT;
        } else if (classType == ZonedDateTime.class) {
            return DATETIME;
        } else if (classType == String.class || classType == char.class || classType == Character.class || classType == BytesRef.class) {
            // Note: BytesRef is an object, not a primitive or wrapper, so it's directly compared.
            // char.class and Character.class map to KEYWORD
            return KEYWORD;
        } else if (List.class.isAssignableFrom(classType)) {
            // Consistent with fromJava(Object) returning null for empty lists or lists with unknown element types.
            return null;
        }
        // Fallback for any other Class<?> type not explicitly handled
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

    public static boolean isTimeDuration(DataType t) {
        return t == TIME_DURATION;
    }

    public static boolean isDateNanos(DataType t) {
        return t == DATE_NANOS;
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

    public static boolean isDateTimeOrNanosOrTemporal(DataType t) {
        return isDateTime(t) || isTemporalAmount(t) || t == DATE_NANOS;
    }

    public static boolean isMillisOrNanos(DataType t) {
        return t == DATETIME || t == DATE_NANOS;
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return areTypesCompatible(left, right);
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
            && t.isCounter() == false;
    }

    public static boolean isCounter(DataType t) {
        return t == COUNTER_DOUBLE || t == COUNTER_INTEGER || t == COUNTER_LONG;
    }

    public static boolean isSpatialPoint(DataType t) {
        return isGeoPoint(t) || isCartesianPoint(t);
    }

    public static boolean isGeoPoint(DataType t) {
        return t == GEO_POINT;
    }

    public static boolean isCartesianPoint(DataType t) {
        return t == CARTESIAN_POINT;
    }

    public static boolean isSpatialShape(DataType t) {
        return t == GEO_SHAPE || t == CARTESIAN_SHAPE || t == GEOHASH || t == GEOTILE || t == GEOHEX;
    }

    public static boolean isSpatialGeo(DataType t) {
        return t == GEO_POINT || t == GEO_SHAPE || t == GEOHASH || t == GEOTILE || t == GEOHEX;
    }

    public static boolean isSpatial(DataType t) {
        return t == GEO_POINT || t == CARTESIAN_POINT || t == GEO_SHAPE || t == CARTESIAN_SHAPE;
    }

    public static boolean isSpatialOrGrid(DataType t) {
        return isSpatial(t) || isGeoGrid(t);
    }

    public static boolean isGeoGrid(DataType t) {
        return t == GEOHASH || t == GEOTILE || t == GEOHEX;
    }

    public static boolean isSortable(DataType t) {
        return false == (t == SOURCE || isCounter(t) || isSpatialOrGrid(t) || t == AGGREGATE_METRIC_DOUBLE);
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
     * {@code true} if the type represents any kind of histogram, {@code false} otherwise.
     */
    public boolean isHistogram() {
        return this == HISTOGRAM || this == EXPONENTIAL_HISTOGRAM || this == TDIGEST;
    }

    /**
     * An estimate of the size of values of this type in a Block. All types must have an
     * estimate, and generally follow the following rules:
     * <ol>
     *     <li>
     *         If you know the precise size of a single element of this type, use that.
     *         For example {@link #INTEGER} uses {@link Integer#BYTES}.
     *     </li>
     *     <li>
     *         Overestimates are better than under-estimates. Over-estimates make less
     *         efficient operations, but under-estimates make circuit breaker errors.
     *     </li>
     * </ol>
     * @return the estimated size of this data type in bytes
     */
    public int estimatedSize() {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportedVersion.supportedOn(out.getTransportVersion(), Build.current().isSnapshot()) == false) {
            /*
             * Throw a 500 error - this is a bug, we failed to account for an old node during planning.
             */
            throw new QlIllegalArgumentException(
                "remote node at version [" + out.getTransportVersion() + "] doesn't understand data type [" + this + "]"
            );
        }
        ((PlanStreamOutput) out).writeCachedString(typeName);
    }

    public static DataType readFrom(StreamInput in) throws IOException {
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

    public DataType noText() {
        return isString(this) ? KEYWORD : this;
    }

    public DataType noCounter() {
        return switch (this) {
            case COUNTER_DOUBLE -> DOUBLE;
            case COUNTER_INTEGER -> INTEGER;
            case COUNTER_LONG -> LONG;
            default -> this;
        };
    }

    public boolean isDate() {
        return switch (this) {
            case DATETIME, DATE_NANOS -> true;
            default -> false;
        };
    }

    public SupportedVersion supportedVersion() {
        return supportedVersion;
    }

    public static DataType suggestedCast(Set<DataType> originalTypes) {
        if (originalTypes.isEmpty() || originalTypes.contains(UNSUPPORTED)) {
            return null;
        }
        if (originalTypes.contains(DATE_NANOS) && originalTypes.contains(DATETIME) && originalTypes.size() == 2) {
            return DATE_NANOS;
        }
        if (originalTypes.contains(AGGREGATE_METRIC_DOUBLE)) {
            boolean allNumeric = true;
            for (DataType type : originalTypes) {
                if (type.isNumeric() == false && type != AGGREGATE_METRIC_DOUBLE) {
                    allNumeric = false;
                    break;
                }
            }
            if (allNumeric) {
                return AGGREGATE_METRIC_DOUBLE;
            }
        }

        return KEYWORD;
    }

    /**
     * Named parameters with default values. It's just easier to do this with
     * a builder in java....
     */
    private static class Builder {
        private String esType;

        private String typeName;

        private Integer estimatedSize;

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

        /**
         * The version from when on a {@link DataType} is supported. When a query tries to use a data type
         * not supported on the node it runs on, we throw during serialization of the type.
         */
        private SupportedVersion supportedVersion;

        Builder() {}

        Builder esType(String esType) {
            this.esType = esType;
            return this;
        }

        Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        /**
         * See {@link DataType#estimatedSize}.
         */
        Builder estimatedSize(int size) {
            this.estimatedSize = size;
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

        /**
         * Marks a type that is supported in production since {@code supportedVersion}.
         * When a query tries to use a data type not supported on the nodes it runs on, this is a bug.
         * <p>
         * On snapshot builds, the {@code createdVersion} is used instead, so that existing tests continue
         * to work after release if the type was previously {@link #underConstruction(TransportVersion)};
         * the under-construction version should be used as the {@code createdVersion}.
         */
        Builder supportedSince(TransportVersion createdVersion, TransportVersion supportedVersion) {
            this.supportedVersion = SupportedVersion.supportedSince(createdVersion, supportedVersion);
            return this;
        }

        Builder supportedOnAllNodes() {
            this.supportedVersion = SupportedVersion.SUPPORTED_ON_ALL_NODES;
            return this;
        }

        /**
         * Marks a type that is not supported in production yet, but is supported in snapshot builds
         * starting with the given version.
         */
        Builder underConstruction(TransportVersion createdVersion) {
            this.supportedVersion = SupportedVersion.underConstruction(createdVersion);
            return this;
        }
    }

    public static class DataTypesTransportVersions {

        /**
         * The first transport version after the PR that introduced geotile/geohash/geohex, resp.
         * after 9.1. We didn't require transport versions at that point in time, as geotile/hash/hex require
         * using specific functions to even occur in query plans.
         */
        public static final TransportVersion INDEX_SOURCE = TransportVersion.fromName("index_source");

        /**
         * We retroactively need a suitable transport version as aggregate_metric_double's "created version".
         * This type is supported on all snapshot builds that we run in bwc tests; at the time of writing,
         * the oldest versions should be 8.19.x and 9.0.x.
         * We can thus choose any transport version as long as it's on 9.0 and was backported to 8.18.
         */
        private static final TransportVersion COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED = TransportVersion.fromName(
            "cohere_bit_embedding_type_support_added"
        );
        public static final TransportVersion ESQL_AGGREGATE_METRIC_DOUBLE_CREATED_VERSION = TransportVersion.fromName(
            "esql_aggregate_metric_double_created_version"
        );

        /**
         * The first version after dense_vector was supported in SNAPSHOT (but not in production)
         */
        public static final TransportVersion ML_INFERENCE_SAGEMAKER_CHAT_COMPLETION = TransportVersion.fromName(
            "ml_inference_sagemaker_chat_completion"
        );
        public static final TransportVersion ESQL_DENSE_VECTOR_CREATED_VERSION = TransportVersion.fromName(
            "esql_dense_vector_created_version"
        );

        /**
         * First transport version after the PR that introduced the exponential_histogram data type which was NOT also backported to 9.2.
         * (Exp. histogram was added as SNAPSHOT-only to 9.3.)
         */
        public static final TransportVersion TEXT_SIMILARITY_RANK_DOC_EXPLAIN_CHUNKS_VERSION = TransportVersion.fromName(
            "text_similarity_rank_docs_explain_chunks"
        );
        public static final TransportVersion ESQL_EXPONENTIAL_HISTOGRAM_SUPPORTED_VERSION = TransportVersion.fromName(
            "esql_exponential_histogram_supported_version"
        );

        private static final TransportVersion ESQL_SERIALIZEABLE_TDIGEST = TransportVersion.fromName("esql_serializeable_tdigest");
        /**
         * Development version for histogram support
         */
        public static final TransportVersion ESQL_HISTOGRAM_DATATYPE = TransportVersion.fromName("esql_histogram_datatype");

        /**
         * Transport version for when the feature flag for the ESQL TDigest type was removed.
         */
        public static final TransportVersion ESQL_TDIGEST_TECH_PREVIEW = TransportVersion.fromName("esql_tdigest_tech_preview");

        /**
         * Release version for Histogram data type support
         */
        public static final TransportVersion ESQL_HISTOGRAM_DATATYPE_RELEASE = TransportVersion.fromName("esql_histogram_datatype_release");
    }
}
