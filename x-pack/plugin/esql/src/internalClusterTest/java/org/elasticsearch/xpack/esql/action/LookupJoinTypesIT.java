/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.queryparser.ext.Extensions.Pair;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOC_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.HALF_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.SCALED_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TSID_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNDER_CONSTRUCTION;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This test suite tests the lookup join functionality in ESQL with various data types.
 * For each pair of types being tested, it builds a main index called "index" containing a single document with as many fields as
 * types being tested on the left of the pair, and then creates that many other lookup indexes, each with a single document containing
 * exactly two fields: the field to join on, and a field to return.
 * The assertion is that for valid combinations, the return result should exist, and for invalid combinations an exception should be thrown.
 * If no exception is thrown, and no result is returned, our validation rules are not aligned with the internal behaviour (i.e. a bug).
 * Let's assume we want to test a lookup using a byte field in the main index and integer in the lookup index, then we'll create 2 indices,
 * named {@code main_index} and {@code lookup_byte_integer} resp.
 * The main index contains a field called {@code main_byte} and the lookup index has {@code lookup_integer}. To test the pair, we run
 * {@code FROM main_index | RENAME main_byte AS lookup_integer | LOOKUP JOIN lookup_index ON lookup_integer | KEEP other}
 * and assert that the result exists and is equal to "value".
 * For tests using union types, the same applies but there are additional main indices so that we have actual mapping conflicts.
 * E.g. the field {@code main_byte} will occur another time in the index {@code main_byte_as_short} when we're testing a byte-short union
 * type.
 */
// TODO: This suite creates a lot of indices. It should be sufficient to just create 1 main index with 1 field per relevant type and 1
// lookup index with 1 field per relevant type; only union types require additional main indices so we can have the same field mapped to
// different types.
@ClusterScope(scope = SUITE, numClientNodes = 1, numDataNodes = 1)
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class LookupJoinTypesIT extends ESIntegTestCase {
    private static final String MAIN_INDEX_PREFIX = "main_";
    private static final String MAIN_INDEX = MAIN_INDEX_PREFIX + "index";
    private static final String LOOKUP_INDEX_PREFIX = "lookup_";

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            EsqlPlugin.class,
            MapperExtrasPlugin.class,
            VersionFieldPlugin.class,
            UnsignedLongMapperPlugin.class,
            SpatialPlugin.class
        );
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> operations = new ArrayList<>();
        operations.add(new Object[] { null });
        if (EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()) {
            for (BinaryComparisonOperation operation : BinaryComparisonOperation.values()) {
                operations.add(new Object[] { operation });
            }
        }
        return operations;
    }

    private final BinaryComparisonOperation operationParameterized;

    public LookupJoinTypesIT(BinaryComparisonOperation operation) {
        this.operationParameterized = operation;
    }

    private static final Map<Pair<String, BinaryComparisonOperation>, TestConfigs> testConfigurations = new HashMap<>();
    static {
        List<BinaryComparisonOperation> operations = new ArrayList<>(List.of(BinaryComparisonOperation.values()));
        operations.add(null); // null means field-based join
        for (BinaryComparisonOperation operation : operations) {

            // Initialize the test configurations for string tests
            {
                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("strings", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                configs.addPasses(KEYWORD, KEYWORD, operation);
                configs.addPasses(TEXT, KEYWORD, operation);
                configs.addFailsUnsupported(KEYWORD, TEXT, operation);
            }

            // Test integer types
            var integerTypes = List.of(BYTE, SHORT, INTEGER, LONG);
            {
                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("integers", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType mainType : integerTypes) {
                    for (DataType lookupType : integerTypes) {
                        configs.addPasses(mainType, lookupType, operation);
                    }
                }
            }

            // Test float and double
            var floatTypes = List.of(HALF_FLOAT, FLOAT, DOUBLE, SCALED_FLOAT);
            {

                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("floats", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType mainType : floatTypes) {
                    for (DataType lookupType : floatTypes) {
                        configs.addPasses(mainType, lookupType, operation);
                    }
                }
            }

            // Tests for mixed-numerical types
            {

                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("mixed-numerical", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType mainType : integerTypes) {
                    for (DataType lookupType : floatTypes) {
                        configs.addPasses(mainType, lookupType, operation);
                        configs.addPasses(lookupType, mainType, operation);
                    }
                }
            }

            // Tests for mixed-date/time types
            var dateTypes = List.of(DATETIME, DATE_NANOS);
            {
                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("mixed-temporal", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType mainType : dateTypes) {
                    for (DataType lookupType : dateTypes) {
                        if (mainType != lookupType) {
                            configs.addFails(mainType, lookupType);
                        }
                    }
                }
            }

            // Union types; non-exhaustive and can be extended
            {
                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("union-types", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                configs.addUnionTypePasses(SHORT, INTEGER, INTEGER);
                configs.addUnionTypePasses(BYTE, DOUBLE, LONG);
                configs.addUnionTypePasses(DATETIME, DATE_NANOS, DATE_NANOS);
                configs.addUnionTypePasses(DATE_NANOS, DATETIME, DATETIME);
                configs.addUnionTypePasses(SCALED_FLOAT, HALF_FLOAT, DOUBLE);
                configs.addUnionTypePasses(TEXT, KEYWORD, KEYWORD);

            }

            // Tests for all unsupported types
            DataType[] unsupported = Join.UNSUPPORTED_TYPES;
            boolean isLessOrGreater = operation == BinaryComparisonOperation.GT
                || operation == BinaryComparisonOperation.GTE
                || operation == BinaryComparisonOperation.LT
                || operation == BinaryComparisonOperation.LTE;
            {

                Collection<TestConfigs> existing = testConfigurations.values();
                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("unsupported", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType type : unsupported) {
                    if (type == NULL
                        || type == DOC_DATA_TYPE
                        || type == TSID_DATA_TYPE
                        || type == AGGREGATE_METRIC_DOUBLE  // need special handling for loads at the moment
                        || type == DENSE_VECTOR  // need special handling for loads at the moment
                        || type == EXPONENTIAL_HISTOGRAM
                        || type == GEOHASH
                        || type == GEOTILE
                        || type == GEOHEX
                        || type.esType() == null
                        || type.isCounter()
                        || DataType.isRepresentable(type) == false) {
                        // Skip unmappable types, or types not supported in ES|QL in general
                        continue;
                    }
                    if (existingIndex(existing, type, type, operation)) {
                        // Skip existing configurations
                        continue;
                    }
                    if (operation != null && type == DENSE_VECTOR
                        || isLessOrGreater
                            && (type == GEO_POINT || type == GEO_SHAPE || type == CARTESIAN_POINT || type == CARTESIAN_SHAPE)) {
                        configs.addUnsupportedComparisonFails(type, type, operation);
                    } else {
                        configs.addFailsUnsupported(type, type, operation);
                    }
                }
            }

            // Tests for all types where left and right are the same type
            DataType[] supported = {
                BOOLEAN,
                LONG,
                INTEGER,
                DOUBLE,
                SHORT,
                BYTE,
                FLOAT,
                HALF_FLOAT,
                DATETIME,
                DATE_NANOS,
                IP,
                KEYWORD,
                SCALED_FLOAT };
            {
                Collection<TestConfigs> existing = testConfigurations.values();
                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("same", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType type : supported) {
                    assertThat("Claiming supported for unsupported type: " + type, List.of(unsupported).contains(type), is(false));
                    if (existingIndex(existing, type, type, operation) == false) {
                        // Only add the configuration if it doesn't already exist
                        if (type == BOOLEAN && isLessOrGreater) {
                            // Boolean does not support inequality operations
                            configs.addUnsupportedComparisonFails(type, type, operation);
                        } else {
                            configs.addPasses(type, type, operation);
                        }
                    }
                }
            }

            // Assert that unsupported types are not in the supported list
            for (DataType type : unsupported) {
                assertThat("Claiming supported for unsupported type: " + type, List.of(supported).contains(type), is(false));
            }

            // Assert that unsupported+supported covers all types:
            List<DataType> missing = new ArrayList<>();
            for (DataType type : DataType.values()) {
                boolean isUnsupported = List.of(unsupported).contains(type);
                boolean isSupported = List.of(supported).contains(type);
                if (isUnsupported == false && isSupported == false) {
                    missing.add(type);
                }
            }
            assertThat(missing + " are not in the supported or unsupported list", missing.size(), is(0));

            // Tests for all other type combinations
            {
                Collection<TestConfigs> existing = testConfigurations.values();

                TestConfigs configs = testConfigurations.computeIfAbsent(
                    new Pair<>("others", operation),
                    x -> new TestConfigs(x.cur(), x.cud())
                );
                for (DataType mainType : supported) {
                    for (DataType lookupType : supported) {
                        if (existingIndex(existing, mainType, lookupType, operation) == false) {
                            if (operation == null) {
                                // Only add the configuration if it doesn't already exist
                                configs.addFails(mainType, lookupType);
                            } else if (isLessOrGreater) {
                                configs.addIncompatibleDifferentTypesLessGreater(mainType, lookupType, operation);
                            } else {
                                configs.addMismatchedComparisonFailsEqualNotEqual(mainType, lookupType, operation);
                            }
                        }
                    }
                }
            }
        }
        // Make sure we have never added two configurations with the same lookup index name.
        // This prevents accidentally adding the same test config to two different groups.
        Set<String> knownTypes = new HashSet<>();
        for (TestConfigs configs : testConfigurations.values()) {
            for (TestConfig config : configs.configs.values()) {
                if (knownTypes.contains(config.lookupIndexName())) {
                    throw new IllegalArgumentException("Duplicate lookup index name: " + config.lookupIndexName());
                }
                knownTypes.add(config.lookupIndexName());
            }
        }
    }

    static String stringForOperation(BinaryComparisonOperation operation) {
        return operation == null ? "_field" : "_" + operation.name().toLowerCase(Locale.ROOT);
    }

    private static boolean existingIndex(
        Collection<TestConfigs> existing,
        DataType mainType,
        DataType lookupType,
        BinaryComparisonOperation operation
    ) {
        String indexName = LOOKUP_INDEX_PREFIX + mainType.esType() + "_" + lookupType.esType() + stringForOperation(operation);
        return existing.stream().anyMatch(c -> c.exists(indexName));
    }

    /** This test generates documentation for the supported output types of the lookup join. */
    public void testOutputSupportedTypes() throws Exception {
        Set<DocsV3Support.TypeSignature> signatures = new LinkedHashSet<>();
        for (TestConfigs configs : testConfigurations.values()) {
            if (configs.group.equals("unsupported") || configs.group.equals("union-types") || configs.operation != null) {
                continue;
            }
            for (TestConfig config : configs.configs.values()) {
                if (config instanceof TestConfigPasses) {
                    signatures.add(
                        new DocsV3Support.TypeSignature(
                            List.of(
                                new DocsV3Support.Param(config.mainType(), List.of()),
                                new DocsV3Support.Param(config.lookupType(), null)
                            ),
                            null
                        )
                    );
                }
            }
        }
        saveJoinTypes(() -> signatures);
    }

    public void testLookupJoinStrings() {
        testLookupJoinTypes("strings", operationParameterized);

    }

    public void testLookupJoinIntegers() {
        testLookupJoinTypes("integers", operationParameterized);
    }

    public void testLookupJoinFloats() {
        testLookupJoinTypes("floats", operationParameterized);
    }

    public void testLookupJoinMixedNumerical() {
        testLookupJoinTypes("mixed-numerical", operationParameterized);
    }

    public void testLookupJoinMixedTemporal() {
        testLookupJoinTypes("mixed-temporal", operationParameterized);
    }

    public void testLookupJoinSame() {
        testLookupJoinTypes("same", operationParameterized);
    }

    public void testLookupJoinUnsupported() {
        testLookupJoinTypes("unsupported", operationParameterized);
    }

    public void testLookupJoinOthers() {
        testLookupJoinTypes("others", operationParameterized);
    }

    public void testLookupJoinUnionTypes() {
        testLookupJoinTypes("union-types", operationParameterized);
    }

    private void testLookupJoinTypes(String group, BinaryComparisonOperation operation) {
        TestConfigs configs = testConfigurations.get(new Pair<>(group, operation));
        initIndexes(configs);
        initData(configs);
        for (TestConfig config : configs.values()) {
            if ((isValidDataType(config.mainType()) && isValidDataType(config.lookupType())) == false) {
                continue;
            }
            config.validateMainIndex();
            config.validateLookupIndex();
            config.validateAdditionalMainIndex();

            config.doTest();
        }
    }

    private void initIndexes(TestConfigs configs) {
        for (TestMapping mapping : configs.indices()) {
            CreateIndexRequestBuilder builder = prepareCreate(mapping.indexName).setMapping(mapping.propertiesAsJson());
            if (mapping.settings != null) {
                builder = builder.setSettings(mapping.settings);
            }

            assertAcked(builder);
        }
    }

    private void initData(TestConfigs configs) {
        List<TestDocument> docs = configs.docs();
        List<IndexRequestBuilder> indexRequests = new ArrayList<>(docs.size());

        for (TestDocument doc : docs) {
            var indexRequest = client().prepareIndex().setIndex(doc.indexName()).setId(doc.id).setSource(doc.source, XContentType.JSON);
            indexRequests.add(indexRequest);
        }
        indexRandom(true, indexRequests);
    }

    static String suffixLeftFieldName(BinaryComparisonOperation operation) {
        if (operation != null) {
            return "_left";
        }
        return "";
    }

    private static String propertyFor(String fieldName, DataType type) {
        return String.format(Locale.ROOT, "\"%s\": %s", fieldName, sampleDataTextFor(type));
    }

    private static String sampleDataTextFor(DataType type) {
        return sampleDataForValue(sampleDataFor(type));
    }

    private static String sampleDataForValue(Object value) {
        if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (value instanceof List<?> list) {
            return "[" + list.stream().map(LookupJoinTypesIT::sampleDataForValue).collect(Collectors.joining(", ")) + "]";
        }
        return String.valueOf(value);
    }

    private static final double SCALING_FACTOR = 10.0;

    private static Object sampleDataFor(DataType type) {
        return switch (type) {
            case BOOLEAN -> true;
            case DATETIME, DATE_NANOS -> "2025-04-02T12:00:00.000Z";
            case IP -> "127.0.0.1";
            case KEYWORD, TEXT -> "key";
            case BYTE, SHORT, INTEGER -> 1;
            case LONG, UNSIGNED_LONG -> 1L;
            case HALF_FLOAT, FLOAT, DOUBLE, SCALED_FLOAT -> 1.0;
            case VERSION -> "1.2.19";
            case GEO_POINT, CARTESIAN_POINT -> "POINT (1.0 2.0)";
            case GEO_SHAPE, CARTESIAN_SHAPE -> "POLYGON ((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))";
            case DENSE_VECTOR -> List.of(0.2672612f, 0.5345224f, 0.8017837f);
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    public record TestMapping(String indexName, Collection<String> properties, Settings settings) {

        private static final String PROPERTY_PREFIX = "{\n  \"properties\" : {\n";
        private static final String PROPERTY_SUFFIX = "  }\n}\n";

        /**
         * {@link TestMapping#indexName} and {@link TestMapping#settings} should be the same across the collection, otherwise they're
         * obtained from an arbitrary element.
         */
        public static TestMapping mergeProperties(Collection<TestMapping> mappings) {
            TestMapping lastMapping = null;

            Set<String> properties = new HashSet<>();
            for (TestMapping mapping : mappings) {
                properties.addAll(mapping.properties);
                lastMapping = mapping;
            }
            String indexName = lastMapping == null ? null : lastMapping.indexName;
            Settings settings = lastMapping == null ? null : lastMapping.settings;

            return new TestMapping(indexName, properties, settings);
        }

        public static String propertiesAsJson(Collection<String> properties) {
            return PROPERTY_PREFIX + String.join(", ", properties) + PROPERTY_SUFFIX;
        }

        public String propertiesAsJson() {
            return propertiesAsJson(properties);
        }
    };

    private record TestDocument(String indexName, String id, String source) {};

    private static class TestConfigs {
        final String group;
        final Map<String, TestConfig> configs;
        @Nullable
        final BinaryComparisonOperation operation; // null means field based join

        TestConfigs(String group, BinaryComparisonOperation operation) {
            this.group = group;
            this.configs = new LinkedHashMap<>();
            this.operation = operation;
        }

        public List<TestMapping> indices() {
            List<TestMapping> results = new ArrayList<>();

            // The main index will have many fields, one of each type to use in later type specific joins
            List<TestMapping> mainIndices = new ArrayList<>();
            for (TestConfig config : configs.values()) {
                mainIndices.add(config.mainIndex());
                TestMapping otherIndex = config.additionalMainIndex();
                if (otherIndex != null) {
                    results.add(otherIndex);
                }
            }
            TestMapping mainIndex = TestMapping.mergeProperties(mainIndices);

            results.add(mainIndex);

            configs.values()
                .forEach(
                    // Each lookup index will get a document with a field to join on, and a results field to get back
                    (c) -> results.add(c.lookupIndex())
                );

            return results;
        }

        public List<TestDocument> docs() {
            List<TestDocument> results = new ArrayList<>();

            int docId = 0;
            for (TestConfig config : configs.values()) {
                String doc = String.format(Locale.ROOT, """
                    {
                      %s,
                      "other": "value"
                    }
                    """, propertyFor(config.lookupFieldName(), config.lookupType()));
                results.add(new TestDocument(config.lookupIndexName(), "" + (++docId), doc));
            }

            List<String> mainProperties = configs.values()
                .stream()
                .map(c -> propertyFor(c.mainFieldName(), c.mainType()))
                .distinct()
                .collect(Collectors.toList());
            results.add(new TestDocument(MAIN_INDEX, "1", String.format(Locale.ROOT, """
                {
                  %s
                }
                """, String.join(",\n  ", mainProperties))));

            for (TestConfig config : configs.values()) {
                TestMapping additionalIndex = config.additionalMainIndex();
                if (additionalIndex != null) {
                    String doc = String.format(Locale.ROOT, """
                        {
                          %s
                        }
                        """, propertyFor(config.mainFieldName(), ((TestConfigPassesUnionType) config).otherMainType()));
                    // TODO: Casting to TestConfigPassesUnionType is an ugly hack; better to derive the test data from the TestMapping or
                    // from the TestConfig.
                    results.add(new TestDocument(additionalIndex.indexName, "1", doc));
                }
            }

            return results;
        }

        private Collection<TestConfig> values() {
            return configs.values();
        }

        private boolean exists(String indexName) {
            return configs.containsKey(indexName);
        }

        private void add(TestConfig config) {
            if (configs.containsKey(config.lookupIndexName())) {
                throw new IllegalArgumentException("Duplicate lookup index name: " + config.lookupIndexName());
            }
            configs.put(config.lookupIndexName(), config);
        }

        private void addPasses(DataType mainType, DataType lookupType, BinaryComparisonOperation operation) {
            add(new TestConfigPasses(mainType, lookupType, operation));
        }

        private void addUnionTypePasses(DataType mainType, DataType otherMainType, DataType lookupType) {
            add(new TestConfigPassesUnionType(mainType, otherMainType, lookupType, operation));
        }

        private void addFails(DataType mainType, DataType lookupType) {
            String fieldNameLeft = LOOKUP_INDEX_PREFIX + lookupType.esType() + suffixLeftFieldName(operation);
            String fieldNameRight = LOOKUP_INDEX_PREFIX + lookupType.esType();

            String errorMessage = String.format(
                Locale.ROOT,
                "JOIN left field [%s] of type [%s] is incompatible with right field [%s] of type [%s]",
                fieldNameLeft,
                mainType.widenSmallNumeric(),
                fieldNameRight,
                lookupType.widenSmallNumeric()
            );
            add(
                new TestConfigFails<>(
                    mainType,
                    lookupType,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMessage)),
                    operation
                )
            );
        }

        private void addMismatchedComparisonFailsEqualNotEqual(
            DataType mainType,
            DataType lookupType,
            BinaryComparisonOperation operation
        ) {
            String fieldNameLeft = LOOKUP_INDEX_PREFIX + lookupType.esType() + suffixLeftFieldName(operation);
            String fieldNameRight = LOOKUP_INDEX_PREFIX + lookupType.esType();
            final Consumer<VerificationException> assertion = e -> {
                String errorMessage1 = String.format(
                    Locale.ROOT,
                    "first argument of [%s %s %s] is [",
                    fieldNameLeft,
                    operation.symbol(),
                    fieldNameRight
                );
                String errorMessage3 = String.format(Locale.ROOT, " but was [%s]", lookupType.widenSmallNumeric().typeName());
                assertThat(e.getMessage(), containsString(errorMessage1));
                assertThat(e.getMessage(), containsString("] so second argument must also be ["));
                assertThat(e.getMessage(), containsString(errorMessage3));
            };

            add(new TestConfigFails<>(mainType, lookupType, VerificationException.class, assertion, operation));
        }

        private void addIncompatibleDifferentTypesLessGreater(DataType mainType, DataType lookupType, BinaryComparisonOperation operation) {
            String fieldNameLeft = LOOKUP_INDEX_PREFIX + lookupType.esType() + suffixLeftFieldName(operation);
            String fieldNameRight = LOOKUP_INDEX_PREFIX + lookupType.esType();

            String errorMessage1 = String.format(Locale.ROOT, "argument of [%s %s %s]", fieldNameLeft, operation.symbol(), fieldNameRight);

            add(new TestConfigFails<>(mainType, lookupType, VerificationException.class, e -> {
                assertThat(e.getMessage().toLowerCase(Locale.ROOT), containsString(errorMessage1));
                assertThat(
                    e.getMessage().toLowerCase(Locale.ROOT),
                    anyOf(List.of(containsString(mainType.widenSmallNumeric().typeName()), containsString("numeric")))
                );
                assertThat(e.getMessage().toLowerCase(Locale.ROOT), containsString(lookupType.typeName()));
            }, operation));
        }

        private void addUnsupportedComparisonFails(DataType mainType, DataType lookupType, BinaryComparisonOperation operation) {
            String fieldNameLeft = LOOKUP_INDEX_PREFIX + lookupType.esType() + suffixLeftFieldName(operation);
            String fieldNameRight = LOOKUP_INDEX_PREFIX + lookupType.esType();

            String errorMessage1 = String.format(
                Locale.ROOT,
                "first argument of [%s %s %s] must be",
                fieldNameLeft,
                operation.symbol(),
                fieldNameRight
            );
            String errorMessage2 = String.format(Locale.ROOT, "found value [%s] type [%s]", fieldNameLeft, mainType.typeName());

            add(new TestConfigFails<>(mainType, lookupType, VerificationException.class, e -> {
                assertThat(e.getMessage().toLowerCase(Locale.ROOT), containsString(errorMessage1));
                assertThat(e.getMessage().toLowerCase(Locale.ROOT), containsString(errorMessage2));
            }, operation));
        }

        private void addFailsUnsupported(DataType mainType, DataType lookupType, BinaryComparisonOperation operation) {
            String fieldName = "lookup_" + lookupType.esType();
            String errorMessage = String.format(
                Locale.ROOT,
                "JOIN with right field [%s] of type [%s] is not supported",
                fieldName,
                lookupType
            );
            add(
                new TestConfigFails<>(
                    mainType,
                    lookupType,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMessage)),
                    operation
                )
            );
        }
    }

    interface TestConfig {
        DataType mainType();

        DataType lookupType();

        BinaryComparisonOperation operation();

        default TestMapping mainIndex() {
            return new TestMapping(MAIN_INDEX, List.of(propertySpecFor(mainFieldName(), mainType())), null);
        }

        /** Make sure the left index has the expected fields and types */
        default void validateMainIndex() {
            validateIndex(MAIN_INDEX, mainFieldName(), sampleDataFor(mainType()));
        }

        /**
         * The same across main indices (necessary for union types).
         */
        default String mainFieldName() {
            return MAIN_INDEX_PREFIX + mainType().esType();
        }

        /**
         * Used for union types. Will have the same main field name, but using a different type.
         */
        default TestMapping additionalMainIndex() {
            return null;
        }

        /** Make sure the additional indexes have the expected fields and types */
        default void validateAdditionalMainIndex() {
            return;
        }

        default String lookupIndexName() {
            return LOOKUP_INDEX_PREFIX + mainType().esType() + "_" + lookupType().esType() + stringForOperation(operation());
        }

        default TestMapping lookupIndex() {
            return new TestMapping(
                lookupIndexName(),
                List.of(propertySpecFor(lookupFieldName(), lookupType()), "\"other\": { \"type\" : \"keyword\" }"),
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.mode", "lookup").build()
            );
        }

        /** Make sure the lookup index has the expected fields and types */
        default void validateLookupIndex() {
            validateIndex(lookupIndexName(), lookupFieldName(), sampleDataFor(lookupType()));
        }

        default String lookupFieldName() {
            return LOOKUP_INDEX_PREFIX + lookupType().esType();
        }

        default String testQuery(BinaryComparisonOperation operation) {
            if (operation != null) {
                String mainField = mainFieldName();
                String lookupField = lookupFieldName();
                String lookupFieldLeft = lookupFieldName() + suffixLeftFieldName(operation);
                String lookupIndex = lookupIndexName();

                return String.format(
                    Locale.ROOT,
                    "FROM %s | EVAL %s = %s | LOOKUP JOIN %s ON %s %s %s | KEEP other",
                    MAIN_INDEX,
                    lookupFieldLeft,
                    mainField,
                    lookupIndex,
                    lookupFieldLeft,
                    operation.symbol(),
                    lookupField
                );
            } else {
                String mainField = mainFieldName();
                String lookupField = lookupFieldName();
                String lookupIndex = lookupIndexName();

                return String.format(
                    Locale.ROOT,
                    "FROM %s | RENAME %s AS %s | LOOKUP JOIN %s ON %s | KEEP other",
                    MAIN_INDEX,
                    mainField,
                    lookupField,
                    lookupIndex,
                    lookupField
                );
            }
        }

        void doTest();
    }

    private static String propertySpecFor(String fieldName, DataType type) {
        if (type == SCALED_FLOAT) {
            return String.format(
                Locale.ROOT,
                "\"%s\": { \"type\" : \"%s\", \"scaling_factor\": %f }",
                fieldName,
                type.esType(),
                SCALING_FACTOR
            );
        }
        return String.format(Locale.ROOT, "\"%s\": { \"type\" : \"%s\" }", fieldName, type.esType().replaceAll("cartesian_", ""));
    }

    private static void validateIndex(String indexName, String fieldName, Object expectedValue) {
        String query = String.format(Locale.ROOT, "FROM %s | KEEP %s", indexName, fieldName);
        try (var response = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
            ColumnInfo info = response.response().columns().getFirst();
            assertThat("Expected index '" + indexName + "' to have column '" + fieldName + ": " + query, info.name(), is(fieldName));
            Iterator<Object> results = response.response().column(0).iterator();
            assertTrue("Expected at least one result for query: " + query, results.hasNext());
            Object indexedResult = response.response().column(0).iterator().next();
            assertThat("Expected valid result: " + query, indexedResult, is(expectedValue));
        }
    }

    /**
     * Test case for a pair of types that can successfully be used in {@code LOOKUP JOIN}.
     */
    private record TestConfigPasses(DataType mainType, DataType lookupType, BinaryComparisonOperation operation) implements TestConfig {
        @Override
        public void doTest() {
            String query = testQuery(operation);
            try (var response = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
                Iterator<Object> results = response.response().column(0).iterator();
                assertTrue("Expected at least one result for query: " + query, results.hasNext());
                Object indexedResult = response.response().column(0).iterator().next();
                List<BinaryComparisonOperation> valueProducingOperations = new ArrayList<>();
                valueProducingOperations.add(null);
                valueProducingOperations.addAll(
                    List.of(BinaryComparisonOperation.EQ, BinaryComparisonOperation.GTE, BinaryComparisonOperation.LTE)
                );
                if (valueProducingOperations.contains(operation)) {
                    assertThat("Expected valid result: " + query, indexedResult, equalTo("value"));
                } else {
                    assertTrue("Expected valid result: " + query, (indexedResult == null) || (indexedResult.equals("value") == false));
                }
            }
        }
    }

    /**
     * Test case for a {@code LOOKUP JOIN} where a field with a mapping conflict is cast to the type of the lookup field.
     */
    private record TestConfigPassesUnionType(
        DataType mainType,
        DataType otherMainType,
        DataType lookupType,
        BinaryComparisonOperation operation
    ) implements TestConfig {
        @Override
        public String lookupIndexName() {
            // Override so it doesn't clash with other lookup indices from non-union type tests.
            return LOOKUP_INDEX_PREFIX
                + mainType().esType()
                + "_union_"
                + otherMainType().esType()
                + "_"
                + lookupType().esType()
                + stringForOperation(operation());
        }

        private String additionalIndexName() {
            return mainFieldName() + "_as_" + otherMainType().typeName() + stringForOperation(operation());
        }

        @Override
        public TestMapping additionalMainIndex() {
            return new TestMapping(additionalIndexName(), List.of(propertySpecFor(mainFieldName(), otherMainType)), null);
        }

        @Override
        public void validateAdditionalMainIndex() {
            validateIndex(additionalIndexName(), mainFieldName(), sampleDataFor(otherMainType));
        }

        @Override
        public String testQuery(BinaryComparisonOperation operation) {
            if (operation == null) {
                String mainField = mainFieldName();
                String lookupField = lookupFieldName();
                String lookupIndex = lookupIndexName();

                return String.format(
                    Locale.ROOT,
                    "FROM %s, %s | EVAL %s = %s::%s | LOOKUP JOIN %s ON %s | KEEP other",
                    MAIN_INDEX,
                    additionalIndexName(),
                    lookupField,
                    mainField,
                    lookupType.typeName(),
                    lookupIndex,
                    lookupField
                );
            } else {
                String mainField = mainFieldName();
                String lookupField = lookupFieldName();
                String lookupIndex = lookupIndexName();
                String lookupFieldLeft = lookupField + suffixLeftFieldName(operation);

                return String.format(
                    Locale.ROOT,
                    "FROM %s, %s | EVAL %s = %s::%s | LOOKUP JOIN %s ON %s %s %s | KEEP other",
                    MAIN_INDEX,
                    additionalIndexName(),
                    lookupFieldLeft,
                    mainField,
                    lookupType.typeName(),
                    lookupIndex,
                    lookupFieldLeft,
                    operation.symbol(),
                    lookupField
                );
            }
        }

        @Override
        public void doTest() {
            String query = testQuery(operation);
            try (var response = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
                Iterator<Object> results = response.response().column(0).iterator();

                assertTrue("Expected at least two results for query, but result was empty: " + query, results.hasNext());
                Object indexedResult = results.next();
                List<BinaryComparisonOperation> valueProducingOperations = new ArrayList<>();
                valueProducingOperations.add(null);
                valueProducingOperations.addAll(
                    List.of(BinaryComparisonOperation.EQ, BinaryComparisonOperation.GTE, BinaryComparisonOperation.LTE)
                );
                if (valueProducingOperations.contains(operation)) {
                    assertThat("Expected valid result: " + query, indexedResult, equalTo("value"));

                    assertTrue("Expected at least two results for query: " + query, results.hasNext());
                    indexedResult = results.next();
                    assertThat("Expected valid result: " + query, indexedResult, equalTo("value"));
                } else {
                    assertTrue("Expected valid result: " + query, (indexedResult == null) || (indexedResult.equals("value") == false));
                }
            }
        }
    }

    /**
     * Test case for a pair of types that generate an error message when used in {@code LOOKUP JOIN}.
     */
    private record TestConfigFails<E extends Exception>(
        DataType mainType,
        DataType lookupType,
        Class<E> exception,
        Consumer<E> assertion,
        BinaryComparisonOperation operation
    ) implements TestConfig {
        @Override
        public void doTest() {
            String query = testQuery(operation);
            E e = expectThrows(
                exception(),
                "Expected exception " + exception().getSimpleName() + " but no exception was thrown: " + query,
                () -> {
                    // noinspection EmptyTryBlock
                    try (var ignored = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
                        // We use try-with-resources to ensure the request is closed if the exception is not thrown (less cluttered errors)
                    }
                }
            );
            assertion().accept(e);
        }
    }

    private boolean isValidDataType(DataType dataType) {
        return UNDER_CONSTRUCTION.contains(dataType) == false;
    }

    private static void saveJoinTypes(Supplier<Set<DocsV3Support.TypeSignature>> signatures) throws Exception {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        ArrayList<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>();
        args.add(new EsqlFunctionRegistry.ArgSignature("field from the left index", null, null, false, false));
        args.add(new EsqlFunctionRegistry.ArgSignature("field from the lookup index", null, null, false, false));
        DocsV3Support.CommandsDocsSupport docs = new DocsV3Support.CommandsDocsSupport(
            "lookup-join",
            LookupJoinTypesIT.class,
            null,
            args,
            signatures,
            DocsV3Support.callbacksFromSystemProperty()
        );
        docs.renderDocs();
    }
}
