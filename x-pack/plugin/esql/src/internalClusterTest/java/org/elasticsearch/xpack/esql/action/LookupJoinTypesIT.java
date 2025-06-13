/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOC_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
 */
@ClusterScope(scope = SUITE, numClientNodes = 1, numDataNodes = 1)
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

    private static final Map<String, TestConfigs> testConfigurations = new HashMap<>();
    static {
        // Initialize the test configurations for string tests
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("strings", TestConfigs::new);
            configs.addPasses(KEYWORD, KEYWORD);
            configs.addPasses(TEXT, KEYWORD);
            configs.addFailsUnsupported(KEYWORD, TEXT);
        }

        // Test integer types
        var integerTypes = List.of(BYTE, SHORT, INTEGER, LONG);
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("integers", TestConfigs::new);
            for (DataType mainType : integerTypes) {
                for (DataType lookupType : integerTypes) {
                    configs.addPasses(mainType, lookupType);
                }
            }
        }

        // Test float and double
        var floatTypes = List.of(HALF_FLOAT, FLOAT, DOUBLE, SCALED_FLOAT);
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("floats", TestConfigs::new);
            for (DataType mainType : floatTypes) {
                for (DataType lookupType : floatTypes) {
                    configs.addPasses(mainType, lookupType);
                }
            }
        }

        // Tests for mixed-numerical types
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("mixed-numerical", TestConfigs::new);
            for (DataType mainType : integerTypes) {
                for (DataType lookupType : floatTypes) {
                    configs.addPasses(mainType, lookupType);
                    configs.addPasses(lookupType, mainType);
                }
            }
        }

        // Tests for union types; non-exhaustive but includes every valid type
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("multi-typed", TestConfigs::new);

        }

        // Tests for mixed-date/time types
        var dateTypes = List.of(DATETIME, DATE_NANOS);
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("mixed-temporal", TestConfigs::new);
            for (DataType mainType : dateTypes) {
                for (DataType lookupType : dateTypes) {
                    if (mainType != lookupType) {
                        configs.addFails(mainType, lookupType);
                    }
                }
            }
        }

        // Tests for all unsupported types
        DataType[] unsupported = Join.UNSUPPORTED_TYPES;
        {
            Collection<TestConfigs> existing = testConfigurations.values();
            TestConfigs configs = testConfigurations.computeIfAbsent("unsupported", TestConfigs::new);
            for (DataType type : unsupported) {
                if (type == NULL
                    || type == DOC_DATA_TYPE
                    || type == TSID_DATA_TYPE
                    || type == AGGREGATE_METRIC_DOUBLE
                    || type.esType() == null
                    || type.isCounter()
                    || DataType.isRepresentable(type) == false) {
                    // Skip unmappable types, or types not supported in ES|QL in general
                    continue;
                }
                if (existingIndex(existing, type, type)) {
                    // Skip existing configurations
                    continue;
                }
                configs.addFailsUnsupported(type, type);
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
            TestConfigs configs = testConfigurations.computeIfAbsent("same", TestConfigs::new);
            for (DataType type : supported) {
                assertThat("Claiming supported for unsupported type: " + type, List.of(unsupported).contains(type), is(false));
                if (existingIndex(existing, type, type) == false) {
                    // Only add the configuration if it doesn't already exist
                    configs.addPasses(type, type);
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
            TestConfigs configs = testConfigurations.computeIfAbsent("others", TestConfigs::new);
            for (DataType mainType : supported) {
                for (DataType lookupType : supported) {
                    if (existingIndex(existing, mainType, lookupType) == false) {
                        // Only add the configuration if it doesn't already exist
                        configs.addFails(mainType, lookupType);
                    }
                }
            }
        }

        // Make sure we have never added two configurations with the same index name
        Set<String> knownTypes = new HashSet<>();
        for (TestConfigs configs : testConfigurations.values()) {
            for (TestConfig config : configs.configs.values()) {
                String lookupIndexName = lookupIndexName(config.mainType(), config.lookupType());
                if (knownTypes.contains(lookupIndexName)) {
                    throw new IllegalArgumentException("Duplicate index name: " + lookupIndexName);
                }
                knownTypes.add(lookupIndexName);
            }
        }
    }

    private static boolean existingIndex(Collection<TestConfigs> existing, DataType mainType, DataType lookupType) {
        String indexName = LOOKUP_INDEX_PREFIX + mainType.esType() + "_" + lookupType.esType();
        return existing.stream().anyMatch(c -> c.exists(indexName));
    }

    public void testLookupJoinStrings() {
        testLookupJoinTypes("strings");
    }

    public void testLookupJoinIntegers() {
        testLookupJoinTypes("integers");
    }

    public void testLookupJoinFloats() {
        testLookupJoinTypes("floats");
    }

    public void testLookupJoinMixedNumerical() {
        testLookupJoinTypes("mixed-numerical");
    }

    public void testLookupJoinMixedTemporal() {
        testLookupJoinTypes("mixed-temporal");
    }

    public void testLookupJoinSame() {
        testLookupJoinTypes("same");
    }

    public void testLookupJoinUnsupported() {
        testLookupJoinTypes("unsupported");
    }

    public void testLookupJoinOthers() {
        testLookupJoinTypes("others");
    }

    private void testLookupJoinTypes(String group) {
        initIndexes(group);
        initData(group);
        for (TestConfig config : testConfigurations.get(group).configs.values()) {
            if ((isValidDataType(config.mainType()) && isValidDataType(config.lookupType())) == false) {
                continue;
            }
            String mainField = mainFieldName(config.mainType());
            String lookupField = lookupFieldName(config.lookupType());
            String lookupIndex = lookupIndexName(config.mainType(), config.lookupType());
            String query = "FROM "
                + MAIN_INDEX
                + " | RENAME "
                + mainField
                + " AS "
                + lookupField
                + " | LOOKUP JOIN "
                + lookupIndex
                + " ON "
                + lookupField
                + " | KEEP other";

            validateIndex(MAIN_INDEX, mainField, config.mainType());
            validateIndex(lookupIndex, lookupField, config.lookupType());
            config.testQuery(query);
        }
    }

    private void initIndexes(String group) {
        Collection<TestConfig> configs = testConfigurations.get(group).configs.values();
        String propertyPrefix = "{\n  \"properties\" : {\n";
        String propertySuffix = "  }\n}\n";
        // The main index will have many fields, one of each type to use in later type specific joins
        String mainFields = propertyPrefix + configs.stream()
            .map(config -> mainPropertySpec(config.mainType()))
            .distinct()
            .collect(Collectors.joining(",\n    ")) + propertySuffix;
        assertAcked(prepareCreate(MAIN_INDEX).setMapping(mainFields));

        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.mode", "lookup");
        configs.forEach(
            // Each lookup index will get a document with a field to join on, and a results field to get back
            (c) -> assertAcked(
                prepareCreate(lookupIndexName(c.mainType(), c.lookupType())).setSettings(settings.build())
                    .setMapping(propertyPrefix + lookupPropertySpec(c.lookupType()) + propertySuffix)
            )
        );
    }

    private void initData(String group) {
        Collection<TestConfig> configs = testConfigurations.get(group).configs.values();
        int docId = 0;
        for (TestConfig config : configs) {
            String lookupIndexName = lookupIndexName(config.mainType(), config.lookupType());
            String doc = String.format(Locale.ROOT, """
                {
                  %s,
                  "other": "value"
                }
                """, lookupPropertyFor(config));
            index(lookupIndexName, "" + (++docId), doc);
            refresh(lookupIndexName);
        }
        List<String> mainProperties = configs.stream().map(this::mainPropertyFor).distinct().collect(Collectors.toList());
        index(MAIN_INDEX, "1", String.format(Locale.ROOT, """
            {
              %s
            }
            """, String.join(",\n  ", mainProperties)));
        refresh(MAIN_INDEX);
    }

    private String lookupPropertyFor(TestConfig config) {
        return String.format(Locale.ROOT, "\"%s\": %s", lookupFieldName(config.lookupType()), sampleDataTextFor(config.lookupType()));
    }

    private String mainPropertyFor(TestConfig config) {
        return String.format(Locale.ROOT, "\"%s\": %s", mainFieldName(config.mainType()), sampleDataTextFor(config.mainType()));
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

    private static class TestConfigs {
        final String group;
        final Map<String, TestConfig> configs;

        TestConfigs(String group) {
            this.group = group;
            this.configs = new LinkedHashMap<>();
        }

        private boolean exists(String indexName) {
            return configs.containsKey(indexName);
        }

        private void add(TestConfig config) {
            String lookupIndexName = lookupIndexName(config.mainType(), config.lookupType());
            if (configs.containsKey(lookupIndexName)) {
                throw new IllegalArgumentException("Duplicate index name: " + lookupIndexName);
            }
            configs.put(lookupIndexName, config);
        }

        private void addPasses(DataType mainType, DataType lookupType) {
            add(new TestConfigPasses(mainType, lookupType, true));
        }

        private void addFails(DataType mainType, DataType lookupType) {
            String fieldName = LOOKUP_INDEX_PREFIX + lookupType.esType();
            String errorMessage = String.format(
                Locale.ROOT,
                "JOIN left field [%s] of type [%s] is incompatible with right field [%s] of type [%s]",
                fieldName,
                mainType.widenSmallNumeric(),
                fieldName,
                lookupType.widenSmallNumeric()
            );
            add(
                new TestConfigFails<>(
                    mainType,
                    lookupType,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMessage))
                )
            );
        }

        private void addFailsUnsupported(DataType mainType, DataType lookupType) {
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
                    e -> assertThat(e.getMessage(), containsString(errorMessage))
                )
            );
        }
    }

    interface TestConfig {
        DataType mainType();

        DataType lookupType();

        void testQuery(String query);
    }

    private static String lookupIndexName(DataType mainType, DataType lookupType) {
        return LOOKUP_INDEX_PREFIX + mainType.esType() + "_" + lookupType.esType();
    }

    private static String mainFieldName(DataType mainType) {
        return MAIN_INDEX_PREFIX + mainType.esType();
    }

    private static String lookupFieldName(DataType lookupType) {
        return LOOKUP_INDEX_PREFIX + lookupType.esType();
    }

    private static String mainPropertySpec(DataType mainType) {
        return propertySpecFor(mainFieldName(mainType), mainType, "");
    }

    private static String lookupPropertySpec(DataType lookupType) {
        return propertySpecFor(lookupFieldName(lookupType), lookupType, ", \"other\": { \"type\" : \"keyword\" }");
    }

    private static String propertySpecFor(String fieldName, DataType type, String extra) {
        if (type == SCALED_FLOAT) {
            return String.format(
                Locale.ROOT,
                "\"%s\": { \"type\" : \"%s\", \"scaling_factor\": %f }",
                fieldName,
                type.esType(),
                SCALING_FACTOR
            ) + extra;
        }
        return String.format(Locale.ROOT, "\"%s\": { \"type\" : \"%s\" }", fieldName, type.esType().replaceAll("cartesian_", "")) + extra;
    }

    private static void validateIndex(String indexName, String fieldName, DataType fieldType) {
        validateIndex(indexName, fieldName, sampleDataFor(fieldType));
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

    private record TestConfigPasses(DataType mainType, DataType lookupType, boolean hasResults) implements TestConfig {
        @Override
        public void testQuery(String query) {
            try (var response = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
                Iterator<Object> results = response.response().column(0).iterator();
                assertTrue("Expected at least one result for query: " + query, results.hasNext());
                Object indexedResult = response.response().column(0).iterator().next();
                if (hasResults) {
                    assertThat("Expected valid result: " + query, indexedResult, equalTo("value"));
                } else {
                    assertThat("Expected empty results for query: " + query, indexedResult, is(nullValue()));
                }
            }
        }
    }

    private record TestConfigFails<E extends Exception>(DataType mainType, DataType lookupType, Class<E> exception, Consumer<E> assertion)
        implements
            TestConfig {
        @Override
        public void testQuery(String query) {
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
        return UNDER_CONSTRUCTION.get(dataType) == null || UNDER_CONSTRUCTION.get(dataType).isEnabled();
    }
}
