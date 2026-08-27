/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Build;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.SupportedVersion;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLATTENED;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.HALF_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SCALED_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TDIGEST;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isCounter;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This test suite tests the {@code WHERE field IN (subquery)} functionality in ES|QL with various data types.
 * <p>
 * Three indices are created:
 * <ul>
 *     <li>{@code main_index} (regular index) — holds {@code main_<type>} for every non-counter test type and supplies the left-hand
 *     side of the IN comparison.</li>
 *     <li>{@code sub_index} (regular index) — holds {@code sub_<type>} for every non-counter test type and supplies the right-hand
 *     side.</li>
 *     <li>{@code tsdb_index} ({@code time_series}-mode index) — holds the counter-typed {@code main_counter_*} and {@code sub_counter_*}
 *     fields, plus the {@code @timestamp} / {@code dim} fields that TSDB requires. Both sides of an IN comparison whose type is
 *     {@code COUNTER_*} route here through {@link TestConfig#mainIndex} / {@link TestConfig#subIndex}.</li>
 * </ul>
 * Every {@code (mainType, subType)} pair is then exercised as
 * {@code FROM <main_idx> | WHERE main_<x> IN (FROM <sub_idx> | KEEP sub_<y>) | KEEP <output>}. For valid combinations the row should
 * come back; for invalid combinations the analyzer should reject the query with a {@link VerificationException}. If no exception is
 * thrown and no row is returned, our validation rules are out of sync with the runtime behaviour.
 * <ul>
 *     <li>The left and right effective types must be equal, or both must be string types ({@code KEYWORD} or {@code TEXT}). Small
 *     numerics are widened on load by the analyzer ({@code BYTE} / {@code SHORT} → {@code INTEGER}; {@code FLOAT} / {@code HALF_FLOAT}
 *     / {@code SCALED_FLOAT} → {@code DOUBLE}), so e.g. a {@code BYTE} field can be compared against an {@code INTEGER} subquery and a
 *     {@code HALF_FLOAT} field against a {@code DOUBLE} subquery.</li>
 *     <li>The right-hand type must not be in {@code Join.UNSUPPORTED_TYPES} — with the exception that {@code TEXT} and {@code VERSION}
 *     are allowed in the IN subquery's right side (unlike {@code LOOKUP JOIN}).</li>
 * </ul>
 */
@ClusterScope(scope = SUITE, numClientNodes = 1, numDataNodes = 1)
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class InSubqueryTypesIT extends ESIntegTestCase {

    private static final String MAIN_INDEX = "main_index";
    private static final String SUB_INDEX = "sub_index";
    /**
     * Time-series-mode index that holds the {@code main_counter_*} / {@code sub_counter_*} fields. Counter typed columns can only be
     * produced by a field with {@code time_series_metric=counter}, which in turn requires the whole index to be in
     * {@code time_series} mode (with dimension fields and a {@code @timestamp} field). The index hosts both the main and sub copies of
     * every counter field so a counter pair just routes both sides of its query to {@link #TSDB_INDEX} and stays inside the standard
     * {@link TestConfig} machinery.
     */
    private static final String TSDB_INDEX = "tsdb_index";
    private static final String MAIN_FIELD_PREFIX = "main_";
    private static final String SUB_FIELD_PREFIX = "sub_";
    private static final double SCALING_FACTOR = 10.0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            // Must precede the ESQL plugin: EsqlPlugin.createComponents reads EncryptionServiceRegistry, populated by this stub.
            TestEncryptionServicePlugin.class,
            EsqlPluginWithEnterpriseOrTrialLicense.class,
            MapperExtrasPlugin.class,
            VersionFieldPlugin.class,
            UnsignedLongMapperPlugin.class,
            SpatialPlugin.class,
            AnalyticsPlugin.class,
            AggregateMetricMapperPlugin.class
        );
    }

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    @Before
    public void setupIndices() {
        // ESIntegTestCase wipes indices between tests, so we (re)create the shared main / sub indices in @Before. Both indices contain
        // one field per testable type; the sample data for that type fills the field so that for valid pairs the IN match succeeds.
        createIndex(MAIN_INDEX, MAIN_FIELD_PREFIX);
        createIndex(SUB_INDEX, SUB_FIELD_PREFIX);
        indexSampleDocument(MAIN_INDEX, MAIN_FIELD_PREFIX);
        indexSampleDocument(SUB_INDEX, SUB_FIELD_PREFIX);
        createTsdbIndex();
    }

    private void createTsdbIndex() {
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("dim"))
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(TSDB_INDEX)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "dim",
                    "type=keyword,time_series_dimension=true",
                    MAIN_FIELD_PREFIX + COUNTER_LONG.esType(),
                    "type=long,time_series_metric=counter",
                    MAIN_FIELD_PREFIX + COUNTER_INTEGER.esType(),
                    "type=integer,time_series_metric=counter",
                    MAIN_FIELD_PREFIX + COUNTER_DOUBLE.esType(),
                    "type=double,time_series_metric=counter",
                    SUB_FIELD_PREFIX + COUNTER_LONG.esType(),
                    "type=long,time_series_metric=counter",
                    SUB_FIELD_PREFIX + COUNTER_INTEGER.esType(),
                    "type=integer,time_series_metric=counter",
                    SUB_FIELD_PREFIX + COUNTER_DOUBLE.esType(),
                    "type=double,time_series_metric=counter"
                )
        );
        client().prepareIndex(TSDB_INDEX)
            .setSource(
                "@timestamp",
                "2025-04-02T12:00:00Z",
                "dim",
                "host1",
                MAIN_FIELD_PREFIX + COUNTER_LONG.esType(),
                1L,
                MAIN_FIELD_PREFIX + COUNTER_INTEGER.esType(),
                1,
                MAIN_FIELD_PREFIX + COUNTER_DOUBLE.esType(),
                1.0,
                SUB_FIELD_PREFIX + COUNTER_LONG.esType(),
                1L,
                SUB_FIELD_PREFIX + COUNTER_INTEGER.esType(),
                1,
                SUB_FIELD_PREFIX + COUNTER_DOUBLE.esType(),
                1.0
            )
            .get();
        client().admin().indices().prepareRefresh(TSDB_INDEX).get();
    }

    /**
     * Types that can be mapped via REST and supported by {@code WHERE field IN (subquery)}. {@code TEXT} and {@code VERSION} are listed
     * here because they are valid right-hand-side types for {@code IN}.
     */
    private static final DataType[] SUPPORTED = {
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
        TEXT,
        VERSION,
        SCALED_FLOAT };

    /**
     * Subset of {@link Join#UNSUPPORTED_TYPES} that we cannot exercise with a regular REST mapping in this test. We split them out so
     * the {@link #UNSUPPORTED} derivation below stays automatically in sync with {@link Join#UNSUPPORTED_TYPES} for any new
     * type that has a regular REST mapping:
     * <ul>
     *     <li>{@code UNSUPPORTED}, {@code NULL}, {@code OBJECT}, {@code SOURCE}, {@code PARTIAL_AGG}, {@code DOC_DATA_TYPE},
     *     {@code TSID_DATA_TYPE}: internal types with no public ES field mapping.</li>
     *     <li>{@code DATE_PERIOD}, {@code TIME_DURATION}: ES|QL-only types (no {@code esType}).</li>
     *     <li>{@code GEOHASH}, {@code GEOTILE}, {@code GEOHEX}: aggregation result types, not directly mappable as fields.</li>
     * </ul>
     */
    private static final Set<DataType> NOT_MAPPABLE_VIA_REST = Set.of(
        DataType.UNSUPPORTED,
        DataType.NULL,
        DataType.OBJECT,
        DataType.SOURCE,
        DataType.PARTIAL_AGG,
        DataType.DOC_DATA_TYPE,
        DataType.TSID_DATA_TYPE,
        DataType.DATE_PERIOD,
        DataType.TIME_DURATION,
        DataType.GEOHASH,
        DataType.GEOTILE,
        DataType.GEOHEX,
        DataType.LAMBDA
    );

    /**
     * Types that the {@link AbstractSubqueryJoin}'s post-analysis verifier rejects these data types on an IN subquery. Derived from
     * {@link Join#UNSUPPORTED_TYPES} and filtered through {@link AbstractSubqueryJoin#isSubqueryJoinUnsupported(DataType)} so the test
     * automatically picks up any new unsupported type that comes with a regular REST mapping; the entries in
     * {@link #NOT_MAPPABLE_VIA_REST} are filtered out.
     */
    private static final List<DataType> UNSUPPORTED;
    static {
        List<DataType> list = new ArrayList<>();
        for (DataType t : Join.UNSUPPORTED_TYPES) {
            if (AbstractSubqueryJoin.isSubqueryJoinUnsupported(t) == false) {
                continue; // TEXT and VERSION are allowed on the right side of an IN subquery even though Join rejects them
            }
            if (NOT_MAPPABLE_VIA_REST.contains(t)) {
                continue; // skip data types that cannot be mapped
            }
            list.add(t);
        }
        UNSUPPORTED = List.copyOf(list);
    }

    /**
     * Types whose mapped form does not survive a plain {@code FROM index | KEEP field} round-trip with a comparable scalar value (either
     * because ES|QL surfaces them as an UNSUPPORTED column or because the round-tripped representation isn't equal to the JSON used at
     * index time). The indexing setup still creates fields for them so that the actual IN subquery test exercises them; only the
     * post-index scalar value check is skipped.
     */
    private static final Set<DataType> SKIP_VALIDATION_AFTER_POPULATING_INDICES = Set.of(
        DATE_RANGE,
        DOUBLE_RANGE,
        FLATTENED,
        DENSE_VECTOR,
        AGGREGATE_METRIC_DOUBLE,
        HISTOGRAM,
        EXPONENTIAL_HISTOGRAM,
        TDIGEST
    );

    /**
     * All types that get a column in both {@code main_index} and {@code sub_index}, including under-construction types
     * ({@code DATE_RANGE}, {@code DOUBLE_RANGE}, {@code FLATTENED} as of writing): on snapshot builds these are reported as
     * {@code supportedOn(...) == true} by {@link SupportedVersion#underConstruction}, so the analyzer surfaces them with their proper
     * {@code DataType}. On release builds they surface as {@link DataType#UNSUPPORTED} instead, so the pairs that involve them are skipped
     * at test time (see {@link #testableOnCurrentBuild}). Counter types are intentionally excluded — they live in {@link #TSDB_INDEX}
     * instead.
     */
    private static final List<DataType> ALL_TEST_TYPES;
    static {
        Set<DataType> types = new LinkedHashSet<>();
        types.addAll(Arrays.asList(SUPPORTED));
        for (DataType t : UNSUPPORTED) {
            if (isCounter(t) == false) {
                types.add(t);
            }
        }
        ALL_TEST_TYPES = List.copyOf(types);
    }

    /**
     * Type families shared by {@link #testConfigurations} and {@link #multiColumnTestConfigurations}:
     * <ul>
     *     <li>{@link #STRING_TYPES}: KEYWORD and TEXT are interchangeable on either side of IN.</li>
     *     <li>{@link #INTEGER_TYPES}: BYTE / SHORT widen to INTEGER on load, so they are compatible with each other and with INTEGER;
     *     LONG stays LONG and is incompatible with the rest.</li>
     *     <li>{@link #FLOAT_TYPES}: all widen to DOUBLE on load, so every pair is compatible.</li>
     *     <li>{@link #DATE_TYPES}: DATETIME and DATE_NANOS are not interchangeable.</li>
     * </ul>
     */
    private static final List<DataType> STRING_TYPES = List.of(KEYWORD, TEXT);
    private static final List<DataType> INTEGER_TYPES = List.of(BYTE, SHORT, INTEGER, LONG);
    private static final List<DataType> FLOAT_TYPES = List.of(HALF_FLOAT, FLOAT, DOUBLE, SCALED_FLOAT);
    private static final List<DataType> DATE_TYPES = List.of(DATETIME, DATE_NANOS);

    private static final Map<String, TestConfigs> testConfigurations = new HashMap<>();
    static {
        // Strings: KEYWORD and TEXT are interchangeable on either side, so every pair passes.
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("strings", TestConfigs::new);
            for (DataType mainType : STRING_TYPES) {
                for (DataType subType : STRING_TYPES) {
                    configs.addPasses(mainType, subType);
                }
            }
        }

        // Integers. Small whole numerics are widened on load: BYTE / SHORT -> INTEGER. Pairs whose effective types match pass; LONG
        // remains LONG, so any LONG-vs-(BYTE/SHORT/INTEGER) pair fails (with the widened type names in the error).
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("integers", TestConfigs::new);
            for (DataType mainType : INTEGER_TYPES) {
                for (DataType subType : INTEGER_TYPES) {
                    if (semiJoinCompatible(mainType, subType)) {
                        configs.addPasses(mainType, subType);
                    } else {
                        configs.addFailsMismatch(mainType, subType);
                    }
                }
            }
        }

        // Floating-point types: FLOAT, HALF_FLOAT, SCALED_FLOAT, DOUBLE all widen to DOUBLE, so every pair is compatible.
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("floats", TestConfigs::new);
            for (DataType mainType : FLOAT_TYPES) {
                for (DataType subType : FLOAT_TYPES) {
                    assertThat("Float types should all widen to DOUBLE", semiJoinCompatible(mainType, subType), is(true));
                    configs.addPasses(mainType, subType);
                }
            }
        }

        // Mixed numerical: integer-vs-float (in either direction) always fails — INTEGER / LONG vs DOUBLE never collapse to the same
        // widened type.
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("mixed-numerical", TestConfigs::new);
            for (DataType mainType : INTEGER_TYPES) {
                for (DataType subType : FLOAT_TYPES) {
                    configs.addFailsMismatch(mainType, subType);
                    configs.addFailsMismatch(subType, mainType);
                }
            }
        }

        // Mixed temporal: DATETIME / DATE_NANOS are not interchangeable.
        {
            TestConfigs configs = testConfigurations.computeIfAbsent("mixed-temporal", TestConfigs::new);
            for (DataType mainType : DATE_TYPES) {
                for (DataType subType : DATE_TYPES) {
                    if (mainType != subType) {
                        configs.addFailsMismatch(mainType, subType);
                    }
                }
            }
        }

        // Unsupported types. Each pair uses the same type on both sides so the only triggered failure is
        // "IN subquery with right field [...] of type [...] is not supported".
        {
            Collection<TestConfigs> existing = testConfigurations.values();
            TestConfigs configs = testConfigurations.computeIfAbsent("unsupported", TestConfigs::new);
            for (DataType type : UNSUPPORTED) {
                if (existingPair(existing, type, type)) {
                    continue;
                }
                configs.addFailsUnsupported(type, type);
            }
        }

        // Same-type pairs for the supported list — all should pass.
        {
            Collection<TestConfigs> existing = testConfigurations.values();
            TestConfigs configs = testConfigurations.computeIfAbsent("same", TestConfigs::new);
            for (DataType type : SUPPORTED) {
                assertThat(
                    "Claiming supported for unsupported type: " + type,
                    AbstractSubqueryJoin.isSubqueryJoinUnsupported(type),
                    is(false)
                );
                if (existingPair(existing, type, type) == false) {
                    configs.addPasses(type, type);
                }
            }
        }

        // All remaining supported pairs (different left/right types not already covered above) — all fail with a type mismatch error.
        {
            Collection<TestConfigs> existing = testConfigurations.values();
            TestConfigs configs = testConfigurations.computeIfAbsent("others", TestConfigs::new);
            for (DataType mainType : SUPPORTED) {
                for (DataType subType : SUPPORTED) {
                    if (existingPair(existing, mainType, subType) == false) {
                        configs.addFailsMismatch(mainType, subType);
                    }
                }
            }
        }

        // Sanity check: every DataType must be covered either by SUPPORTED or by Join.UNSUPPORTED_TYPES, so any newly added DataType
        // either gets exercised as supported here or is automatically picked up in the UNSUPPORTED derivation. Mirrors the equivalent
        // assertion in LookupJoinTypesIT.
        List<DataType> missing = new ArrayList<>();
        for (DataType type : DataType.values()) {
            boolean isSupported = Arrays.asList(SUPPORTED).contains(type);
            boolean isUnsupported = Arrays.asList(Join.UNSUPPORTED_TYPES).contains(type);
            if (isSupported == false && isUnsupported == false) {
                missing.add(type);
            }
        }
        assertThat(missing + " are not in the SUPPORTED or Join.UNSUPPORTED_TYPES list", missing.size(), is(0));

        // Sanity check: no two configurations claim the same (mainType, subType) pair (which would silently drop a test).
        Set<String> knownPairs = new HashSet<>();
        for (TestConfigs configs : testConfigurations.values()) {
            for (TestConfig config : configs.configs.values()) {
                if (knownPairs.contains(config.pairKey())) {
                    throw new IllegalArgumentException("Duplicate test pair: " + config.pairKey());
                }
                knownPairs.add(config.pairKey());
            }
        }
    }

    /**
     * Two-column test configurations where both column types are independently configurable. The groups mirror {@link #testConfigurations}:
     * <ul>
     *     <li>The within-family groups ({@code mc-strings}, {@code mc-integers}, {@code mc-floats}) cross every
     *     {@code (mainType, subType)} pair of the family in the first column with every pair of the family in the second column; each
     *     column's expected outcome is decided independently by {@link #semiJoinCompatible}, covering pass/pass, single-column and
     *     both-column failures.</li>
     *     <li>The remaining groups ({@code mc-mixed-numerical}, {@code mc-mixed-temporal}, {@code mc-unsupported}, {@code mc-same},
     *     {@code mc-others}) sweep each single-column pair through both column positions (and, where interesting, both at once),
     *     anchoring the other column to the always-compatible {@code KEYWORD} pair so the swept column is the only source of
     *     failure.</li>
     * </ul>
     * Counter types are excluded throughout — the multi-column query reads only {@code main_index}, which has no counter fields.
     */
    private static final Map<String, MultiColumnTestConfigs> multiColumnTestConfigurations = new HashMap<>();
    static {
        // Pair used for the non-swept column: KEYWORD IN KEYWORD always passes.
        DataType anchor = KEYWORD;

        // mc-strings: KEYWORD and TEXT are interchangeable on either side, so every combination across both columns passes.
        {
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent("mc-strings", MultiColumnTestConfigs::new);
            for (DataType mainType1 : STRING_TYPES) {
                for (DataType subType1 : STRING_TYPES) {
                    for (DataType mainType2 : STRING_TYPES) {
                        for (DataType subType2 : STRING_TYPES) {
                            configs.addPasses(mainType1, subType1, mainType2, subType2);
                        }
                    }
                }
            }
        }

        // mc-integers: every integer pair in column 1 crossed with every integer pair in column 2. BYTE / SHORT widen to INTEGER so
        // they are compatible with each other but incompatible with LONG; each column passes or fails independently.
        {
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent("mc-integers", MultiColumnTestConfigs::new);
            for (DataType mainType1 : INTEGER_TYPES) {
                for (DataType subType1 : INTEGER_TYPES) {
                    for (DataType mainType2 : INTEGER_TYPES) {
                        for (DataType subType2 : INTEGER_TYPES) {
                            boolean col1Compatible = semiJoinCompatible(mainType1, subType1);
                            boolean col2Compatible = semiJoinCompatible(mainType2, subType2);
                            if (col1Compatible && col2Compatible) {
                                configs.addPasses(mainType1, subType1, mainType2, subType2);
                            } else if (col1Compatible) {
                                configs.addFailsMismatchCol2(mainType1, subType1, mainType2, subType2);
                            } else if (col2Compatible) {
                                configs.addFailsMismatchCol1(mainType1, subType1, mainType2, subType2);
                            } else {
                                configs.addFailsMismatchBoth(mainType1, subType1, mainType2, subType2);
                            }
                        }
                    }
                }
            }
        }

        // mc-floats: FLOAT / HALF_FLOAT / SCALED_FLOAT / DOUBLE all widen to DOUBLE, so every combination across both columns passes.
        {
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent("mc-floats", MultiColumnTestConfigs::new);
            for (DataType mainType1 : FLOAT_TYPES) {
                for (DataType subType1 : FLOAT_TYPES) {
                    for (DataType mainType2 : FLOAT_TYPES) {
                        for (DataType subType2 : FLOAT_TYPES) {
                            assertThat(
                                "Float types should all widen to DOUBLE",
                                semiJoinCompatible(mainType1, subType1) && semiJoinCompatible(mainType2, subType2),
                                is(true)
                            );
                            configs.addPasses(mainType1, subType1, mainType2, subType2);
                        }
                    }
                }
            }
        }

        // mc-mixed-numerical: integer-vs-float (in either direction) always fails. Sweep every mismatching pair through column 1 and
        // column 2 (with the anchor pair in the other column), plus a both-columns-fail combination per type pair.
        {
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent(
                "mc-mixed-numerical",
                MultiColumnTestConfigs::new
            );
            for (DataType intType : INTEGER_TYPES) {
                for (DataType floatType : FLOAT_TYPES) {
                    configs.addFailsMismatchCol1(intType, floatType, anchor, anchor);
                    configs.addFailsMismatchCol1(floatType, intType, anchor, anchor);
                    configs.addFailsMismatchCol2(anchor, anchor, intType, floatType);
                    configs.addFailsMismatchCol2(anchor, anchor, floatType, intType);
                    configs.addFailsMismatchBoth(intType, floatType, floatType, intType);
                }
            }
        }

        // mc-mixed-temporal: DATETIME / DATE_NANOS are not interchangeable; sweep the mismatch through each column position and both.
        {
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent(
                "mc-mixed-temporal",
                MultiColumnTestConfigs::new
            );
            for (DataType mainType : DATE_TYPES) {
                for (DataType subType : DATE_TYPES) {
                    if (mainType != subType) {
                        configs.addFailsMismatchCol1(mainType, subType, anchor, anchor);
                        configs.addFailsMismatchCol2(anchor, anchor, mainType, subType);
                        configs.addFailsMismatchBoth(mainType, subType, subType, mainType);
                    }
                }
            }
        }

        // mc-unsupported: every REST-mappable type the IN subquery verifier rejects, swept through column 1, column 2, and both. The
        // same type is used on both sides of the swept column so the only triggered failure is "IN subquery with right field [...] of
        // type [...] is not supported". Counter types are skipped because main_index has no counter fields.
        {
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent("mc-unsupported", MultiColumnTestConfigs::new);
            for (DataType type : UNSUPPORTED) {
                if (isCounter(type)) {
                    continue;
                }
                configs.addFailsUnsupportedCol1(type, type, anchor, anchor);
                configs.addFailsUnsupportedCol2(anchor, anchor, type, type);
                configs.addFailsUnsupportedBoth(type, type, type, type);
            }
        }

        // mc-same: same-type pairs for every supported type, swept through each column with the anchor pair in the other — all pass.
        {
            Collection<MultiColumnTestConfigs> existing = multiColumnTestConfigurations.values();
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent("mc-same", MultiColumnTestConfigs::new);
            for (DataType type : SUPPORTED) {
                if (existingQuad(existing, type, type, anchor, anchor) == false) {
                    configs.addPasses(type, type, anchor, anchor);
                }
                if (existingQuad(existing, anchor, anchor, type, type) == false) {
                    configs.addPasses(anchor, anchor, type, type);
                }
            }
        }

        // mc-others: all remaining supported pair mismatches (different left/right types not covered above), swept through both
        // column positions with the anchor pair in the other column — all fail with a type mismatch on the swept column.
        {
            Collection<MultiColumnTestConfigs> existing = multiColumnTestConfigurations.values();
            MultiColumnTestConfigs configs = multiColumnTestConfigurations.computeIfAbsent("mc-others", MultiColumnTestConfigs::new);
            for (DataType mainType : SUPPORTED) {
                for (DataType subType : SUPPORTED) {
                    if (semiJoinCompatible(mainType, subType)) {
                        continue;
                    }
                    if (existingQuad(existing, mainType, subType, anchor, anchor) == false) {
                        configs.addFailsMismatchCol1(mainType, subType, anchor, anchor);
                    }
                    if (existingQuad(existing, anchor, anchor, mainType, subType) == false) {
                        configs.addFailsMismatchCol2(anchor, anchor, mainType, subType);
                    }
                }
            }
        }

        // Sanity check: no two multi-column configurations claim the same type quad (which would silently drop a test).
        Set<String> knownQuads = new HashSet<>();
        for (MultiColumnTestConfigs configs : multiColumnTestConfigurations.values()) {
            for (MultiColumnTestConfig config : configs.configs.values()) {
                if (knownQuads.add(config.quadKey()) == false) {
                    throw new IllegalArgumentException("Duplicate multi-column test quad: " + config.quadKey());
                }
            }
        }
    }

    private static boolean existingPair(Collection<TestConfigs> existing, DataType mainType, DataType subType) {
        String key = pairKey(mainType, subType);
        return existing.stream().anyMatch(c -> c.exists(key));
    }

    private static String pairKey(DataType mainType, DataType subType) {
        return mainType.esType() + "_" + subType.esType();
    }

    private static boolean existingQuad(
        Collection<MultiColumnTestConfigs> existing,
        DataType mainType1,
        DataType subType1,
        DataType mainType2,
        DataType subType2
    ) {
        String key = quadKey(mainType1, subType1, mainType2, subType2);
        return existing.stream().anyMatch(c -> c.exists(key));
    }

    private static String quadKey(DataType mainType1, DataType subType1, DataType mainType2, DataType subType2) {
        return pairKey(mainType1, subType1) + "__" + pairKey(mainType2, subType2);
    }

    /**
     * The mismatch error the analyzer reports for one IN column. The analyzer reports the widened types
     * (BYTE -> INTEGER, FLOAT -> DOUBLE, ...) in the failure message.
     */
    private static String mismatchErrorFor(DataType mainType, String subFieldName, DataType subType) {
        return String.format(
            Locale.ROOT,
            "left field [%s] of type [%s] is incompatible with right field [%s] of type [%s]",
            MAIN_FIELD_PREFIX + mainType.esType(),
            mainType.widenSmallNumeric(),
            subFieldName,
            subType.widenSmallNumeric()
        );
    }

    private static String unsupportedErrorFor(String subFieldName, DataType subType) {
        return String.format(Locale.ROOT, "IN subquery with right field [%s] of type [%s] is not supported", subFieldName, subType);
    }

    /**
     * The name under which column 2's sub-index field surfaces in error messages: when both columns read the same sub field,
     * {@link MultiColumnTestConfig#subquerySource()} aliases the second occurrence via EVAL.
     */
    private static String subFieldName2For(DataType subType1, DataType subType2) {
        String subFieldName2 = SUB_FIELD_PREFIX + subType2.esType();
        return subType1.esType().equals(subType2.esType()) ? subFieldName2 + "_2" : subFieldName2;
    }

    /**
     * Mirrors {@code SemiJoin#semiJoinCompatible}, using the {@code widenSmallNumeric()} types that the analyzer materializes for
     * mapped fields. {@code BYTE} / {@code SHORT} fields end up as {@code INTEGER}; {@code FLOAT} / {@code HALF_FLOAT} /
     * {@code SCALED_FLOAT} fields end up as {@code DOUBLE}; everything else stays as itself.
     */
    private static boolean semiJoinCompatible(DataType mainType, DataType subType) {
        DataType mainEffective = mainType.widenSmallNumeric();
        DataType subEffective = subType.widenSmallNumeric();
        if (mainEffective == subEffective) {
            return true;
        }
        return DataType.isString(mainEffective) && DataType.isString(subEffective);
    }

    public void testInSubqueryStrings() {
        testInSubqueryTypes("strings");
    }

    public void testInSubqueryIntegers() {
        testInSubqueryTypes("integers");
    }

    public void testInSubqueryFloats() {
        testInSubqueryTypes("floats");
    }

    public void testInSubqueryMixedNumerical() {
        testInSubqueryTypes("mixed-numerical");
    }

    public void testInSubqueryMixedTemporal() {
        testInSubqueryTypes("mixed-temporal");
    }

    public void testInSubqueryUnsupported() {
        testInSubqueryTypes("unsupported");
    }

    public void testInSubquerySame() {
        testInSubqueryTypes("same");
    }

    public void testInSubqueryOthers() {
        testInSubqueryTypes("others");
    }

    // ---- multi-column IN subquery type tests ----
    //
    // Both column types are independently parameterized (see multiColumnTestConfigurations).
    // The groups mirror the single-column testConfigurations: within-family crosses plus anchored sweeps of every remaining pair,
    // covering passes, per-column failures, both-column failures, and unsupported types.

    public void testMultiColumnInSubqueryStrings() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-strings");
    }

    public void testMultiColumnInSubqueryIntegers() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-integers");
    }

    public void testMultiColumnInSubqueryFloats() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-floats");
    }

    public void testMultiColumnInSubqueryMixedNumerical() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-mixed-numerical");
    }

    public void testMultiColumnInSubqueryMixedTemporal() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-mixed-temporal");
    }

    public void testMultiColumnInSubqueryUnsupported() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-unsupported");
    }

    public void testMultiColumnInSubquerySame() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-same");
    }

    public void testMultiColumnInSubqueryOthers() {
        assumeTrue("Requires multi-column IN subquery support", Build.current().isSnapshot());
        testMultiColumnInSubqueryTypes("mc-others");
    }

    private void testMultiColumnInSubqueryTypes(String group) {
        MultiColumnTestConfigs configs = multiColumnTestConfigurations.get(group);
        assumeTrue("No configurations available for group: " + group, configs != null);
        for (MultiColumnTestConfig config : configs.values()) {
            config.doTest();
        }
    }

    private void testInSubqueryTypes(String group) {
        TestConfigs configs = testConfigurations.get(group);
        for (TestConfig config : configs.values()) {
            if (testableOnCurrentBuild(config) == false) {
                continue;
            }
            config.doTest();
        }
    }

    /**
     * Under-construction data types ({@code DATE_RANGE}, {@code FLATTENED} as of writing) are only surfaced with their real
     * {@link DataType} on snapshot builds; on release builds {@link DataType#fromEs} maps them to {@link DataType#UNSUPPORTED}, so the
     * analyzer rejects the query with a generic {@code "Cannot use field [...] with unsupported type [...]"} error before the IN subquery
     * verifier ever runs — which doesn't match the type-specific error these configs assert. Skip any pair that involves an
     * under-construction type when not on a snapshot build so the suite still exercises every other combination on release builds. This
     * mirrors {@code LookupJoinTypesIT}, which filters out {@link DataType#UNDER_CONSTRUCTION} types the same way.
     */
    private static boolean testableOnCurrentBuild(TestConfig config) {
        if (Build.current().isSnapshot()) {
            return true;
        }
        return DataType.UNDER_CONSTRUCTION.contains(config.mainType()) == false
            && DataType.UNDER_CONSTRUCTION.contains(config.subType()) == false;
    }

    private void createIndex(String indexName, String fieldPrefix) {
        List<String> properties = new ArrayList<>(ALL_TEST_TYPES.size());
        for (DataType type : ALL_TEST_TYPES) {
            properties.add(propertySpecFor(fieldPrefix + type.esType(), type));
        }
        String mapping = "{\n  \"properties\" : {\n" + String.join(", ", properties) + "  }\n}\n";
        CreateIndexRequestBuilder builder = prepareCreate(indexName).setMapping(mapping)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        assertAcked(builder);
    }

    private void indexSampleDocument(String indexName, String fieldPrefix) {
        List<String> properties = ALL_TEST_TYPES.stream().map(t -> propertyFor(fieldPrefix + t.esType(), t)).collect(Collectors.toList());
        String source = String.format(Locale.ROOT, "{\n  %s\n}\n", String.join(",\n  ", properties));
        client().prepareIndex().setIndex(indexName).setId("1").setSource(source, XContentType.JSON).get();
        // Refresh so the doc is searchable by all subsequent tests.
        client().admin().indices().prepareRefresh(indexName).get();
        for (DataType type : ALL_TEST_TYPES) {
            if (SKIP_VALIDATION_AFTER_POPULATING_INDICES.contains(type)) {
                continue;
            }
            validateIndex(indexName, fieldPrefix + type.esType(), sampleDataFor(type));
        }
    }

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
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    private static String sampleDataTextFor(DataType type) {
        Object value = sampleDataFor(type);
        return value instanceof String ? "\"" + value + "\"" : String.valueOf(value);
    }

    /**
     * Returns the JSON snippet used as the value of a sampled field in the indexed document. Scalar types delegate to
     * {@link #sampleDataTextFor}; compound types ({@code DATE_RANGE}, {@code FLATTENED}, {@code DENSE_VECTOR},
     * {@code AGGREGATE_METRIC_DOUBLE}, {@code HISTOGRAM}) have their own object / array literal.
     */
    private static String sampleValueJsonFor(DataType type) {
        return switch (type) {
            case DATE_RANGE -> "{ \"gte\": \"2025-04-02T12:00:00.000Z\", \"lte\": \"2025-04-02T13:00:00.000Z\" }";
            case DOUBLE_RANGE -> "{ \"gte\": 1.0, \"lte\": 2.0 }";
            case FLATTENED -> "{ \"k\": \"v\" }";
            case DENSE_VECTOR -> "[0.2672612, 0.5345224, 0.8017837]";
            case AGGREGATE_METRIC_DOUBLE -> "{ \"min\": 1, \"max\": 2, \"sum\": 3, \"value_count\": 1 }";
            case HISTOGRAM -> "{ \"values\": [0.1, 0.2], \"counts\": [1, 2] }";
            case EXPONENTIAL_HISTOGRAM -> "{ \"scale\": 0, \"positive\": { \"indices\": [0, 1], \"counts\": [1, 2] } }";
            case TDIGEST -> "{ \"centroids\": [1.0, 2.0], \"counts\": [1, 2] }";
            default -> sampleDataTextFor(type);
        };
    }

    private static String propertyFor(String fieldName, DataType type) {
        return String.format(Locale.ROOT, "\"%s\": %s", fieldName, sampleValueJsonFor(type));
    }

    private static String propertySpecFor(String fieldName, DataType type) {
        return switch (type) {
            case SCALED_FLOAT -> String.format(
                Locale.ROOT,
                "\"%s\": { \"type\" : \"scaled_float\", \"scaling_factor\": %f }",
                fieldName,
                SCALING_FACTOR
            );
            case DENSE_VECTOR -> String.format(Locale.ROOT, "\"%s\": { \"type\" : \"dense_vector\", \"dims\" : 3 }", fieldName);
            case AGGREGATE_METRIC_DOUBLE -> String.format(
                Locale.ROOT,
                "\"%s\": { \"type\" : \"aggregate_metric_double\", \"metrics\": [\"min\", \"max\", \"sum\", \"value_count\"],"
                    + " \"default_metric\": \"max\" }",
                fieldName
            );
            default -> String.format(Locale.ROOT, "\"%s\": { \"type\" : \"%s\" }", fieldName, type.esType().replaceAll("cartesian_", ""));
        };
    }

    private static void validateIndex(String indexName, String fieldName, Object expectedValue) {
        String query = String.format(Locale.ROOT, "FROM %s | KEEP %s", indexName, fieldName);
        try (var response = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
            ColumnInfo info = response.response().columns().getFirst();
            assertThat("Expected index '" + indexName + "' to have column '" + fieldName + "': " + query, info.name(), is(fieldName));
            Iterator<Object> results = response.response().column(0).iterator();
            assertTrue("Expected at least one result for query: " + query, results.hasNext());
            Object indexedResult = results.next();
            assertThat("Expected valid result: " + query, indexedResult, is(expectedValue));
        }
    }

    private static class TestConfigs {
        final String group;
        final Map<String, TestConfig> configs;

        TestConfigs(String group) {
            this.group = group;
            this.configs = new LinkedHashMap<>();
        }

        private Collection<TestConfig> values() {
            return configs.values();
        }

        private boolean exists(String pairKey) {
            return configs.containsKey(pairKey);
        }

        private void add(TestConfig config) {
            if (configs.containsKey(config.pairKey())) {
                throw new IllegalArgumentException("Duplicate test pair: " + config.pairKey());
            }
            configs.put(config.pairKey(), config);
        }

        private void addPasses(DataType mainType, DataType subType) {
            add(new TestConfigPasses(mainType, subType));
        }

        private void addFailsMismatch(DataType mainType, DataType subType) {
            String errorMessage = mismatchErrorFor(mainType, SUB_FIELD_PREFIX + subType.esType(), subType);
            add(
                new TestConfigFails<>(
                    mainType,
                    subType,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMessage))
                )
            );
        }

        private void addFailsUnsupported(DataType mainType, DataType subType) {
            String errorMessage = unsupportedErrorFor(SUB_FIELD_PREFIX + subType.esType(), subType);
            add(
                new TestConfigFails<>(
                    mainType,
                    subType,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMessage))
                )
            );
        }
    }

    interface TestConfig {
        DataType mainType();

        DataType subType();

        default String pairKey() {
            return InSubqueryTypesIT.pairKey(mainType(), subType());
        }

        default String mainFieldName() {
            return MAIN_FIELD_PREFIX + mainType().esType();
        }

        default String subFieldName() {
            return SUB_FIELD_PREFIX + subType().esType();
        }

        /** Index that the left side of the IN comparison reads from. Counter types live in the TSDB index. */
        default String mainIndex() {
            return isCounter(mainType()) ? TSDB_INDEX : MAIN_INDEX;
        }

        /** Index that the right-side subquery reads from. Counter types live in the TSDB index. */
        default String subIndex() {
            return isCounter(subType()) ? TSDB_INDEX : SUB_INDEX;
        }

        /**
         * Field projected by the outer {@code KEEP}. Failure cases never get past the analyzer so this only matters when it would be
         * rejected on its own — for counter mainTypes the safe choice is the {@code @timestamp} field that always exists in the TSDB
         * index. For everything else we keep the left field, which doubles as the result column for {@link TestConfigPasses}.
         */
        default String outputField() {
            return isCounter(mainType()) ? "@timestamp" : mainFieldName();
        }

        default String testQuery() {
            return String.format(
                Locale.ROOT,
                "FROM %s | WHERE %s IN (FROM %s | KEEP %s) | KEEP %s",
                mainIndex(),
                mainFieldName(),
                subIndex(),
                subFieldName(),
                outputField()
            );
        }

        void doTest();
    }

    /**
     * Test case for a pair of types that should successfully match in {@code WHERE field IN (subquery)}.
     */
    private record TestConfigPasses(DataType mainType, DataType subType) implements TestConfig {
        @Override
        public void doTest() {
            String query = testQuery();
            try (var response = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
                Iterator<Object> results = response.response().column(0).iterator();
                assertTrue("Expected at least one result for query: " + query, results.hasNext());
                Object indexedResult = results.next();
                assertThat("Expected valid result: " + query, indexedResult, equalTo(sampleDataFor(mainType)));
            }
        }

    }

    /**
     * Test case for a pair of types that should generate an error message in {@code WHERE field IN (subquery)}.
     */
    private record TestConfigFails<E extends Exception>(DataType mainType, DataType subType, Class<E> exception, Consumer<E> assertion)
        implements
            TestConfig {
        @Override
        public void doTest() {
            String query = testQuery();
            E e = expectThrows(
                exception(),
                "Expected exception " + exception().getSimpleName() + " but no exception was thrown: " + query,
                () -> {
                    // noinspection EmptyTryBlock
                    try (var ignored = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
                        // We use try-with-resources to ensure the request is closed if the exception is not thrown.
                    }
                }
            );
            assertion().accept(e);
        }
    }

    // ---- multi-column IN subquery: both column types independently configurable ----
    //
    // Each MultiColumnTestConfig specifies (mainType1, subType1) for the first IN column and
    // (mainType2, subType2) for the second, so type compatibility is exercised independently per
    // column. Counter types are excluded because tsdb_index has no plain-typed column to pair with.

    interface MultiColumnTestConfig {
        DataType mainType1();

        DataType subType1();

        DataType mainType2();

        DataType subType2();

        default String quadKey() {
            return InSubqueryTypesIT.quadKey(mainType1(), subType1(), mainType2(), subType2());
        }

        default String mainFieldName1() {
            return MAIN_FIELD_PREFIX + mainType1().esType();
        }

        default String subFieldName1() {
            return SUB_FIELD_PREFIX + subType1().esType();
        }

        default String mainFieldName2() {
            return MAIN_FIELD_PREFIX + mainType2().esType();
        }

        default String subFieldName2() {
            return SUB_FIELD_PREFIX + subType2().esType();
        }

        /**
         * The subquery source fragment. When both columns reference the same sub-index field, KEEP would deduplicate
         * the repeated name down to a single column and the analyzer would reject the 2-field IN; alias the second
         * occurrence via EVAL so the subquery always returns two columns.
         */
        default String subquerySource() {
            if (subFieldName1().equals(subFieldName2()) == false) {
                return String.format(Locale.ROOT, "FROM %s | KEEP %s, %s", SUB_INDEX, subFieldName1(), subFieldName2());
            }
            String alias = subFieldName2() + "_2";
            return String.format(
                Locale.ROOT,
                "FROM %s | EVAL %s = %s | KEEP %s, %s",
                SUB_INDEX,
                alias,
                subFieldName2(),
                subFieldName1(),
                alias
            );
        }

        void doTest();
    }

    /**
     * Multi-column test case where both column pairs are type-compatible: the query should return a row.
     */
    private record MultiColumnTestConfigPasses(DataType mainType1, DataType subType1, DataType mainType2, DataType subType2)
        implements
            MultiColumnTestConfig {
        @Override
        public void doTest() {
            String query = String.format(
                Locale.ROOT,
                "FROM %s | WHERE (%s, %s) IN (%s) | KEEP %s",
                MAIN_INDEX,
                mainFieldName1(),
                mainFieldName2(),
                subquerySource(),
                mainFieldName1()
            );
            try (var response = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
                Iterator<Object> results = response.response().column(0).iterator();
                assertTrue("Expected at least one result for multi-column query: " + query, results.hasNext());
                Object indexedResult = results.next();
                assertThat("Expected valid result: " + query, indexedResult, equalTo(sampleDataFor(mainType1)));
            }
        }
    }

    /**
     * Multi-column test case where at least one column pair has a type incompatibility or unsupported type:
     * the query should fail with the asserted error.
     */
    private record MultiColumnTestConfigFails<E extends Exception>(
        DataType mainType1,
        DataType subType1,
        DataType mainType2,
        DataType subType2,
        Class<E> exception,
        Consumer<E> assertion
    ) implements MultiColumnTestConfig {
        @Override
        public void doTest() {
            // Keep main_keyword as a safe output field; the query fails at analysis time before KEEP runs.
            String query = String.format(
                Locale.ROOT,
                "FROM %s | WHERE (%s, %s) IN (%s) | KEEP %s",
                MAIN_INDEX,
                mainFieldName1(),
                mainFieldName2(),
                subquerySource(),
                MAIN_FIELD_PREFIX + KEYWORD.esType()
            );
            E e = expectThrows(exception(), "Expected " + exception().getSimpleName() + " for multi-column query: " + query, () -> {
                // noinspection EmptyTryBlock
                try (var ignored = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {}
            });
            assertion().accept(e);
        }
    }

    private static class MultiColumnTestConfigs {
        final String group;
        final Map<String, MultiColumnTestConfig> configs = new LinkedHashMap<>();

        MultiColumnTestConfigs(String group) {
            this.group = group;
        }

        Collection<MultiColumnTestConfig> values() {
            return configs.values();
        }

        private boolean exists(String quadKey) {
            return configs.containsKey(quadKey);
        }

        private void add(MultiColumnTestConfig config) {
            if (configs.containsKey(config.quadKey())) {
                throw new IllegalArgumentException("Duplicate multi-column test quad: " + config.quadKey());
            }
            configs.put(config.quadKey(), config);
        }

        void addPasses(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            add(new MultiColumnTestConfigPasses(mt1, st1, mt2, st2));
        }

        /** Column 1 has a type mismatch; column 2 is compatible. */
        void addFailsMismatchCol1(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            String errorMsg = mismatchErrorFor(mt1, SUB_FIELD_PREFIX + st1.esType(), st1);
            add(
                new MultiColumnTestConfigFails<>(
                    mt1,
                    st1,
                    mt2,
                    st2,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMsg))
                )
            );
        }

        /** Column 1 is compatible; column 2 has a type mismatch. */
        void addFailsMismatchCol2(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            String errorMsg = mismatchErrorFor(mt2, subFieldName2For(st1, st2), st2);
            add(
                new MultiColumnTestConfigFails<>(
                    mt1,
                    st1,
                    mt2,
                    st2,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMsg))
                )
            );
        }

        /** Both columns have type mismatches; the error message must contain at least one failure. */
        void addFailsMismatchBoth(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            String errorCol1 = mismatchErrorFor(mt1, SUB_FIELD_PREFIX + st1.esType(), st1);
            String errorCol2 = mismatchErrorFor(mt2, subFieldName2For(st1, st2), st2);
            add(
                new MultiColumnTestConfigFails<>(
                    mt1,
                    st1,
                    mt2,
                    st2,
                    VerificationException.class,
                    e -> assertTrue(
                        "Expected error for col1 or col2 in: " + e.getMessage(),
                        e.getMessage().contains(errorCol1) || e.getMessage().contains(errorCol2)
                    )
                )
            );
        }

        /** Column 1 is an unsupported type for IN subquery; column 2 is compatible. */
        void addFailsUnsupportedCol1(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            String errorMsg = unsupportedErrorFor(SUB_FIELD_PREFIX + st1.esType(), st1);
            add(
                new MultiColumnTestConfigFails<>(
                    mt1,
                    st1,
                    mt2,
                    st2,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMsg))
                )
            );
        }

        /** Column 1 is compatible; column 2 is an unsupported type for IN subquery. */
        void addFailsUnsupportedCol2(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            String errorMsg = unsupportedErrorFor(subFieldName2For(st1, st2), st2);
            add(
                new MultiColumnTestConfigFails<>(
                    mt1,
                    st1,
                    mt2,
                    st2,
                    VerificationException.class,
                    e -> assertThat(e.getMessage(), containsString(errorMsg))
                )
            );
        }

        /** Both columns are unsupported types for IN subquery; the error message must contain at least one failure. */
        void addFailsUnsupportedBoth(DataType mt1, DataType st1, DataType mt2, DataType st2) {
            String errorCol1 = unsupportedErrorFor(SUB_FIELD_PREFIX + st1.esType(), st1);
            String errorCol2 = unsupportedErrorFor(subFieldName2For(st1, st2), st2);
            add(
                new MultiColumnTestConfigFails<>(
                    mt1,
                    st1,
                    mt2,
                    st2,
                    VerificationException.class,
                    e -> assertTrue(
                        "Expected unsupported error for col1 or col2 in: " + e.getMessage(),
                        e.getMessage().contains(errorCol1) || e.getMessage().contains(errorCol2)
                    )
                )
            );
        }
    }
}
