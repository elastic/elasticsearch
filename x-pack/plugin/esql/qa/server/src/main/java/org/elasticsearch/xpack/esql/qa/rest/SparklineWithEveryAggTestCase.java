/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Runs SPARKLINE with every registered aggregation function without asserting
 * on the output. We <strong>just</strong> assert that the agg runs or the query
 * fails with a reasonable error message.
 * <p>
 *     Defaults to invoking each agg as {@code THE_AGG(val_long)}. Use
 *     {@code HAND_CRAFTED_INVOCATIONS} to build the agg in another way.
 * </p>
 * <p>
 *     Each agg runs six times:
 * </p>
 * <ul>
 *     <li>{@link Mode#INSIDE}: {@code SPARKLINE(THE_AGG(input), ...)}</li>
 *     <li>{@link Mode#ALONGSIDE}: {@code SPARKLINE(SUM(1), ...), THE_AGG(input)}</li>
 *     <li>{@link Mode#INSIDE_AND_ALONGSIDE}: {@code SPARKLINE(THE_AGG(input), ...), THE_AGG(input)}</li>
 *     <li>{@link Mode#INSIDE_BY}: {@code SPARKLINE(THE_AGG(input), ...) BY group}</li>
 *     <li>{@link Mode#ALONGSIDE_BY}: {@code SPARKLINE(SUM(1), ...), THE_AGG(input) BY group}</li>
 *     <li>{@link Mode#INSIDE_AND_ALONGSIDE_BY}: {@code SPARKLINE(THE_AGG(input), ...), THE_AGG(input) BY group}</li>
 * </ul>
 */
public abstract class SparklineWithEveryAggTestCase extends ESRestTestCase {

    protected static final String INDEX_NAME = "sparkline_test";
    private static final String FROM_DATE = "2020-01-01";
    private static final String TO_DATE = "2020-12-31";
    private static final int BUCKETS = 12;

    /**
     * A single (aggregation, mode) combination under test.
     *
     * @param error null if the query is expected to succeed; otherwise a substring of the expected error message.
     */
    public record AggTestCase(String invocation, Mode mode, String error, List<String> capabilitiesRequired) {
        @Override
        public String toString() {
            return mode.buildQuery(invocation, "");
        }
    }

    /**
     * Aggs that return multiple values per group and so cannot be used as the first argument of SPARKLINE.
     * These must produce an error when used INSIDE sparkline.
     */
    private static final Set<String> MULTI_VALUED_AGGS = Set.of("top", "sample", "values");

    /**
     * Aggs whose invocations cannot be auto-generated from {@code NAME(val_long)}:
     * multi-arg, star syntax, or needing literal constants.
     */
    private static final Map<String, String> HAND_CRAFTED_INVOCATIONS = Map.ofEntries(
        Map.entry("count_distinct", "COUNT_DISTINCT(val_long)"),
        Map.entry("percentile", "PERCENTILE(val_long, 50)"),
        Map.entry("weighted_avg", "WEIGHTED_AVG(val_long, val_int)"),
        Map.entry("first", "FIRST(val_long, @timestamp)"),
        Map.entry("last", "LAST(val_long, @timestamp)"),
        Map.entry("top", "TOP(val_long, 3, \"asc\")"),
        Map.entry("sample", "SAMPLE(val_long, 3)"),
        Map.entry("st_centroid_agg", "ST_CENTROID_AGG(val_geo_point)"),
        Map.entry("st_extent_agg", "ST_EXTENT_AGG(val_geo_point)")
    );

    @ParametersFactory(argumentFormatting = "%s")
    public static List<Object[]> parameters() {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry().snapshotRegistry();
        List<AggTestCase> params = new ArrayList<>();

        for (FunctionDefinition def : registry.listFunctions()) {
            FunctionInfo info = requireNonNull(EsqlFunctionRegistry.functionInfo(def), "registered function doesn't have info");
            if (info.type() != FunctionType.AGGREGATE) {
                continue;
            }
            String name = def.name();
            if (name.equals("sparkline")) {
                continue;
            }
            String invocation = HAND_CRAFTED_INVOCATIONS.getOrDefault(name, name.toUpperCase(Locale.ROOT) + "(val_long)");
            addCases(params, info, name, invocation);
        }
        FunctionInfo countInfo = requireNonNull(EsqlFunctionRegistry.functionInfo(registry.resolveFunction("count")));
        for (String extra : List.of("COUNT(*)", "COUNT(val_int)", "COUNT(val_double)", "COUNT(val_geo_point)")) {
            addCases(params, countInfo, "count", extra);
        }

        return params.stream().map(tc -> new Object[] { tc }).toList();
    }

    private static void addCases(List<AggTestCase> params, FunctionInfo info, String name, String invocation) {
        List<String> capabilitiesRequired = new ArrayList<>();
        capabilitiesRequired.add("fn_sparkline");
        capabilitiesRequired.add("fn_" + name);
        if (name.equals("avg")) {
            // https://github.com/elastic/elasticsearch/issues/150265
            capabilitiesRequired.add("fn_sparkline_complex");
        }

        // Every agg supports running ALONGSIDE and ALONGSIDE_BY
        params.add(new AggTestCase(invocation, Mode.ALONGSIDE, null, capabilitiesRequired));
        params.add(new AggTestCase(invocation, Mode.ALONGSIDE_BY, null, capabilitiesRequired));

        // Multi-valued aggs fail with a "single-valued" error when used INSIDE sparkline
        if (MULTI_VALUED_AGGS.contains(name)) {
            List<String> capabilitiesWithRejectMv = new ArrayList<>(capabilitiesRequired);
            capabilitiesWithRejectMv.add("fn_sparkline_reject_mv");
            String mvError = "single-valued aggregate function";
            params.add(new AggTestCase(invocation, Mode.INSIDE, mvError, capabilitiesWithRejectMv));
            params.add(new AggTestCase(invocation, Mode.INSIDE_BY, mvError, capabilitiesWithRejectMv));
            params.add(new AggTestCase(invocation, Mode.INSIDE_AND_ALONGSIDE, mvError, capabilitiesWithRejectMv));
            params.add(new AggTestCase(invocation, Mode.INSIDE_AND_ALONGSIDE_BY, mvError, capabilitiesWithRejectMv));
            return;
        }

        // Non-numeric aggs will fail when INSIDE, INSIDE_AND_ALONGSIDE, INSIDE_BY, INSIDE_AND_ALONGSIDE_BY
        boolean numericReturn = Arrays.stream(info.returnType()).anyMatch(List.of("integer", "long", "double")::contains);
        String typeError = numericReturn ? null : "must be [integer or long or double]";
        params.add(new AggTestCase(invocation, Mode.INSIDE, typeError, capabilitiesRequired));
        params.add(new AggTestCase(invocation, Mode.INSIDE_BY, typeError, capabilitiesRequired));
        if (name.equals("weighted_avg") || name.equals("avg")) {
            // https://github.com/elastic/elasticsearch/issues/150224
            return;
        }
        params.add(new AggTestCase(invocation, Mode.INSIDE_AND_ALONGSIDE, typeError, capabilitiesRequired));
        params.add(new AggTestCase(invocation, Mode.INSIDE_AND_ALONGSIDE_BY, typeError, capabilitiesRequired));
    }

    private final AggTestCase testCase;

    public SparklineWithEveryAggTestCase(AggTestCase testCase) {
        this.testCase = testCase;
    }

    protected List<String> requiredCapabilities() {
        return testCase.capabilitiesRequired();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * The index pattern used in {@code FROM}. Override in multi-cluster subclasses to add
     * cross-cluster references.
     */
    protected String indexPattern() {
        return INDEX_NAME;
    }

    /**
     * Create the test index on the given client if it does not already exist.
     * Override in multi-cluster subclasses to also seed the remote cluster.
     */
    protected void createIndexAndData(RestClient restClient) throws IOException {
        if (indexExists(restClient, INDEX_NAME)) {
            return;
        }
        createIndex(restClient, INDEX_NAME, Settings.EMPTY, """
            {
              "properties": {
                "@timestamp":    { "type": "date" },
                "val_long":      { "type": "long" },
                "val_int":       { "type": "integer" },
                "val_double":    { "type": "double" },
                "val_geo_point": { "type": "geo_point" },
                "group":         { "type": "keyword" }
              }
            }
            """);
        Request bulk = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity("""
            {"index":{}}
            {"@timestamp":"2020-02-01T00:00:00Z","val_long":10,"val_int":1,"val_double":1.5,"val_geo_point":"40.0,-70.0","group":"a"}
            {"index":{}}
            {"@timestamp":"2020-04-01T00:00:00Z","val_long":20,"val_int":2,"val_double":2.5,"val_geo_point":"41.0,-71.0","group":"b"}
            {"index":{}}
            {"@timestamp":"2020-06-01T00:00:00Z","val_long":30,"val_int":3,"val_double":3.5,"val_geo_point":"42.0,-72.0","group":"a"}
            {"index":{}}
            {"@timestamp":"2020-08-01T00:00:00Z","val_long":40,"val_int":4,"val_double":4.5,"val_geo_point":"43.0,-73.0","group":"b"}
            {"index":{}}
            {"@timestamp":"2020-10-01T00:00:00Z","val_long":50,"val_int":5,"val_double":5.5,"val_geo_point":"44.0,-74.0","group":"a"}
            """);
        Response response = restClient.performRequest(bulk);
        Map<String, Object> result = entityToMap(response.getEntity(), XContentType.JSON);
        assertMap(result, matchesMap().extraOk().entry("errors", false));
    }

    @Before
    public final void ensureIndex() throws IOException {
        doEnsureIndex();
    }

    /**
     * Called by {@link #ensureIndex()} to set up the test index before each test.
     * Override in multi-cluster subclasses to also seed remote clusters.
     */
    protected void doEnsureIndex() throws IOException {
        createIndexAndData(client());
    }

    private static String sparklineArgs() {
        return String.format(Locale.ROOT, ", @timestamp, %d, \"%s\", \"%s\"", BUCKETS, FROM_DATE, TO_DATE);
    }

    public void testSparkline() throws IOException {
        String invocation = testCase.invocation();
        String query = "FROM " + indexPattern() + " | " + testCase.mode().buildQuery(invocation, sparklineArgs());

        if (testCase.error() != null) {
            ResponseException re = expectThrows(
                ResponseException.class,
                () -> runEsqlSync(new RestEsqlTestCase.RequestObjectBuilder().query(query), new AssertWarnings.NoWarnings(), null)
            );
            assertThat(re.getMessage(), containsString(testCase.error()));
            return;
        }

        Map<String, Object> result = runEsqlSync(
            new RestEsqlTestCase.RequestObjectBuilder().query(query),
            new AssertWarnings.NoWarnings(),
            null
        );

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columns = (List<Map<String, Object>>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");

        testCase.mode().assertResult(columns, values);
    }

    /**
     * Which query shape to exercise.
     */
    public enum Mode {
        /**
         * {@code STATS s = SPARKLINE(THE_AGG(input), @timestamp, 10, "2020-01-01", "2020-12-31")}
         * <p>
         * The parameterized agg is wrapped by SPARKLINE, so it must return integer, long, or double.
         */
        INSIDE {
            @Override
            public String buildQuery(String invocation, String sparklineArgs) {
                return String.format(Locale.ROOT, "STATS s = SPARKLINE(%s%s)", invocation, sparklineArgs);
            }

            @Override
            public void assertResult(List<Map<String, Object>> columns, List<List<Object>> values) {
                assertThat(values, hasSize(1));
                assertMap(columns, matchesList().item(Map.of("name", "s", "type", any(String.class))));
                assertMap(values.getFirst(), matchesList().item(instanceOf(List.class)));
            }
        },
        /**
         * {@code STATS s = SPARKLINE(SUM(1), @timestamp, 10, "2020-01-01", "2020-12-31"), a = THE_AGG(input)}
         * <p>
         * SPARKLINE wraps a fixed {@code SUM(1)}; the parameterized agg runs bare alongside it.
         * Any registered aggregation can appear here, regardless of return type.
         */
        ALONGSIDE {
            @Override
            public String buildQuery(String invocation, String sparklineArgs) {
                return String.format(Locale.ROOT, "STATS s = SPARKLINE(SUM(1)%s), a = %s", sparklineArgs, invocation);
            }

            @Override
            public void assertResult(List<Map<String, Object>> columns, List<List<Object>> values) {
                assertThat(values, hasSize(1));
                assertMap(
                    columns,
                    matchesList().item(Map.of("name", "s", "type", "long")).item(Map.of("name", "a", "type", any(String.class)))
                );
                assertMap(values.getFirst(), matchesList().item(instanceOf(List.class)).item(anything()));
            }
        },
        /**
         * {@code STATS s = SPARKLINE(THE_AGG(input), @timestamp, 10, "2020-01-01", "2020-12-31"), a = THE_AGG(input)}
         * <p>
         * The parameterized agg is both wrapped by SPARKLINE and runs bare in the same STATS.
         * The agg must return integer, long, or double.
         */
        INSIDE_AND_ALONGSIDE {
            @Override
            public String buildQuery(String invocation, String sparklineArgs) {
                return String.format(Locale.ROOT, "STATS s = SPARKLINE(%s%s), a = %s", invocation, sparklineArgs, invocation);
            }

            @Override
            public void assertResult(List<Map<String, Object>> columns, List<List<Object>> values) {
                assertThat(values, hasSize(1));
                assertMap(
                    columns,
                    matchesList().item(Map.of("name", "s", "type", any(String.class))).item(Map.of("name", "a", "type", any(String.class)))
                );
                assertMap(values.getFirst(), matchesList().item(instanceOf(List.class)).item(anything()));
            }
        },
        /**
         * {@code STATS s = SPARKLINE(THE_AGG(input), @timestamp, 10, "2020-01-01", "2020-12-31") BY group}
         * <p>
         * Like {@link #INSIDE} but grouped by {@code group}; the result has one row per group value.
         * The parameterized agg must return integer, long, or double.
         */
        INSIDE_BY {
            @Override
            public String buildQuery(String invocation, String sparklineArgs) {
                return String.format(Locale.ROOT, "STATS s = SPARKLINE(%s%s) BY group", invocation, sparklineArgs);
            }

            @Override
            public void assertResult(List<Map<String, Object>> columns, List<List<Object>> values) {
                assertThat(values, hasSize(2));
                assertMap(
                    columns,
                    matchesList().item(Map.of("name", "s", "type", any(String.class))).item(Map.of("name", "group", "type", "keyword"))
                );
                for (List<Object> row : values) {
                    assertMap(row, matchesList().item(instanceOf(List.class)).item(instanceOf(String.class)));
                }
            }
        },
        /**
         * {@code STATS s = SPARKLINE(SUM(1), @timestamp, 10, "2020-01-01", "2020-12-31"), a = THE_AGG(input) BY group}
         * <p>
         * Like {@link #ALONGSIDE} but grouped by {@code group}; the result has one row per group value.
         * Any registered aggregation can appear here, regardless of return type.
         */
        ALONGSIDE_BY {
            @Override
            public String buildQuery(String invocation, String sparklineArgs) {
                return String.format(Locale.ROOT, "STATS s = SPARKLINE(SUM(1)%s), a = %s BY group", sparklineArgs, invocation);
            }

            @Override
            public void assertResult(List<Map<String, Object>> columns, List<List<Object>> values) {
                assertThat(values, hasSize(2));
                assertMap(
                    columns,
                    matchesList().item(Map.of("name", "s", "type", "long"))
                        .item(Map.of("name", "a", "type", any(String.class)))
                        .item(Map.of("name", "group", "type", "keyword"))
                );
                for (List<Object> row : values) {
                    assertMap(row, matchesList().item(instanceOf(List.class)).item(anything()).item(instanceOf(String.class)));
                }
            }
        },
        /**
         * {@code STATS s = SPARKLINE(THE_AGG(input), @timestamp, 10, "2020-01-01", "2020-12-31"), a = THE_AGG(input) BY group}
         * <p>
         * Like {@link #INSIDE_AND_ALONGSIDE} but grouped by {@code group}; the result has one row per group value.
         * The parameterized agg must return integer, long, or double.
         */
        INSIDE_AND_ALONGSIDE_BY {
            @Override
            public String buildQuery(String invocation, String sparklineArgs) {
                return String.format(Locale.ROOT, "STATS s = SPARKLINE(%s%s), a = %s BY group", invocation, sparklineArgs, invocation);
            }

            @Override
            public void assertResult(List<Map<String, Object>> columns, List<List<Object>> values) {
                assertThat(values, hasSize(2));
                assertMap(
                    columns,
                    matchesList().item(Map.of("name", "s", "type", any(String.class)))
                        .item(Map.of("name", "a", "type", any(String.class)))
                        .item(Map.of("name", "group", "type", "keyword"))
                );
                for (List<Object> row : values) {
                    assertMap(row, matchesList().item(instanceOf(List.class)).item(anything()).item(instanceOf(String.class)));
                }
            }
        };

        /**
         * Builds the {@code STATS} clause (without the leading {@code FROM index | }) for this mode.
         *
         * @param invocation   the aggregation call expression, e.g. {@code SUM(val_long)}
         * @param sparklineArgs the trailing SPARKLINE arguments: {@code @timestamp, buckets, from, to}
         */
        public abstract String buildQuery(String invocation, String sparklineArgs);

        /**
         * Asserts the shape of the result columns and the values returned by the query.
         *
         * @param columns the {@code columns} list from the ES|QL response
         * @param values the {@code values} list from the ES|QL response
         */
        public abstract void assertResult(List<Map<String, Object>> columns, List<List<Object>> values);
    }
}
