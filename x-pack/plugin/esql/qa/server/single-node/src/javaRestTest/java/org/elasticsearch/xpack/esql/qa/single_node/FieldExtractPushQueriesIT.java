/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.commonProfile;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.fixTypesOnProfile;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * End-to-end tests that {@code field_extract(<flattened root>, "<key>") <op> <literal>} does not
 * just compile to a {@code TermQuery}/{@code TermsQuery} object in the unit tests, but actually
 * shows up as a query against the {@code <root>._keyed} sub-field in the
 * {@code LuceneSourceOperator} {@code processed_queries} profile field on the data node. The tests
 * also assert that the data-node compute pipeline drops the {@code FilterOperator}, which is the
 * signal that the predicate is being filtered by Lucene rather than re-checked per row.
 * <p>
 *     Parameterized over {@link Mode} so every assertion runs against {@code standard},
 *     {@code time_series} and {@code logsdb} indices. The Lucene query shape and the data-driver
 *     operator chain are mode-independent (the keyed sub-field {@code TermQuery} doesn't care
 *     about the underlying doc-values format), so the existing assertions transfer verbatim and
 *     the parameterization mainly guards against silent regressions in the per-mode index
 *     wiring (TSDB dimensions, logsdb {@code @timestamp} requirement, etc.).
 * </p>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FieldExtractPushQueriesIT extends ESRestTestCase {

    /**
     * Index modes the parameterized suite runs against. {@link #STANDARD} is the baseline,
     * {@link #TIME_SERIES} requires a routing dimension and a bounded time window, {@link #LOGSDB}
     * requires only an {@code @timestamp} field. Other modes ({@code lookup}, {@code columnar},
     * {@code vectordb_document}) either don't host regular query targets or don't apply to
     * {@code flattened} fields, so they are not covered here.
     */
    public enum Mode {
        STANDARD,
        TIME_SERIES,
        LOGSDB
    }

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> args() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    private final Mode mode;

    public FieldExtractPushQueriesIT(Mode mode) {
        this.mode = mode;
    }

    private static final String FLATTENED_ROOT = "attrs";
    private static final String SUBKEY = "host.name";
    /**
     * The {@code _keyed} sub-field of a {@code flattened} root holds the {@code <key>\0<value>}
     * terms (see {@code KeyedFlattenedFieldType} in {@code FlattenedFieldMapper}). Per-key Lucene
     * queries target this {@code <root>._keyed} field directly. Lucene's {@code TermQuery.toString}
     * preserves the literal NUL byte between the key and value, so the printed term is
     * {@code attrs._keyed:host.name<NUL>v}.
     */
    private static final String KEYED_INTERNAL_FIELD = FLATTENED_ROOT + "._keyed";
    /**
     * Reserved separator between the key and the value in a flattened {@code _keyed} term, see
     * {@code FlattenedFieldParser#SEPARATOR}. The assertions below use the literal NUL char
     * because the Java REST client decodes JSON's mandatory {@code \u0000} escape back to one
     * character before the response reaches us.
     */
    private static final char KEYED_TERM_SEPARATOR = '\0';

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    /**
     * {@code field_extract(...) == "v"} must push to a {@code TermQuery} against the keyed
     * sub-field, so the data driver has no {@code FilterOperator}.
     */
    public void testEqualityPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String value = randomAlphaOfLengthBetween(1, 16);
        String otherValue = randomValueOtherThan(value, () -> randomAlphaOfLengthBetween(1, 16));
        indexDocs(List.of(value, otherValue));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE field_extract(%s, "%s") == "%s"
            | KEEP id
            """, FLATTENED_ROOT, SUBKEY, value), equalTo(expectedEqualityQuery(value)), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code field_extract(...) != "v"} must push to a negated {@code TermQuery} against the keyed
     * sub-field. Lucene renders the negation as {@code -<inner> #<filter>} (must-not + filter); see
     * {@link #expectedInequalityQuery} for why the filter clause depends on the index mode.
     */
    public void testInequalityPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String value = randomAlphaOfLengthBetween(1, 16);
        String otherValue = randomValueOtherThan(value, () -> randomAlphaOfLengthBetween(1, 16));
        indexDocs(List.of(value, otherValue));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE field_extract(%s, "%s") != "%s"
            | KEEP id
            """, FLATTENED_ROOT, SUBKEY, value), equalTo(expectedInequalityQuery(value)), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code field_extract(...) IN (a, b)} must push to a {@code TermsQuery} against the keyed
     * sub-field. We only assert the field prefix because TermsQuery's toString depends on the
     * iteration order of the underlying byte-prefixed term set.
     */
    public void testInPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String first = randomAlphaOfLengthBetween(1, 16);
        String second = randomValueOtherThan(first, () -> randomAlphaOfLengthBetween(1, 16));
        String third = randomValueOtherThanMany(s -> s.equals(first) || s.equals(second), () -> randomAlphaOfLengthBetween(1, 16));
        indexDocs(List.of(first, second, third));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE field_extract(%s, "%s") IN ("%s", "%s")
            | KEEP id
            """, FLATTENED_ROOT, SUBKEY, first, second), startsWith(expectedInQueryPrefix()), ComputeSignature.FILTER_IN_QUERY, 2);
    }

    /**
     * {@code SingleValueQuery} wraps the inner per-key term query at planning time but rewrites
     * itself away to the inner query at execution time when the field can prove its doc-value set
     * is singleton (so the per-row "single value" check would always succeed). For a flattened
     * keyed sub-field this depends on the underlying doc-values format:
     * <ul>
     *   <li>{@link Mode#STANDARD} stores keyed values in {@code SortedSetDocValues}, where
     *       singleton-ness is observable per leaf. The wrapper rewrites away and the printed
     *       Lucene query is the bare {@code TermQuery}.</li>
     *   <li>{@link Mode#TIME_SERIES} and {@link Mode#LOGSDB} store keyed values in
     *       {@code BinaryDocValues} (driven by {@code IndexSettings.useTimeSeriesDocValuesFormat()},
     *       which is on for any columnar index mode). The wrapper can't cheaply prove
     *       singleton-ness here, so it stays and the printed query is
     *       {@code #<inner> #single_value_match(<keyed>)}.</li>
     * </ul>
     * Either form is correct: pushdown still happens, the data driver still drops the
     * {@code FilterOperator}, and per-row results are identical. The shape of the printed query
     * just differs because of where Lucene gets to short-circuit.
     */
    private String expectedEqualityQuery(String value) {
        String inner = KEYED_INTERNAL_FIELD + ":" + SUBKEY + KEYED_TERM_SEPARATOR + value;
        return switch (mode) {
            case STANDARD -> inner;
            case TIME_SERIES, LOGSDB -> "#" + inner + " #single_value_match(" + KEYED_INTERNAL_FIELD + ")";
        };
    }

    /**
     * Inequality already produces a {@code NotQuery} (see {@code EsqlBinaryComparison.NotEquals}),
     * which Lucene renders as {@code -<term> #<filter>}. In {@link Mode#STANDARD} the filter slot
     * is the {@code SingleValueQuery}'s rewritten {@code MatchAllDocsQuery} ({@code *:*}); in TSDB
     * and logsdb the filter slot keeps the {@code single_value_match(<keyed>)} clause. Same
     * reasoning as {@link #expectedEqualityQuery}.
     */
    private String expectedInequalityQuery(String value) {
        String negated = "-" + KEYED_INTERNAL_FIELD + ":" + SUBKEY + KEYED_TERM_SEPARATOR + value;
        return switch (mode) {
            case STANDARD -> negated + " #*:*";
            case TIME_SERIES, LOGSDB -> negated + " #single_value_match(" + KEYED_INTERNAL_FIELD + ")";
        };
    }

    /**
     * Same {@link #expectedEqualityQuery} reasoning, applied to the {@code TermsQuery} the
     * {@code IN} predicate compiles to. We only assert the leading prefix because the printed
     * order of the byte-prefixed terms isn't stable.
     */
    private String expectedInQueryPrefix() {
        String prefix = KEYED_INTERNAL_FIELD + ":(" + SUBKEY + KEYED_TERM_SEPARATOR;
        return switch (mode) {
            case STANDARD -> prefix;
            case TIME_SERIES, LOGSDB -> "#" + prefix;
        };
    }

    /**
     * Range comparisons cannot push to the keyed sub-field (the underlying
     * {@code KeyedFlattenedFieldType.rangeQuery} requires both bounds), so the plan keeps a
     * {@code FilterOperator} on the data node and the per-row evaluator handles the comparison.
     * The Lucene query degenerates to match-all because no part of the predicate is pushable.
     */
    public void testGreaterThanNotPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String low = "a" + randomAlphaOfLengthBetween(1, 8);
        String high = "z" + randomAlphaOfLengthBetween(1, 8);
        indexDocs(List.of(low, high));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE field_extract(%s, "%s") > "m"
            | KEEP id
            """, FLATTENED_ROOT, SUBKEY), equalTo("*:*"), ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * Compute signatures expected on the data driver. {@link #FILTER_IN_QUERY} mirrors
     * {@code PushQueriesIT.ComputeSignature.FILTER_IN_QUERY} (no FilterOperator). The
     * {@link #FILTER_IN_COMPUTE} case has an extra {@code ValuesSourceReaderOperator} after
     * {@code LimitOperator} because the WHERE clause reads the flattened sub-field and the
     * KEEP clause reads {@code id}; with two distinct fields we end up with two reader stages.
     */
    private enum ComputeSignature {
        FILTER_IN_QUERY(
            matchesList().item("LuceneSourceOperator")
                .item("ValuesSourceReaderOperator")
                .item("ProjectOperator")
                .item("ExchangeSinkOperator")
        ),
        FILTER_IN_COMPUTE(
            matchesList().item("LuceneSourceOperator")
                .item("ValuesSourceReaderOperator")
                .item("FilterOperator")
                .item("LimitOperator")
                .item("ValuesSourceReaderOperator")
                .item("ProjectOperator")
                .item("ExchangeSinkOperator")
        );

        private final ListMatcher matcher;

        ComputeSignature(ListMatcher matcher) {
            this.matcher = matcher;
        }
    }

    private void runAndAssert(
        String esqlQuery,
        org.hamcrest.Matcher<String> luceneQueryMatcher,
        ComputeSignature dataNodeSignature,
        int expectedHitCount
    ) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(esqlQuery).profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), profileLogger, RestEsqlTestCase.Mode.SYNC);

        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertEquals(expectedHitCount, values.size());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        boolean assertedDataDriver = false;
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            if ("data".equals(p.get("description")) == false) {
                continue;
            }
            assertedDataDriver = true;
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");
            List<String> sig = new ArrayList<>();
            for (Map<String, Object> op : operators) {
                String name = (String) op.get("operator");
                name = PushQueriesIT.TO_NAME.matcher(name).replaceAll("");
                sig.add(name);
                if (name.equals("LuceneSourceOperator")) {
                    MapMatcher expectedOp = matchesMap().entry("operator", startsWith(name))
                        .entry("status", matchesMap().entry("processed_queries", matchesList().item(luceneQueryMatcher)).extraOk());
                    assertMap(op, expectedOp);
                }
            }
            assertMap(sig, dataNodeSignature.matcher);
        }
        assertTrue("expected the data driver profile in result", assertedDataDriver);
    }

    private void indexDocs(List<String> hostNameValues) throws IOException {
        // ESRestTestCase wipes user indices in @After (cleanUpCluster -> wipeCluster), so each test
        // starts from a clean slate and we can just (re)create "test" here without a prior DELETE.
        Request createIndex = new Request("PUT", "test");
        createIndex.setJsonEntity(createIndexBodyFor(mode));
        Response response = client().performRequest(createIndex);
        assertThat(
            entityToMap(response.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < hostNameValues.size(); i++) {
            body.append("{\"create\":{\"_index\":\"test\"}}\n");
            body.append(docBodyFor(mode, i, hostNameValues.get(i)));
        }
        bulk.setJsonEntity(body.toString());
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    /**
     * Index settings + mappings appropriate for {@code mode}. TSDB needs a routing dimension and a
     * bounded time window; logsdb just needs an {@code @timestamp} field. The flattened root and
     * the {@code id} keyword are present in all three so the queries in the tests above can stay
     * mode-agnostic.
     */
    private static String createIndexBodyFor(Mode mode) {
        return switch (mode) {
            case STANDARD -> String.format(Locale.ROOT, """
                {
                  "settings": { "index": { "number_of_shards": 1 } },
                  "mappings": {
                    "properties": {
                      "id": { "type": "keyword" },
                      "%s": { "type": "flattened" }
                    }
                  }
                }
                """, FLATTENED_ROOT);
            case TIME_SERIES -> String.format(Locale.ROOT, """
                {
                  "settings": {
                    "index": {
                      "mode": "time_series",
                      "routing_path": ["dim"],
                      "number_of_shards": 1,
                      "time_series": {
                        "start_time": "2024-04-14T00:00:00Z",
                        "end_time": "2024-04-16T00:00:00Z"
                      }
                    }
                  },
                  "mappings": {
                    "properties": {
                      "@timestamp": { "type": "date" },
                      "dim": { "type": "keyword", "time_series_dimension": true },
                      "id": { "type": "keyword" },
                      "%s": { "type": "flattened" }
                    }
                  }
                }
                """, FLATTENED_ROOT);
            case LOGSDB -> String.format(Locale.ROOT, """
                {
                  "settings": { "index": { "mode": "logsdb", "number_of_shards": 1 } },
                  "mappings": {
                    "properties": {
                      "@timestamp": { "type": "date" },
                      "id": { "type": "keyword" },
                      "%s": { "type": "flattened" }
                    }
                  }
                }
                """, FLATTENED_ROOT);
        };
    }

    /**
     * Document body appropriate for {@code mode}. TSDB needs a unique {@code dim} per doc to give
     * each document its own {@code _tsid} (otherwise docs sharing a tsid+timestamp deduplicate);
     * logsdb just needs the {@code @timestamp}. The flattened payload is identical in all modes.
     */
    private static String docBodyFor(Mode mode, int i, String hostNameValue) {
        return switch (mode) {
            case STANDARD -> String.format(
                Locale.ROOT,
                "{\"id\":\"doc-%d\",\"%s\":{\"%s\":\"%s\"}}\n",
                i,
                FLATTENED_ROOT,
                SUBKEY,
                hostNameValue
            );
            case TIME_SERIES -> String.format(
                Locale.ROOT,
                "{\"@timestamp\":\"2024-04-15T00:%02d:00Z\",\"dim\":\"dim-%d\",\"id\":\"doc-%d\",\"%s\":{\"%s\":\"%s\"}}\n",
                i,
                i,
                i,
                FLATTENED_ROOT,
                SUBKEY,
                hostNameValue
            );
            case LOGSDB -> String.format(
                Locale.ROOT,
                "{\"@timestamp\":\"2024-04-15T00:%02d:00Z\",\"id\":\"doc-%d\",\"%s\":{\"%s\":\"%s\"}}\n",
                i,
                i,
                FLATTENED_ROOT,
                SUBKEY,
                hostNameValue
            );
        };
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
