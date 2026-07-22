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
import org.hamcrest.Matcher;
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
 * {@code LuceneSourceOperator} {@code processed_queries} profile field on the data node.
 * <p>
 *     Pushdown uses the RECHECK contract: the candidate query is pushed to Lucene <em>and</em> the
 *     predicate is retained in a {@code FilterOperator} that re-applies it per row to restore ES|QL
 *     single-value semantics. So the tests assert both signals: the keyed candidate query appears in
 *     {@code processed_queries}, and the data-node compute pipeline keeps the {@code FilterOperator}
 *     (see {@link ComputeSignature#FILTER_IN_COMPUTE}).
 * </p>
 * <p>
 *     Parameterized over {@link Mode} so every assertion runs against {@code standard},
 *     {@code time_series} and {@code logsdb} indices. The candidate query is a bare
 *     {@code TermQuery}/{@code TermsQuery}/{@code RangeQuery} with no {@code SingleValueQuery}
 *     wrapper, so its printed shape no longer depends on the doc-values format and is identical
 *     across modes. The parameterization mainly guards against silent regressions in the per-mode
 *     index wiring (TSDB dimensions, logsdb {@code @timestamp} requirement, etc.).
 * </p>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushQueriesFieldExtractIT extends ESRestTestCase {

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

    public PushQueriesFieldExtractIT(Mode mode) {
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
    /**
     * Upper-end sentinel byte the keyed flattened mapper substitutes for an open upper bound,
     * see {@code KeyedFlattenedFieldType.rangeQuery}. Byte {@code 0x01} is the lowest byte
     * strictly greater than the {@code \0} key-value separator so it sits past every
     * {@code <key>\0<value>} encoding for the open key and strictly before the first term of
     * any sibling key. The assertions below use the literal {@code char} because the Java REST
     * client decodes JSON's mandatory {@code \u0001} escape back to one character before the
     * response reaches us.
     */
    private static final char KEYED_TERM_OPEN_UPPER_SENTINEL = (char) 0x01;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    /**
     * {@code field_extract(...) == "v"} must push a candidate {@code TermQuery} against the keyed
     * sub-field, and the data driver keeps the {@code FilterOperator} that rechecks the predicate.
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
            """, FLATTENED_ROOT, SUBKEY, value), equalTo(expectedEqualityQuery(value)), ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * {@code field_extract(...) != "v"} must push a candidate negated {@code TermQuery} against the
     * keyed sub-field, with the {@code FilterOperator} retained for the recheck. Lucene renders the
     * pure-negative bool as {@code #*:* -<inner>} (match-all filter + must-not).
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
            """, FLATTENED_ROOT, SUBKEY, value), equalTo(expectedInequalityQuery(value)), ComputeSignature.FILTER_IN_COMPUTE, 1);
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
            """, FLATTENED_ROOT, SUBKEY, first, second), startsWith(expectedInQueryPrefix()), ComputeSignature.FILTER_IN_COMPUTE, 2);
    }

    /**
     * RECHECK pushdown emits a bare candidate {@code TermQuery} against the keyed sub-field. There is
     * no {@code SingleValueQuery} wrapper (single-value semantics are restored by the retained
     * {@code FilterOperator}), so the printed query no longer has a {@code single_value_match} clause
     * and is identical across index modes.
     */
    private String expectedEqualityQuery(String value) {
        return KEYED_INTERNAL_FIELD + ":" + SUBKEY + KEYED_TERM_SEPARATOR + value;
    }

    /**
     * {@code NotEquals} produces a bare {@code NotQuery} wrapping the keyed {@code TermQuery} (see
     * {@code NotEquals#asQuery}). A pure-negative boolean needs a positive clause to match against,
     * so Lucene prepends a {@code MatchAllDocsQuery} filter and renders it as {@code #*:* -<term>}.
     * There is no {@code single_value_match} clause under RECHECK, so the shape is identical across
     * index modes.
     */
    private String expectedInequalityQuery(String value) {
        return "#*:* -" + KEYED_INTERNAL_FIELD + ":" + SUBKEY + KEYED_TERM_SEPARATOR + value;
    }

    /**
     * Same {@link #expectedEqualityQuery} reasoning, applied to the bare {@code TermsQuery} the
     * {@code IN} predicate compiles to. We only assert the leading prefix because the printed
     * order of the byte-prefixed terms isn't stable. Mode-independent under RECHECK.
     */
    private String expectedInQueryPrefix() {
        return KEYED_INTERNAL_FIELD + ":(" + SUBKEY + KEYED_TERM_SEPARATOR;
    }

    /**
     * {@code field_extract(...) > "m"} pushes to a single-sided {@code RangeQuery} against the
     * keyed sub-field. The keyed flattened mapper substitutes a {@code <key>\1} sentinel for the
     * open upper bound on the data node so the Lucene term range stays inside this key's slice
     * of the term namespace, and the data driver keeps the {@code FilterOperator} for the recheck.
     */
    public void testGreaterThanPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String low = "a" + randomAlphaOfLengthBetween(1, 8);
        String high = "z" + randomAlphaOfLengthBetween(1, 8);
        indexDocs(List.of(low, high));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE %s
            | KEEP id
            """, randomizedComparison(">", "m")), equalTo(expectedLowerOnlyRangeQuery("m", false)), ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * Same shape as {@link #testGreaterThanPushed} but with an inclusive lower bound. The
     * {@code includeLower=true} flag must reach the printed Lucene query.
     */
    public void testGreaterThanOrEqualPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String low = "a" + randomAlphaOfLengthBetween(1, 8);
        String high = "z" + randomAlphaOfLengthBetween(1, 8);
        indexDocs(List.of(low, high));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE %s
            | KEEP id
            """, randomizedComparison(">=", "m")), equalTo(expectedLowerOnlyRangeQuery("m", true)), ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * {@code field_extract(...) < "m"} pushes to a single-sided {@code RangeQuery} against the
     * keyed sub-field. The keyed flattened mapper substitutes a {@code <key>\0} sentinel for the
     * open lower bound on the data node so the Lucene term range starts at the smallest term
     * for this key, and the data driver keeps the {@code FilterOperator} for the recheck.
     */
    public void testLessThanPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String low = "a" + randomAlphaOfLengthBetween(1, 8);
        String high = "z" + randomAlphaOfLengthBetween(1, 8);
        indexDocs(List.of(low, high));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE %s
            | KEEP id
            """, randomizedComparison("<", "m")), equalTo(expectedUpperOnlyRangeQuery("m", false)), ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * Same shape as {@link #testLessThanPushed} but with an inclusive upper bound. The
     * {@code includeUpper=true} flag must reach the printed Lucene query.
     */
    public void testLessThanOrEqualPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String low = "a" + randomAlphaOfLengthBetween(1, 8);
        String high = "z" + randomAlphaOfLengthBetween(1, 8);
        indexDocs(List.of(low, high));

        runAndAssert(String.format(Locale.ROOT, """
            FROM test
            | WHERE %s
            | KEEP id
            """, randomizedComparison("<=", "m")), equalTo(expectedUpperOnlyRangeQuery("m", true)), ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * A closed range over {@code field_extract(root, "key")} (the conjunction of one {@code >=}
     * and one {@code <=}, equivalent to {@code BETWEEN}) is merged into one {@code RangeQuery} by the
     * logical {@code CombineBinaryComparisons} rule and pushed against the keyed sub-field
     * {@code <root>._keyed}. The data driver keeps the {@code FilterOperator} for the recheck. The
     * Lucene profile should show a single per-key range query with both bounds prefixed by the key's
     * {@code <key>\0} separator.
     */
    public void testBetweenPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        // Three documents with fixed, strictly-ordered host.name values so the [b, y] range
        // catches exactly the middle doc. Using random strings here would risk collisions
        // with the bounds and make the expected hit count non-deterministic.
        indexDocs(List.of("aaa", "mmm", "zzz"));

        runAndAssert(
            String.format(Locale.ROOT, """
                FROM test
                | WHERE %s AND %s
                | KEEP id
                """, randomizedComparison(">=", "b"), randomizedComparison("<=", "y")),
            equalTo(expectedRangeQuery("b", true, "y", true)),
            ComputeSignature.FILTER_IN_COMPUTE,
            1
        );
    }

    /**
     * Expected printed form of a closed {@code field_extract} range pushed to the keyed sub-field.
     * The two single-sided comparators are merged into one {@code RangeQuery} by the logical
     * {@code CombineBinaryComparisons} rule before pushdown. Under RECHECK the range is bare (no
     * {@code SingleValueQuery} wrapper), so the shape is identical across modes. Lucene's
     * {@code TermRangeQuery.toString} renders as {@code field:[lower TO upper]} for inclusive bounds
     * (and {@code {…}} for exclusive), with each bound being the raw UTF-8 of the keyed term
     * ({@code <key>\0<value>}).
     */
    private String expectedRangeQuery(String lower, boolean includeLower, String upper, boolean includeUpper) {
        String openBracket = includeLower ? "[" : "{";
        String closeBracket = includeUpper ? "]" : "}";
        return KEYED_INTERNAL_FIELD
            + ":"
            + openBracket
            + SUBKEY
            + KEYED_TERM_SEPARATOR
            + lower
            + " TO "
            + SUBKEY
            + KEYED_TERM_SEPARATOR
            + upper
            + closeBracket;
    }

    /**
     * Expected printed form of a single-sided {@code field_extract} range with only the lower
     * bound set, pushed to the keyed sub-field. The keyed flattened mapper substitutes
     * {@code <key>\1} (the {@link #KEYED_TERM_OPEN_UPPER_SENTINEL}) exclusive for the open upper
     * bound. The bracket choice reflects only the caller-supplied {@code includeLower}: the
     * upper side is always exclusive because the sentinel itself is exclusive. Bare range under
     * RECHECK, so the shape is identical across modes.
     */
    private String expectedLowerOnlyRangeQuery(String lower, boolean includeLower) {
        String openBracket = includeLower ? "[" : "{";
        return KEYED_INTERNAL_FIELD
            + ":"
            + openBracket
            + SUBKEY
            + KEYED_TERM_SEPARATOR
            + lower
            + " TO "
            + SUBKEY
            + KEYED_TERM_OPEN_UPPER_SENTINEL
            + "}";
    }

    /**
     * Expected printed form of a single-sided {@code field_extract} range with only the upper
     * bound set, pushed to the keyed sub-field. The keyed flattened mapper substitutes
     * {@code <key>\0} (the {@link #KEYED_TERM_SEPARATOR}) inclusive for the open lower bound,
     * which is the encoding of value {@code ""} and the smallest term in this key's slice. The
     * bracket choice reflects only the caller-supplied {@code includeUpper}: the lower side is
     * always inclusive because the sentinel is inclusive. Bare range under RECHECK, so the shape is
     * identical across modes.
     */
    private String expectedUpperOnlyRangeQuery(String upper, boolean includeUpper) {
        String closeBracket = includeUpper ? "]" : "}";
        return KEYED_INTERNAL_FIELD + ":[" + SUBKEY + KEYED_TERM_SEPARATOR + " TO " + SUBKEY + KEYED_TERM_SEPARATOR + upper + closeBracket;
    }

    /**
     * Compute signature expected on the data driver. Under RECHECK the candidate query is pushed to
     * Lucene <em>and</em> the predicate is retained, so every {@code field_extract} pushdown keeps a
     * {@code FilterOperator}. {@link #FILTER_IN_COMPUTE} has two {@code ValuesSourceReaderOperator}
     * stages because the WHERE clause reads the flattened sub-field and the KEEP clause reads
     * {@code id}; with two distinct fields we end up with two reader stages.
     */
    private enum ComputeSignature {
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
        Matcher<String> luceneQueryMatcher,
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
                name = PushQueriesStringIT.TO_NAME.matcher(name).replaceAll("");
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

    /**
     * Returns a randomized comparison fragment between {@code field_extract(<root>, "<key>")} and
     * {@code "<literal>"} that is semantically equivalent to {@code field_extract(...) op literal}
     * but with a 50/50 chance of being printed as {@code "literal" flipped(op) field_extract(...)}.
     * The {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.LiteralsOnTheRight} optimizer
     * rule rotates the literal back to the right, and {@code BinaryComparison#swapLeftAndRight}
     * flips the operator on the way (so {@code 5 < x} becomes {@code x > 5}). The pushed Lucene
     * query is therefore identical regardless of which side the literal started on. Randomizing
     * here guards against a regression where the rotation rule is skipped for {@code field_extract}
     * call sites and an otherwise-pushed comparison silently falls back to a per-row filter.
     */
    private String randomizedComparison(String op, String literal) {
        boolean literalOnRight = randomBoolean();
        String fieldExtract = String.format(Locale.ROOT, "field_extract(%s, \"%s\")", FLATTENED_ROOT, SUBKEY);
        return literalOnRight
            ? String.format(Locale.ROOT, "%s %s \"%s\"", fieldExtract, op, literal)
            : String.format(Locale.ROOT, "\"%s\" %s %s", literal, flippedComparator(op), fieldExtract);
    }

    private static String flippedComparator(String op) {
        return switch (op) {
            case ">" -> "<";
            case ">=" -> "<=";
            case "<" -> ">";
            case "<=" -> ">=";
            default -> throw new IllegalArgumentException("unsupported operator for flip [" + op + "]");
        };
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
