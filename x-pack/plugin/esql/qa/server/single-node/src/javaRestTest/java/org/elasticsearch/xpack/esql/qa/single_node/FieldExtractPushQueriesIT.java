/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
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
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FieldExtractPushQueriesIT extends ESRestTestCase {

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
     * {@code FlattenedFieldParser#SEPARATOR}.
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

        runAndAssert(
            String.format(Locale.ROOT, """
                FROM test
                | WHERE field_extract(%s, "%s") == "%s"
                | KEEP id
                """, FLATTENED_ROOT, SUBKEY, value),
            equalTo(KEYED_INTERNAL_FIELD + ":" + SUBKEY + KEYED_TERM_SEPARATOR + value),
            ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code field_extract(...) != "v"} must push to a negated {@code TermQuery} against the keyed
     * sub-field. Lucene renders the negation as {@code -<inner> #*:*} (must-not + filter match-all).
     */
    public void testInequalityPushed() throws IOException {
        assumeTrue("fn_field_extract must be enabled", FieldExtract.isFnFieldExtractCapabilityMet());
        String value = randomAlphaOfLengthBetween(1, 16);
        String otherValue = randomValueOtherThan(value, () -> randomAlphaOfLengthBetween(1, 16));
        indexDocs(List.of(value, otherValue));

        runAndAssert(
            String.format(Locale.ROOT, """
                FROM test
                | WHERE field_extract(%s, "%s") != "%s"
                | KEEP id
                """, FLATTENED_ROOT, SUBKEY, value),
            equalTo("-" + KEYED_INTERNAL_FIELD + ":" + SUBKEY + KEYED_TERM_SEPARATOR + value + " #*:*"),
            ComputeSignature.FILTER_IN_QUERY,
            1
        );
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

        runAndAssert(
            String.format(Locale.ROOT, """
                FROM test
                | WHERE field_extract(%s, "%s") IN ("%s", "%s")
                | KEEP id
                """, FLATTENED_ROOT, SUBKEY, first, second),
            startsWith(KEYED_INTERNAL_FIELD + ":(" + SUBKEY + KEYED_TERM_SEPARATOR),
            ComputeSignature.FILTER_IN_QUERY,
            2
        );
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
        try {
            client().performRequest(new Request("DELETE", "test"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request createIndex = new Request("PUT", "test");
        createIndex.setJsonEntity(String.format(Locale.ROOT, """
            {
              "settings": { "index": { "number_of_shards": 1 } },
              "mappings": {
                "properties": {
                  "id": { "type": "keyword" },
                  "%s": { "type": "flattened" }
                }
              }
            }
            """, FLATTENED_ROOT));
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
            body.append(
                String.format(Locale.ROOT, "{\"id\":\"doc-%d\",\"%s\":{\"%s\":\"%s\"}}\n", i, FLATTENED_ROOT, SUBKEY, hostNameValues.get(i))
            );
        }
        bulk.setJsonEntity(body.toString());
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
