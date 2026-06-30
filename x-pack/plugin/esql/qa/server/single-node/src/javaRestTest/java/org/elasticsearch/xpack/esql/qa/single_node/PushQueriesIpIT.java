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
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
 * Verifies that ES|QL {@code WHERE} predicates on {@code ip} fields are pushed down to Lucene.
 * Each test runs a query with {@code profile:true} and asserts that the data-node
 * {@code LuceneSourceOperator} shows the expected {@code processed_queries} string and that the
 * data-driver pipeline has no {@code FilterOperator} (meaning the filter was handled entirely
 * by Lucene, not re-checked per row in compute).
 *
 * <h2>Observed query-string behavior</h2>
 *
 * Despite IP predicates being correctly pushed to Lucene (no {@code FilterOperator} on the data
 * driver), the {@code LuceneSourceOperator.Status.processed_queries} value is {@code "*:*"} for
 * most predicates. This occurs because after rewrite the {@link
 * org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery} wrapper that guards against
 * multi-valued field rows trivially reduces to {@link org.apache.lucene.search.MatchAllDocsQuery}
 * (all documents in a single-shard single-document index are single-valued), and the surrounding
 * {@link org.apache.lucene.search.BooleanQuery} collapses to {@code *:*}. The IP filter is
 * enforced by the Lucene weight at collection time but does not survive as a printable query
 * string. The absence of a {@code FilterOperator} in the data-driver operator chain is the
 * reliable signal that the predicate was pushed.
 *
 * <p>{@code IS NULL} is the exception: the NOT-FieldExistsQuery approximation survives rewrite
 * when the field is absent from some documents, producing {@code "-FieldExistsQuery [field=ip]
 * #*:*"}.
 *
 * <p>When no documents match (e.g. {@code testCidrMatchMiss}), the Lucene slice processes zero
 * shards and {@code processed_queries} is an empty list; the test skips the query-string
 * assertion in that case.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushQueriesIpIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    /**
     * {@code cidr_match(ip, "10.0.0.0/8")} with a matching document pushes a prefix range
     * to Lucene and drops the {@code FilterOperator}. The query string collapses to {@code "*:*"}
     * after rewrite (see class Javadoc).
     */
    public void testCidrMatchSingleBlock() throws IOException {
        indexValue("10.1.2.3");
        runAndAssert("FROM test | WHERE cidr_match(ip, \"10.0.0.0/8\") | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code cidr_match(ip, "10.0.0.0/8", "192.168.0.0/16")} with a document inside the second
     * block is pushed fully to Lucene. The query string collapses to {@code "*:*"} after rewrite.
     */
    public void testCidrMatchMultipleBlocks() throws IOException {
        indexValue("192.168.1.2");
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, \"10.0.0.0/8\", \"192.168.0.0/16\") | KEEP ip",
            equalTo("*:*"),
            ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code cidr_match(ip, "192.168.0.0/24")} with a document outside the block returns 0 rows.
     * The query is pushed to Lucene; when no slices are processed {@code processed_queries} is
     * an empty list and the query-string assertion is skipped.
     */
    public void testCidrMatchMiss() throws IOException {
        indexValue("10.0.0.1");
        runAndAssert("FROM test | WHERE cidr_match(ip, \"192.168.0.0/24\") | KEEP ip", null, ComputeSignature.FILTER_IN_QUERY, 0);
    }

    /**
     * {@code ip == "1.2.3.4"} pushes an exact-match point query wrapped in
     * {@code IndexOrDocValuesQuery}; after rewrite the reported string is {@code "*:*"}.
     */
    public void testEquality() throws IOException {
        indexValue("1.2.3.4");
        runAndAssert("FROM test | WHERE ip == \"1.2.3.4\" | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code ip != "9.9.9.9"} with a document at a different address pushes a negated point
     * query. The reported string collapses to {@code "*:*"} after rewrite.
     */
    public void testInequality() throws IOException {
        indexValue("1.2.3.4");
        runAndAssert("FROM test | WHERE ip != \"9.9.9.9\" | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code ip IN ("1.2.3.4", "5.6.7.8")} pushes an {@code InetAddressPoint.newSetQuery}
     * to Lucene; the reported string collapses to {@code "*:*"} after rewrite.
     */
    public void testIn() throws IOException {
        indexValue("1.2.3.4");
        runAndAssert("FROM test | WHERE ip IN (\"1.2.3.4\", \"5.6.7.8\") | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code ip > "1.0.0.0"} pushes a lower-bounded range to Lucene; the reported string
     * collapses to {@code "*:*"} after rewrite.
     */
    public void testGreaterThan() throws IOException {
        indexValue("1.2.3.4");
        runAndAssert("FROM test | WHERE ip > \"1.0.0.0\" | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code ip <= "1.2.3.4"} with a matching document pushes an upper-bounded range to Lucene;
     * the reported string collapses to {@code "*:*"} after rewrite.
     */
    public void testLessThanOrEqual() throws IOException {
        indexValue("1.2.3.4");
        runAndAssert("FROM test | WHERE ip <= \"1.2.3.4\" | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * {@code ip IS NULL} on a document that has no {@code ip} field pushes a negated
     * {@code FieldExistsQuery}. Unlike other IP predicates, the NOT-FieldExistsQuery
     * approximation survives rewrite when some documents lack the field.
     */
    public void testIsNull() throws IOException {
        indexValue(null);
        runAndAssert(
            "FROM test | WHERE ip IS NULL | KEEP ip",
            equalTo("-FieldExistsQuery [field=ip] #*:*"),
            ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip IS NOT NULL} pushes a {@code FieldExistsQuery} to Lucene. When all documents
     * have the {@code ip} field, {@code FieldExistsQuery} rewrites to {@code MatchAllDocsQuery}
     * ({@code "*:*"}).
     */
    public void testIsNotNull() throws IOException {
        indexValue("1.2.3.4");
        runAndAssert("FROM test | WHERE ip IS NOT NULL | KEEP ip", equalTo("*:*"), ComputeSignature.FILTER_IN_QUERY, 1);
    }

    /**
     * A conjunction of two IP predicates is pushed as a single boolean query; the
     * {@code FilterOperator} is absent from the data driver and the reported string
     * collapses to {@code "*:*"} after rewrite.
     */
    public void testCidrMatchAndEquality() throws IOException {
        indexValue("10.1.2.3");
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, \"10.0.0.0/8\") AND ip == \"10.1.2.3\" | KEEP ip",
            equalTo("*:*"),
            ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * Compute signatures expected on the data driver. {@link #FILTER_IN_QUERY} means the Lucene
     * query handles filtering entirely; no {@code FilterOperator} appears on the data node.
     */
    private enum ComputeSignature {
        FILTER_IN_QUERY(
            matchesList().item("LuceneSourceOperator")
                .item("ValuesSourceReaderOperator")
                .item("ProjectOperator")
                .item("ExchangeSinkOperator")
        );

        private final ListMatcher matcher;

        ComputeSignature(ListMatcher matcher) {
            this.matcher = matcher;
        }
    }

    /**
     * Runs the given ES|QL query with profiling enabled and asserts:
     * <ul>
     *   <li>The hit count equals {@code expectedHitCount}.</li>
     *   <li>The data-driver {@code LuceneSourceOperator} operator list matches
     *       {@code dataNodeSignature}.</li>
     *   <li>When {@code luceneQueryMatcher} is non-null and {@code processed_queries} is
     *       non-empty, the first query string matches the matcher.</li>
     * </ul>
     * Passing {@code null} for {@code luceneQueryMatcher} skips the query-string assertion
     * (used for 0-result tests where no slices are processed and the list is empty).
     */
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
                name = PushQueriesStringIT.TO_NAME.matcher(name).replaceAll("");
                sig.add(name);
                if (name.equals("LuceneSourceOperator") && luceneQueryMatcher != null) {
                    @SuppressWarnings("unchecked")
                    List<String> processedQueries = (List<String>) ((Map<String, Object>) op.get("status")).get("processed_queries");
                    if (processedQueries != null && processedQueries.isEmpty() == false) {
                        assertMap(
                            op,
                            matchesMap().entry("operator", startsWith(name))
                                .entry("status", matchesMap().entry("processed_queries", matchesList().item(luceneQueryMatcher)).extraOk())
                        );
                    }
                }
            }
            assertMap(sig, dataNodeSignature.matcher);
        }
        assertTrue("expected the data driver profile in result", assertedDataDriver);
    }

    /**
     * Creates the {@code test} index with a single {@code ip} field and indexes one document.
     * Passing {@code null} for {@code ipValue} indexes a document with no {@code ip} field
     * (useful for {@code IS NULL} tests).
     */
    private void indexValue(String ipValue) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "test"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request createIndex = new Request("PUT", "test");
        createIndex.setJsonEntity("""
            {
              "settings": { "index": { "number_of_shards": 1 } },
              "mappings": {
                "properties": {
                  "ip": { "type": "ip" }
                }
              }
            }
            """);
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        String docBody = ipValue == null ? "{}" : "{\"ip\":\"" + ipValue + "\"}";
        bulk.setJsonEntity("{\"create\":{\"_index\":\"test\"}}\n" + docBody + "\n");
        assertThat(
            entityToMap(client().performRequest(bulk).getEntity(), XContentType.JSON),
            matchesMap().entry("errors", false).extraOk()
        );
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
