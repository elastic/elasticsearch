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
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Verifies that ES|QL {@code WHERE} predicates on {@code ip} fields are pushed down to Lucene.
 * Each test runs a query with {@code profile:true} and asserts that the data-node
 * {@code LuceneSourceOperator} shows the expected {@code processed_queries} string and that the
 * data-driver pipeline has no {@code FilterOperator} (meaning the filter was handled entirely
 * by Lucene, not re-checked per row in compute).
 *
 * <h2>Two-document index baseline</h2>
 *
 * Every test indexes two documents: a primary {@code value} and a secondary {@code differentValue}
 * (fixed at {@code "9.9.9.9"} unless overridden). This prevents {@code PointRangeQuery} from
 * rewriting to {@code MatchAllDocsQuery}: with a single doc the global point range collapses to a
 * single point and exact-match or range queries whose bounds contain that point rewrite to
 * {@code *:*}; with two distinct IP values the query bounds no longer cover the full global range,
 * so the real {@code IndexOrDocValuesQuery} / {@code PointRangeQuery} survives rewrite.
 *
 * <h2>IS NULL and IS NOT NULL exceptions</h2>
 *
 * {@code IS NULL} goes through {@code ExistsQuery.negate()} rather than a point query, so the
 * NOT-FieldExistsQuery approximation survives rewrite when some documents lack the field,
 * producing {@code "-FieldExistsQuery [field=ip] #*:*"}.
 *
 * {@code IS NOT NULL} produces a plain {@code FieldExistsQuery [field=ip]} that does not involve
 * a point range so it is not subject to the MatchAll rewrite.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushQueriesIpIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    /**
     * Fixed secondary IP address used as {@code differentValue} across all tests that do not need
     * a specific secondary value.
     */
    private static final String DIFFERENT_IP = "9.9.9.9";

    /**
     * {@code cidr_match(ip, "10.0.0.0/8")} with a matching document pushes a prefix range to
     * Lucene and drops the {@code FilterOperator}. With two documents the {@code PointRangeQuery}
     * survives rewrite and the profile shows the real query string.
     */
    public void testCidrMatchSingleBlock() throws IOException {
        indexValue("10.1.2.3", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, \"10.0.0.0/8\") | KEEP ip",
            startsWith("IndexOrDocValuesQuery(indexQuery=ip:[10.0.0.0 TO 10.255.255.255],"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code cidr_match(ip, "10.0.0.0/8", "192.168.0.0/16")} with a document inside the second
     * block is pushed fully to Lucene. Both CIDR ranges appear as separate OR clauses. The order
     * of ranges in the printed query string is not guaranteed, so both orderings are accepted.
     */
    public void testCidrMatchMultipleBlocks() throws IOException {
        indexValue("192.168.1.2", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, \"10.0.0.0/8\", \"192.168.0.0/16\") | KEEP ip",
            anyOf(
                startsWith("IndexOrDocValuesQuery(indexQuery=ip:[10.0.0.0 TO 10.255.255.255],"),
                startsWith("IndexOrDocValuesQuery(indexQuery=ip:[192.168.0.0 TO 192.168.255.255],")
            ),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code cidr_match(ip, "192.168.0.0/24")} with documents outside the block returns 0 rows.
     * The query is pushed to Lucene; when no slices are processed {@code processed_queries} is an
     * empty list and the query-string assertion is skipped (pass {@code null} matcher).
     */
    public void testCidrMatchMiss() throws IOException {
        indexValue("10.0.0.1", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, \"192.168.0.0/24\") | KEEP ip",
            null,
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            0
        );
    }

    /**
     * {@code ip == "1.2.3.4"} pushes an exact-match {@code IndexOrDocValuesQuery}. With two
     * documents at different IPs the query survives rewrite and shows the real string.
     */
    public void testEquality() throws IOException {
        indexValue("1.2.3.4", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE ip == \"1.2.3.4\" | KEEP ip",
            startsWith("IndexOrDocValuesQuery(indexQuery=ip:[1.2.3.4 TO 1.2.3.4],"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip != "9.9.9.9"} excludes the secondary document; only the primary survives. The
     * negated point query survives rewrite because the two documents span different IP values.
     */
    public void testInequality() throws IOException {
        indexValue("1.2.3.4", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE ip != \"9.9.9.9\" | KEEP ip",
            startsWith("-IndexOrDocValuesQuery(indexQuery=ip:[9.9.9.9 TO 9.9.9.9],"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip IN ("1.2.3.4", "5.6.7.8")} pushes an {@code InetAddressPoint} set query
     * (as individual {@code IndexOrDocValuesQuery} OR clauses for ES). {@code DIFFERENT_IP}
     * ({@code "9.9.9.9"}) is not in the set so only the primary matches.
     */
    public void testIn() throws IOException {
        indexValue("1.2.3.4", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE ip IN (\"1.2.3.4\", \"5.6.7.8\") | KEEP ip",
            startsWith("IndexOrDocValuesQuery(indexQuery=ip:[1.2.3.4 TO 1.2.3.4],"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip > "1.0.0.0"} pushes a lower-bounded range. The primary document at
     * {@code "1.2.3.4"} matches; the secondary at {@code "0.0.0.1"} does not, so the range
     * query does not cover the entire global point range and survives rewrite.
     */
    public void testGreaterThan() throws IOException {
        indexValue("1.2.3.4", "0.0.0.1");
        runAndAssert(
            "FROM test | WHERE ip > \"1.0.0.0\" | KEEP ip",
            startsWith("IndexOrDocValuesQuery(indexQuery=ip:[1.0.0.1 TO"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip <= "1.2.3.4"} with the primary at {@code "1.2.3.4"} and secondary at
     * {@code "9.9.9.9"}: only the primary satisfies the predicate.
     */
    public void testLessThanOrEqual() throws IOException {
        indexValue("1.2.3.4", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE ip <= \"1.2.3.4\" | KEEP ip",
            startsWith("IndexOrDocValuesQuery(indexQuery=ip:[0:0:0:0:0:0:0:0 TO 1.2.3.4],"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip IS NULL} on a document that has no {@code ip} field pushes a negated
     * {@code FieldExistsQuery}. The primary doc has no {@code ip} field; the secondary has
     * {@code "9.9.9.9"}, making the index non-trivial so the query survives rewrite.
     */
    public void testIsNull() throws IOException {
        indexValue(null, DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE ip IS NULL | KEEP ip",
            equalTo("-FieldExistsQuery [field=ip] #*:*"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * {@code ip IS NOT NULL} pushes a {@code FieldExistsQuery}. The primary document has
     * {@code ip = "1.2.3.4"} and the secondary has no {@code ip} field, so {@code FieldExistsQuery}
     * does not rewrite to {@code *:*} (only 1 of 2 documents has the field) and 1 row is returned.
     */
    public void testIsNotNull() throws IOException {
        indexValue("1.2.3.4", null);
        runAndAssert(
            "FROM test | WHERE ip IS NOT NULL | KEEP ip",
            equalTo("FieldExistsQuery [field=ip]"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * A conjunction of two IP predicates is pushed as a single boolean query; the
     * {@code FilterOperator} is absent from the data driver.
     */
    public void testCidrMatchAndEquality() throws IOException {
        indexValue("10.1.2.3", DIFFERENT_IP);
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, \"10.0.0.0/8\") AND ip == \"10.1.2.3\" | KEEP ip",
            startsWith("#IndexOrDocValuesQuery(indexQuery=ip:[10.0.0.0 TO 10.255.255.255],"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_QUERY,
            1
        );
    }

    /**
     * A field-vs-field equality ({@code ip == ip2}) is not pushable because the right-hand side
     * is not foldable. The predicate stays in the compute engine and a {@code FilterOperator}
     * appears on the data driver ({@link PushQueriesStringIT.ComputeSignature#FILTER_IN_COMPUTE}).
     */
    public void testEqualityNotPushed() throws IOException {
        indexValueTwoFields("1.2.3.4", "1.2.3.4", DIFFERENT_IP, "1.1.1.1");
        runAndAssert("FROM test | WHERE ip == ip2 | KEEP ip", equalTo("*:*"), PushQueriesStringIT.ComputeSignature.FILTER_IN_COMPUTE, 1);
    }

    /**
     * {@code cidr_match(ip, cidr)} where the CIDR mask is stored in a {@code keyword} field is
     * not pushable because {@code cidr} is a {@code FieldAttribute}, not foldable. The compute
     * engine handles the filter via {@code FilterOperator}.
     */
    public void testCidrMatchNotPushed() throws IOException {
        indexValueWithCidr("10.1.2.3", "10.0.0.0/8", DIFFERENT_IP, "10.0.0.0/8");
        runAndAssert(
            "FROM test | WHERE cidr_match(ip, cidr) | KEEP ip",
            equalTo("*:*"),
            PushQueriesStringIT.ComputeSignature.FILTER_IN_COMPUTE,
            1
        );
    }

    /**
     * Runs the given ES|QL query with profiling enabled and asserts:
     * <ul>
     *   <li>The hit count equals {@code expectedHitCount}.</li>
     *   <li>The data-driver operator list matches {@code dataNodeSignature}.</li>
     *   <li>When {@code luceneQueryMatcher} is non-null and {@code processed_queries} is
     *       non-empty, the first query string matches the matcher.</li>
     * </ul>
     * Passing {@code null} for {@code luceneQueryMatcher} skips the query-string assertion
     * (used for 0-result tests where no slices are processed and the list is empty).
     */
    private void runAndAssert(
        String esqlQuery,
        Matcher<String> luceneQueryMatcher,
        PushQueriesStringIT.ComputeSignature dataNodeSignature,
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
     * Creates the {@code test} index with {@code ip} and {@code ip2} fields and indexes two
     * documents: one with {@code ip = value} and one with {@code ip = differentValue}. Passing
     * {@code null} for {@code value} indexes the primary document with no {@code ip} field
     * (useful for {@code IS NULL} tests).
     */
    private void indexValue(String value, String differentValue) throws IOException {
        recreateIndex();

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        String primaryDoc = value == null ? "{}" : "{\"ip\":\"" + value + "\"}";
        String secondaryDoc = differentValue == null ? "{}" : "{\"ip\":\"" + differentValue + "\"}";
        bulk.setJsonEntity(
            "{\"create\":{\"_index\":\"test\"}}\n" + primaryDoc + "\n" + "{\"create\":{\"_index\":\"test\"}}\n" + secondaryDoc + "\n"
        );
        assertThat(
            entityToMap(client().performRequest(bulk).getEntity(), XContentType.JSON),
            matchesMap().entry("errors", false).extraOk()
        );
    }

    /**
     * Creates the {@code test} index with {@code ip} and {@code ip2} fields and indexes two
     * documents, each with both fields set. Used by {@link #testEqualityNotPushed()}.
     */
    private void indexValueTwoFields(String ip1, String ip2Primary, String ip1Different, String ip2Different) throws IOException {
        recreateIndex();

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        String primaryDoc = "{\"ip\":\"" + ip1 + "\",\"ip2\":\"" + ip2Primary + "\"}";
        String secondaryDoc = "{\"ip\":\"" + ip1Different + "\",\"ip2\":\"" + ip2Different + "\"}";
        bulk.setJsonEntity(
            "{\"create\":{\"_index\":\"test\"}}\n" + primaryDoc + "\n" + "{\"create\":{\"_index\":\"test\"}}\n" + secondaryDoc + "\n"
        );
        assertThat(
            entityToMap(client().performRequest(bulk).getEntity(), XContentType.JSON),
            matchesMap().entry("errors", false).extraOk()
        );
    }

    /**
     * Creates the {@code test} index with an {@code ip} (ip type) field and a {@code cidr}
     * (keyword type) field, then indexes two documents. Used by
     * {@link #testCidrMatchNotPushed()}.
     */
    private void indexValueWithCidr(String ip1, String cidr1, String ip1Different, String cidr1Different) throws IOException {
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
                  "ip":   { "type": "ip" },
                  "ip2":  { "type": "ip" },
                  "cidr": { "type": "keyword" }
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
        String primaryDoc = "{\"ip\":\"" + ip1 + "\",\"cidr\":\"" + cidr1 + "\"}";
        String secondaryDoc = "{\"ip\":\"" + ip1Different + "\",\"cidr\":\"" + cidr1Different + "\"}";
        bulk.setJsonEntity(
            "{\"create\":{\"_index\":\"test\"}}\n" + primaryDoc + "\n" + "{\"create\":{\"_index\":\"test\"}}\n" + secondaryDoc + "\n"
        );
        assertThat(
            entityToMap(client().performRequest(bulk).getEntity(), XContentType.JSON),
            matchesMap().entry("errors", false).extraOk()
        );
    }

    /**
     * Drops and recreates the {@code test} index with {@code ip} (ip type) and {@code ip2}
     * (ip type) fields.
     */
    private void recreateIndex() throws IOException {
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
                  "ip":  { "type": "ip" },
                  "ip2": { "type": "ip" }
                }
              }
            }
            """);
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
