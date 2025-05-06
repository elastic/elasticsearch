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
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.commonProfile;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.fixTypesOnProfile;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for pushing queries to lucene.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushQueriesIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    public void testPushEqualityOnDefaults() throws IOException {
        String value = "v".repeat(between(0, 256));
        testPushQuery(value, """
            FROM test
            | WHERE test == "%value"
            """, "*:*", true, true);
    }

    public void testPushEqualityOnDefaultsTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        testPushQuery(value, """
            FROM test
            | WHERE test == "%value"
            """, "*:*", true, true);
    }

    public void testPushInequalityOnDefaults() throws IOException {
        String value = "v".repeat(between(0, 256));
        testPushQuery(value, """
            FROM test
            | WHERE test != "%different_value"
            """, "*:*", true, true);
    }

    public void testPushInequalityOnDefaultsTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        testPushQuery(value, """
            FROM test
            | WHERE test != "%value"
            """, "*:*", true, false);
    }

    public void testPushCaseInsensitiveEqualityOnDefaults() throws IOException {
        String value = "a".repeat(between(0, 256));
        testPushQuery(value, """
            FROM test
            | WHERE TO_LOWER(test) == "%value"
            """, "*:*", true, true);
    }

    private void testPushQuery(String value, String esqlQuery, String luceneQuery, boolean filterInCompute, boolean found)
        throws IOException {
        indexValue(value);
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length() == 0 ? 1 : value.length()));

        String replacedQuery = esqlQuery.replaceAll("%value", value).replaceAll("%different_value", differentValue);
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(replacedQuery + "\n| KEEP test");
        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), RestEsqlTestCase.Mode.SYNC);
        assertResultMap(
            result,
            getResultMatcher(result).entry(
                "profile",
                matchesMap().entry("drivers", instanceOf(List.class))
                    .entry("planning", matchesMap().extraOk())
                    .entry("query", matchesMap().extraOk())
            ),
            matchesList().item(matchesMap().entry("name", "test").entry("type", "text")),
            equalTo(found ? List.of(List.of(value)) : List.of())
        );

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            List<String> sig = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");
            for (Map<String, Object> o : operators) {
                sig.add(checkOperatorProfile(o, luceneQuery.replaceAll("%value", value).replaceAll("%different_value", differentValue)));
            }
            String description = p.get("description").toString();
            switch (description) {
                case "data" -> {
                    ListMatcher matcher = matchesList().item("LuceneSourceOperator").item("ValuesSourceReaderOperator");
                    if (filterInCompute) {
                        matcher = matcher.item("FilterOperator").item("LimitOperator");
                    }
                    matcher = matcher.item("ProjectOperator").item("ExchangeSinkOperator");
                    assertMap(sig, matcher);
                }
                case "node_reduce" -> {
                    if (sig.contains("LimitOperator")) {
                        // TODO figure out why this is sometimes here and sometimes not
                        assertMap(sig, matchesList().item("ExchangeSourceOperator").item("LimitOperator").item("ExchangeSinkOperator"));
                    } else {
                        assertMap(sig, matchesList().item("ExchangeSourceOperator").item("ExchangeSinkOperator"));
                    }
                }
                case "final" -> assertMap(
                    sig,
                    matchesList().item("ExchangeSourceOperator").item("LimitOperator").item("ProjectOperator").item("OutputOperator")
                );
                default -> throw new IllegalArgumentException("can't match " + description);
            }
        }
    }

    private void indexValue(String value) throws IOException {
        Request createIndex = new Request("PUT", "test");
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1
                }
              }
            }""");
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        bulk.setJsonEntity(String.format(Locale.ROOT, """
            {"create":{"_index":"test"}}
            {"test":"%s"}
            """, value));
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private static final Pattern TO_NAME = Pattern.compile("\\[.+", Pattern.DOTALL);

    private static String checkOperatorProfile(Map<String, Object> o, String query) {
        String name = (String) o.get("operator");
        name = TO_NAME.matcher(name).replaceAll("");
        if (name.equals("LuceneSourceOperator")) {
            MapMatcher expectedOp = matchesMap().entry("operator", startsWith(name))
                .entry("status", matchesMap().entry("processed_queries", List.of(query)).extraOk());
            assertMap(o, expectedOp);
        }
        return name;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
