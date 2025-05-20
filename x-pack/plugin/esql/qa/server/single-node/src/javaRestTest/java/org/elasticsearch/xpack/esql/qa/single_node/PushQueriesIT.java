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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for pushing queries to lucene.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushQueriesIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> args() {
        return Stream.of("auto", "text", "match_only_text", "semantic_text").map(s -> new Object[] { s }).toList();
    }

    private final String type;

    public PushQueriesIT(String type) {
        this.type = type;
    }

    public void testEquality() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value"
            """;
        String luceneQuery = switch (type) {
            case "text", "auto" -> "#test.keyword:%value -_ignored:test.keyword";
            case "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        boolean filterInCompute = switch (type) {
            case "text", "auto" -> false;
            case "match_only_text", "semantic_text" -> true;
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), filterInCompute, true);
    }

    public void testEqualityTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value"
            """;
        String luceneQuery = switch (type) {
            case "text", "auto", "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), true, true);
    }

    /**
     * Turns into an {@code IN} which isn't currently pushed.
     */
    public void testEqualityOrTooBig() throws IOException {
        String value = "v".repeat(between(0, 256));
        String tooBig = "a".repeat(between(257, 1000));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" OR test == "%tooBig"
            """.replace("%tooBig", tooBig);
        String luceneQuery = switch (type) {
            case "text", "auto", "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), true, true);
    }

    public void testEqualityOrOther() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" OR foo == 2
            """;
        String luceneQuery = switch (type) {
            case "text", "auto" -> "(#test.keyword:%value -_ignored:test.keyword) foo:[2 TO 2]";
            case "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        boolean filterInCompute = switch (type) {
            case "text", "auto" -> false;
            case "match_only_text", "semantic_text" -> true;
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), filterInCompute, true);
    }

    public void testEqualityAndOther() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" AND foo == 1
            """;
        List<String> luceneQueryOptions = switch (type) {
            case "text", "auto" -> List.of("#test.keyword:%value -_ignored:test.keyword #foo:[1 TO 1]");
            case "match_only_text" -> List.of("foo:[1 TO 1]");
            case "semantic_text" ->
                /*
                 * single_value_match is here because there are extra documents hiding in the index
                 * that don't have the `foo` field.
                 */
                List.of("#foo:[1 TO 1] #single_value_match(foo)", "foo:[1 TO 1]");
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        boolean filterInCompute = switch (type) {
            case "text", "auto" -> false;
            case "match_only_text", "semantic_text" -> true;
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, luceneQueryOptions, filterInCompute, true);
    }

    public void testInequality() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test != "%different_value"
            """;
        String luceneQuery = switch (type) {
            case "text", "auto" -> "(-test.keyword:%different_value #*:*) _ignored:test.keyword";
            case "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), true, true);
    }

    public void testInequalityTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        String esqlQuery = """
            FROM test
            | WHERE test != "%value"
            """;
        String luceneQuery = switch (type) {
            case "text", "auto", "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), true, false);
    }

    public void testCaseInsensitiveEquality() throws IOException {
        String value = "a".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) == "%value"
            """;
        String luceneQuery = switch (type) {
            case "text", "auto", "match_only_text" -> "*:*";
            case "semantic_text" -> "FieldExistsQuery [field=_primary_term]";
            default -> throw new UnsupportedOperationException("unknown type [" + type + "]");
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), true, true);
    }

    private void testPushQuery(String value, String esqlQuery, List<String> luceneQueryOptions, boolean filterInCompute, boolean found)
        throws IOException {
        indexValue(value);
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));

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
        Matcher<String> luceneQueryMatcher = anyOf(
            () -> Iterators.map(
                luceneQueryOptions.iterator(),
                (String s) -> equalTo(s.replaceAll("%value", value).replaceAll("%different_value", differentValue))
            )
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
                sig.add(checkOperatorProfile(o, luceneQueryMatcher));
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
        try {
            // Delete the index if it has already been created.
            client().performRequest(new Request("DELETE", "test"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request createIndex = new Request("PUT", "test");
        String json = """
            {
              "settings": {
                "index": {
                  "number_of_shards": 1
                }
              }""";
        if (false == "auto".equals(type)) {
            json += """
                ,
                "mappings": {
                  "properties": {
                    "test": {
                      "type": "%type",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      }
                    }
                  }
                }""".replace("%type", type);
        }
        json += "}";
        createIndex.setJsonEntity(json);
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        bulk.setJsonEntity(String.format(Locale.ROOT, """
            {"create":{"_index":"test"}}
            {"test":"%s","foo":1}
            """, value));
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private static final Pattern TO_NAME = Pattern.compile("\\[.+", Pattern.DOTALL);

    private static String checkOperatorProfile(Map<String, Object> o, Matcher<String> query) {
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

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Preserve the cluser to speed up the semantic_text tests
        return true;
    }
}
