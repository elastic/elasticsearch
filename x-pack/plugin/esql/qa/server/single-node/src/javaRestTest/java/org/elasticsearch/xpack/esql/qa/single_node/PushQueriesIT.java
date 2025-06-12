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
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.plugin("inference-service-test"));

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> args() {
        return Arrays.stream(Type.values()).map(s -> new Object[] { s }).toList();
    }

    public enum Type {
        AUTO(false),
        CONSTANT_KEYWORD(false),
        KEYWORD(false),
        MATCH_ONLY_TEXT_WITH_KEYWORD(false),
        SEMANTIC_TEXT_WITH_KEYWORD(true),
        TEXT_WITH_KEYWORD(false);

        private final boolean needEmbeddings;

        Type(boolean needEmbeddings) {
            this.needEmbeddings = needEmbeddings;
        }
    }

    private final Type type;

    public PushQueriesIT(Type type) {
        this.type = type;
    }

    public void testEquality() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> "#test.keyword:%value -_ignored:test.keyword";
            case KEYWORD -> "test:%value";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, CONSTANT_KEYWORD, KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, true);
    }

    public void testEqualityTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "#test:%value #single_value_match(test)";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD ->
                ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, type != Type.KEYWORD);
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
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "test:(%tooBig %value)".replace("%tooBig", tooBig);
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, true);
    }

    public void testEqualityOrOther() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" OR foo == 2
            """;
        String luceneQuery = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> "(#test.keyword:%value -_ignored:test.keyword) foo:[2 TO 2]";
            case KEYWORD -> "test:%value foo:[2 TO 2]";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, CONSTANT_KEYWORD, KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, true);
    }

    public void testEqualityAndOther() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" AND foo == 1
            """;
        List<String> luceneQueryOptions = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> List.of("#test.keyword:%value -_ignored:test.keyword #foo:[1 TO 1]");
            case KEYWORD -> List.of("#test:%value #foo:[1 TO 1]");
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> List.of("foo:[1 TO 1]");
            case SEMANTIC_TEXT_WITH_KEYWORD ->
                /*
                 * single_value_match is here because there are extra documents hiding in the index
                 * that don't have the `foo` field.
                 */
                List.of(
                    "#foo:[1 TO 1] #single_value_match(foo) #FieldExistsQuery [field=_primary_term]",
                    "#foo:[1 TO 1] #FieldExistsQuery [field=_primary_term]"
                );
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, CONSTANT_KEYWORD, KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, luceneQueryOptions, dataNodeSignature, true);
    }

    public void testInequality() throws IOException {
        String value = "v".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE test != "%different_value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> "(-test.keyword:%different_value #*:*) _ignored:test.keyword";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-test:%different_value #*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, true);
    }

    public void testInequalityTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        String esqlQuery = """
            FROM test
            | WHERE test != "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-test:%value #single_value_match(test)";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
            case CONSTANT_KEYWORD -> ComputeSignature.FIND_NONE;
            case KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, false);
    }

    public void testCaseInsensitiveEquality() throws IOException {
        String value = "a".repeat(between(0, 256));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) == "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "CaseInsensitiveTermQuery{test:%value}";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD ->
                ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, esqlQuery, List.of(luceneQuery), dataNodeSignature, true);
    }

    enum ComputeSignature {
        FILTER_IN_COMPUTE(
            matchesList().item("LuceneSourceOperator")
                .item("ValuesSourceReaderOperator")
                .item("FilterOperator")
                .item("LimitOperator")
                .item("ProjectOperator")
                .item("ExchangeSinkOperator")
        ),
        FILTER_IN_QUERY(
            matchesList().item("LuceneSourceOperator")
                .item("ValuesSourceReaderOperator")
                .item("ProjectOperator")
                .item("ExchangeSinkOperator")
        ),
        FIND_NONE(matchesList().item("LocalSourceOperator").item("ExchangeSinkOperator"));

        private final ListMatcher matcher;

        ComputeSignature(ListMatcher sig) {
            this.matcher = sig;
        }
    }

    private void testPushQuery(
        String value,
        String esqlQuery,
        List<String> luceneQueryOptions,
        ComputeSignature dataNodeSignature,
        boolean found
    ) throws IOException {
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
            matchesList().item(matchesMap().entry("name", "test").entry("type", anyOf(equalTo("text"), equalTo("keyword")))),
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
                    assertMap(sig, dataNodeSignature.matcher);
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
        json += switch (type) {
            case AUTO -> "";
            case CONSTANT_KEYWORD -> justType();
            case KEYWORD -> keyword();
            case SEMANTIC_TEXT_WITH_KEYWORD -> semanticTextWithKeyword();
            case TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> typeWithKeyword();
        };
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

    private String justType() {
        return """
            ,
            "mappings": {
              "properties": {
                "test": {
                  "type": "%type"
                }
              }
            }""".replace("%type", type.name().toLowerCase(Locale.ROOT));
    }

    private String keyword() {
        return """
            ,
            "mappings": {
              "properties": {
                "test": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }""";
    }

    private String typeWithKeyword() {
        return """
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
            }""".replace("%type", type.name().replace("_WITH_KEYWORD", "").toLowerCase(Locale.ROOT));
    }

    private String semanticTextWithKeyword() {
        return """
            ,
            "mappings": {
              "properties": {
                "test": {
                  "type": "semantic_text",
                  "inference_id": "test",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            }""";
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

    private static boolean setupEmbeddings = false;

    @Before
    public void setUpTextEmbeddingInferenceEndpoint() throws IOException {
        if (type.needEmbeddings == false || setupEmbeddings) {
            return;
        }
        setupEmbeddings = true;
        Request request = new Request("PUT", "_inference/text_embedding/test");
        request.setJsonEntity("""
                  {
                   "service": "text_embedding_test_service",
                   "service_settings": {
                     "model": "my_model",
                     "api_key": "abc64",
                     "dimensions": 128
                   },
                   "task_settings": {
                   }
                 }
            """);
        adminClient().performRequest(request);
    }
}
