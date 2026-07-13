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
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for pushing queries to lucene.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushQueriesStringIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.plugin("inference-service-test"));

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

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
        TEXT_WITH_KEYWORD(false),
        WILDCARD(false);

        private final boolean needEmbeddings;

        Type(boolean needEmbeddings) {
            this.needEmbeddings = needEmbeddings;
        }
    }

    private final Type type;

    public PushQueriesStringIT(Type type) {
        this.type = type;
    }

    public void testEquality() throws IOException {
        String value = "v".repeat(between(0, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> "#test.keyword:%value -_ignored:test.keyword";
            case KEYWORD -> "test:%value";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case WILDCARD -> ": [%value]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, CONSTANT_KEYWORD, KEYWORD, TEXT_WITH_KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testEqualityTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "#test:%value #single_value_match(test)";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            // WILDCARD has no ignore_above, so large values are still pushed
            case WILDCARD -> ": [%value]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        // KEYWORD stores values > 256 chars as null (ignore_above), so the equality query finds no doc.
        // All other types find the indexed big value.
        Matcher<?> resultMatcher = type == Type.KEYWORD ? equalTo(List.of()) : hasItem(List.of(value));
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, resultMatcher);
    }

    /**
     * Turns into an {@code IN} which isn't currently pushed.
     */
    public void testEqualityOrTooBig() throws IOException {
        String value = "v".repeat(between(0, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));
        String tooBig = "a".repeat(between(257, 1000));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" OR test == "%tooBig"
            """.replace("%tooBig", tooBig);
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("test:(%tooBig %value)".replace("%tooBig", tooBig), "test:(%value %tooBig)".replace("%tooBig", tooBig));
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
            // WILDCARD has no ignore_above, so large values are still pushed
            case WILDCARD -> List.of(": [%tooBig, %value]".replace("%tooBig", tooBig), ": [%value, %tooBig]".replace("%tooBig", tooBig));
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testEqualityOrOther() throws IOException {
        String value = "v".repeat(between(0, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" OR foo == 2
            """;
        // query rewrite optimizations apply to foo, since it's query value is always outside the range of indexed values
        List<String> luceneQuery = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> List.of(
                "#test.keyword:%value -_ignored:test.keyword",
                "(#test.keyword:%value -_ignored:test.keyword) foo:[2 TO 2]"
            );
            case KEYWORD -> List.of("test:%value", "test:%value foo:[2 TO 2]");
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> List.of("*:*");
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
            case WILDCARD -> List.of(": [%value]", ": [%value] foo:[2 TO 2]");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, CONSTANT_KEYWORD, KEYWORD, TEXT_WITH_KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testEqualityAndOther() throws IOException {
        String value = "v".repeat(between(0, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test == "%value" AND foo == 1
            """;
        // query rewrite optimizations apply to foo, since it's query value is always within the range of indexed values
        List<String> luceneQueryOptions = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> List.of(
                "#test.keyword:%value -_ignored:test.keyword",
                "#test.keyword:%value -_ignored:test.keyword foo:[2 TO 2]"
            );
            case KEYWORD -> List.of("test:%value", "test:%value foo:[2 TO 2]");
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> List.of("*:*");
            case SEMANTIC_TEXT_WITH_KEYWORD ->
                /*
                 * FieldExistsQuery is because there are extra documents hiding in the index
                 * that don't have the `foo` field. "*:*" is because sometimes we end up on
                 * a shard where all `foo = 1`. single_value_match appears when multiple docs
                 * with the same foo value are in the index.
                 */
                List.of(
                    "#foo:[1 TO 1] #FieldExistsQuery [field=_primary_term]",
                    "#foo:[1 TO 1] #single_value_match(foo) #FieldExistsQuery [field=_primary_term]",
                    "foo:[1 TO 1]",
                    "FieldExistsQuery [field=_primary_term]"
                );
            case WILDCARD -> List.of(": [%value]", ": [%value] foo:[2 TO 2]");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, CONSTANT_KEYWORD, KEYWORD, TEXT_WITH_KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQueryOptions, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testInequality() throws IOException {
        String value = "v".repeat(between(0, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test != "%different_value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, TEXT_WITH_KEYWORD -> "(-test.keyword:%different_value #*:*) _ignored:test.keyword";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-test:%different_value #*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case WILDCARD -> "-: [%different_value] #*:*";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        // value satisfies test != differentValue; differentValue does not satisfy it
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testInequalityTooBigToPush() throws IOException {
        String value = "a".repeat(between(257, 1000));
        // differentValue must be short (<=256) so KEYWORD's ignore_above doesn't drop it
        String differentValue = randomAlphaOfLengthBetween(1, 256);
        String esqlQuery = """
            FROM test
            | WHERE test != "%value"
            """;
        /*
         * With two docs, KEYWORD may produce either single_value_match(test) or *:* depending on
         * whether the shard statistics consider the field single-valued (doc1 has test=null due
         * to ignore_above; doc2 has test=differentValue).
         */
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("-test:%value #single_value_match(test)", "-test:%value #*:*");
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
            // WILDCARD has no ignore_above, so large values are still pushed
            case WILDCARD -> List.of("-: [%value] #*:*");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
            case CONSTANT_KEYWORD -> ComputeSignature.FIND_NONE;
            case KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
        };
        // The big value itself is excluded by the inequality; differentValue (shorter, != value) satisfies test != value.
        // CONSTANT_KEYWORD uses FIND_NONE — optimizer eliminates the operator, returning nothing.
        Matcher<?> resultMatcher = type == Type.CONSTANT_KEYWORD ? equalTo(List.of()) : hasItem(List.of(differentValue));
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, resultMatcher);
    }

    public void testCaseInsensitiveEquality() throws IOException {
        String value = "a".repeat(between(0, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.isEmpty() ? 1 : value.length()));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) == "%value"
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "".equals(value) ? "test:" : "CaseInsensitiveTermQuery{test:%value}";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case WILDCARD -> "".equals(value)
                ? ":PatternAutomatonProvider[matchPattern=, caseInsensitive=true]"
                : ":PatternAutomatonProvider[matchPattern=%value, caseInsensitive=true]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testCaseInsensitiveLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) like "%value*"
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case KEYWORD -> "CaseInsensitiveWildcardQuery{:%value*}";
            case WILDCARD -> ":PatternAutomatonProvider[matchPattern=%value*, caseInsensitive=true]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testNotCaseInsensitiveLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        // Must be lowercase so TO_LOWER(test) like "differentValue*" is not statically folded to false
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()).toLowerCase(Locale.ROOT));
        String esqlQuery = """
            FROM test
            | WHERE NOT (TO_LOWER(test) like "%different_value*")
            """;
        /*
         * With lowercase differentValue in the index the query rewrites differently per type:
         * KEYWORD and WILDCARD expose the real NOT query. TEXT types still produce *:* because
         * the case-insensitive LIKE on text uses a different pushdown path. CONSTANT_KEYWORD has only one
         * document, so its query also remains *:*.
         */
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-CaseInsensitiveWildcardQuery{:%different_value*} #*:*";
            case WILDCARD -> "-:PatternAutomatonProvider[matchPattern=%different_value*, caseInsensitive=true] #*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        /*
         * KEYWORD and WILDCARD push NOT case-insensitive LIKE fully to Lucene (FILTER_IN_QUERY). Text-based
         * types and SEMANTIC use FILTER_IN_COMPUTE because the case-insensitive LIKE is evaluated via a
         * compute-layer recheck. CONSTANT_KEYWORD has only one document and stays *:*
         * (FILTER_IN_QUERY).
         */
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        // value satisfies NOT(like differentValue*); differentValue is excluded for KEYWORD/WILDCARD.
        // For text-based types, both docs may be returned; hasItem checks that value is present.
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testOrNotCaseInsensitiveLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        // Must be lowercase so TO_LOWER(test) like "differentValue*" is not statically folded to false
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()).toLowerCase(Locale.ROOT));
        String esqlQuery = """
            FROM test
            | WHERE test == "cat" OR NOT (TO_LOWER(test) like "%different_value*")
            """;
        /*
         * With lowercase differentValue and two docs, the NOT case-insensitive LIKE survives
         * Lucene rewrite. KEYWORD and WILDCARD expose the real NOT query; the OR with
         * test=="cat" stays in the query for KEYWORD (test:cat) and WILDCARD (: [cat])
         * because TermQuery and BinaryDvConfirmedTermsQuery do not rewrite to MatchNoDocsQuery.
         * TEXT types push with *:* (no inverted index for case-insensitive LIKE).
         * CONSTANT_KEYWORD has only one document, so its query also remains *:*.
         * ESQL adds a compute-layer recheck for all types except WILDCARD and CONSTANT_KEYWORD.
         */
        List<String> luceneQuery = switch (type) {
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case CONSTANT_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of(
                "test:cat (-CaseInsensitiveWildcardQuery{:%different_value*} #*:*)",
                "(-CaseInsensitiveWildcardQuery{:%different_value*} #*:*) test:cat"
            );
            case WILDCARD -> List.of(": [cat] (-:PatternAutomatonProvider[matchPattern=%different_value*, caseInsensitive=true] #*:*)");
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        // value satisfies the combined OR NOT condition.
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testCaseInsensitiveLikeList() throws IOException {
        assumeFalse("WILDCARD field type does not support automaton queries", type == Type.WILDCARD);
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) like ("%value*", "abc*")
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case KEYWORD -> "test:LIKE(\"%value*\", \"abc*\"), caseInsensitive=true";
            case WILDCARD -> throw new AssertionError("unreachable");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
            case WILDCARD -> throw new AssertionError("unreachable");
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test like "%value*"
            """;
        String luceneQuery = switch (type) {
            case KEYWORD -> "test:%value*";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, AUTO, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case WILDCARD -> ":PatternAutomatonProvider[matchPattern=%value*, caseInsensitive=false]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testLikeList() throws IOException {
        assumeFalse("WILDCARD field type does not support automaton queries", type == Type.WILDCARD);
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test like ("%value*", "abc*")
            """;
        String luceneQuery = switch (type) {
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, AUTO, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case KEYWORD -> "test:LIKE(\"%value*\", \"abc*\"), caseInsensitive=false";
            case WILDCARD -> throw new AssertionError("unreachable");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
            case WILDCARD -> throw new AssertionError("unreachable");
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test rlike "%value.*"
            """;
        String luceneQuery = switch (type) {
            case KEYWORD -> "test:/%value.*/";
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, AUTO, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case WILDCARD -> ":RegexAutomatonProvider[value=%value.*, syntaxFlags=65791, matchFlags=0, maxDeterminizedStates=10000]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testRLikeList() throws IOException {
        assumeFalse("WILDCARD field type does not support automaton queries", type == Type.WILDCARD);
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test rlike ("%value.*", "abc.*")
            """;
        String luceneQuery = switch (type) {
            case CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, AUTO, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case KEYWORD -> "test:RLIKE(\"%value.*\", \"abc.*\"), caseInsensitive=false";
            case WILDCARD -> throw new AssertionError("unreachable");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, TEXT_WITH_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
            case WILDCARD -> throw new AssertionError("unreachable");
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testCaseInsensitiveRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) rlike "%value.*"
            """;
        // Case-insensitive RLIKE uses RegexpQuery with CASE_INSENSITIVE flag (matchFlags=512 for WILDCARD).
        // KEYWORD uses the same /pattern/ toString format as non-case-insensitive RLIKE (flag not shown).
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case KEYWORD -> "test:/%value.*/";
            case WILDCARD -> ":RegexAutomatonProvider[value=%value.*, syntaxFlags=65791, matchFlags=512, maxDeterminizedStates=10000]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testCaseInsensitiveRLikeList() throws IOException {
        assumeFalse("WILDCARD field type does not support automaton queries", type == Type.WILDCARD);
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE TO_LOWER(test) rlike ("%value.*", "abc.*")
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
            case KEYWORD -> "test:RLIKE(\"%value.*\", \"abc.*\"), caseInsensitive=true";
            case WILDCARD -> ":RLIKE(\"%value.*\", \"abc.*\"), caseInsensitive=true";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testNotLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE NOT (test like "%different_value*")
            """;
        /*
         * With differentValue indexed, the LIKE query survives Lucene rewrite for KEYWORD and
         * WILDCARD. Text types still produce *:* because LIKE on text is not pushed to Lucene.
         */
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-test:%different_value* #*:*";
            case WILDCARD -> "-:PatternAutomatonProvider[matchPattern=%different_value*, caseInsensitive=false] #*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testNotRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE NOT (test rlike "%different_value.*")
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-test:/%different_value.*/ #*:*";
            case WILDCARD ->
                "-:RegexAutomatonProvider[value=%different_value.*, syntaxFlags=65791, matchFlags=0, maxDeterminizedStates=10000] #*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testNotCaseInsensitiveRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        // Must be lowercase so TO_LOWER(test) rlike "differentValue.*" is not statically folded to false
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()).toLowerCase(Locale.ROOT));
        String esqlQuery = """
            FROM test
            | WHERE NOT (TO_LOWER(test) rlike "%different_value.*")
            """;
        String luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> "*:*";
            case KEYWORD -> "-test:/%different_value.*/ #*:*";
            case WILDCARD ->
                "-:RegexAutomatonProvider[value=%different_value.*, syntaxFlags=65791, matchFlags=512, maxDeterminizedStates=10000] #*:*";
            case SEMANTIC_TEXT_WITH_KEYWORD -> "FieldExistsQuery [field=_primary_term]";
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, List.of(luceneQuery), dataNodeSignature, hasItem(List.of(value)));
    }

    public void testOrNotLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test == "cat" OR NOT (test like "%different_value*")
            """;
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("test:cat (-test:%different_value* #*:*)", "(-test:%different_value* #*:*) test:cat");
            case WILDCARD -> List.of(": [cat] (-:PatternAutomatonProvider[matchPattern=%different_value*, caseInsensitive=false] #*:*)");
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testOrNotRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE test == "cat" OR NOT (test rlike "%different_value.*")
            """;
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("test:cat (-test:/%different_value.*/ #*:*)", "(-test:/%different_value.*/ #*:*) test:cat");
            case WILDCARD -> {
                String regex = ":RegexAutomatonProvider[value=%different_value.*,"
                    + " syntaxFlags=65791, matchFlags=0, maxDeterminizedStates=10000]";
                yield List.of(": [cat] (-" + regex + " #*:*)");
            }
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testOrNotCaseInsensitiveRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        // Must be lowercase so TO_LOWER(test) rlike "differentValue.*" is not statically folded to false
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()).toLowerCase(Locale.ROOT));
        String esqlQuery = """
            FROM test
            | WHERE test == "cat" OR NOT (TO_LOWER(test) rlike "%different_value.*")
            """;
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("test:cat (-test:/%different_value.*/ #*:*)", "(-test:/%different_value.*/ #*:*) test:cat");
            case WILDCARD -> {
                String regex = ":RegexAutomatonProvider[value=%different_value.*,"
                    + " syntaxFlags=65791, matchFlags=512, maxDeterminizedStates=10000]";
                yield List.of(": [cat] (-" + regex + " #*:*)");
            }
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of("FieldExistsQuery [field=_primary_term]");
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testAndNotLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE foo == 1 AND NOT (test like "%different_value*")
            """;
        /*
         * foo:[1 TO 1] is optimized away when all docs have foo=1. SEMANTIC adds single_value_match.
         */
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("-test:%different_value* #*:*");
            case WILDCARD -> List.of("-:PatternAutomatonProvider[matchPattern=%different_value*, caseInsensitive=false] #*:*");
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of(
                "FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #single_value_match(foo) #FieldExistsQuery [field=_primary_term]"
            );
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testAndNotRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()));
        String esqlQuery = """
            FROM test
            | WHERE foo == 1 AND NOT (test rlike "%different_value.*")
            """;
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("-test:/%different_value.*/ #*:*");
            case WILDCARD -> List.of(
                "-:RegexAutomatonProvider[value=%different_value.*, syntaxFlags=65791, matchFlags=0, maxDeterminizedStates=10000] #*:*"
            );
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of(
                "FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #single_value_match(foo) #FieldExistsQuery [field=_primary_term]"
            );
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testAndNotCaseInsensitiveLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        // Must be lowercase so TO_LOWER(test) like "differentValue*" is not statically folded to false
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()).toLowerCase(Locale.ROOT));
        String esqlQuery = """
            FROM test
            | WHERE foo == 1 AND NOT (TO_LOWER(test) like "%different_value*")
            """;
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("-CaseInsensitiveWildcardQuery{:%different_value*} #*:*");
            case WILDCARD -> List.of("-:PatternAutomatonProvider[matchPattern=%different_value*, caseInsensitive=true] #*:*");
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of(
                "FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #single_value_match(foo) #FieldExistsQuery [field=_primary_term]"
            );
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
    }

    public void testAndNotCaseInsensitiveRLike() throws IOException {
        String value = "v".repeat(between(1, 256));
        // Must be lowercase so TO_LOWER(test) rlike "differentValue.*" is not statically folded to false
        String differentValue = randomValueOtherThan(value, () -> randomAlphaOfLength(value.length()).toLowerCase(Locale.ROOT));
        String esqlQuery = """
            FROM test
            | WHERE foo == 1 AND NOT (TO_LOWER(test) rlike "%different_value.*")
            """;
        List<String> luceneQuery = switch (type) {
            case AUTO, CONSTANT_KEYWORD, MATCH_ONLY_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> List.of("*:*");
            case KEYWORD -> List.of("-test:/%different_value.*/ #*:*");
            case WILDCARD -> List.of(
                "-:RegexAutomatonProvider[value=%different_value.*, syntaxFlags=65791, matchFlags=512, maxDeterminizedStates=10000] #*:*"
            );
            case SEMANTIC_TEXT_WITH_KEYWORD -> List.of(
                "FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #FieldExistsQuery [field=_primary_term]",
                "#foo:[1 TO 1] #single_value_match(foo) #FieldExistsQuery [field=_primary_term]"
            );
        };
        ComputeSignature dataNodeSignature = switch (type) {
            case CONSTANT_KEYWORD, KEYWORD, WILDCARD -> ComputeSignature.FILTER_IN_QUERY;
            case AUTO, MATCH_ONLY_TEXT_WITH_KEYWORD, SEMANTIC_TEXT_WITH_KEYWORD, TEXT_WITH_KEYWORD -> ComputeSignature.FILTER_IN_COMPUTE;
        };
        testPushQuery(value, differentValue, esqlQuery, luceneQuery, dataNodeSignature, hasItem(List.of(value)));
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

        final ListMatcher matcher;

        ComputeSignature(ListMatcher sig) {
            this.matcher = sig;
        }
    }

    private void testPushQuery(
        String value,
        String differentValue,
        String esqlQuery,
        List<String> luceneQueryOptions,
        ComputeSignature dataNodeSignature,
        Matcher<?> resultMatcher
    ) throws IOException {
        indexValue(value, differentValue);

        String replacedQuery = esqlQuery.replaceAll("%value", value).replaceAll("%different_value", differentValue);
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(replacedQuery + "\n| KEEP test");
        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), profileLogger, RestEsqlTestCase.Mode.SYNC);
        assertResultMap(
            result,
            getResultMatcher(result).entry(
                "profile",
                matchesMap() //
                    .entry("drivers", instanceOf(List.class))
                    .entry("plans", instanceOf(List.class))
                    .entry("planning", matchesMap().extraOk())
                    .entry("parsing", matchesMap().extraOk())
                    .entry("view_resolution", matchesMap().extraOk())
                    .entry("dataset_resolution", matchesMap().extraOk())
                    .entry("preanalysis", matchesMap().extraOk())
                    .entry("indices_resolution", matchesMap().extraOk())
                    .entry("enrich_resolution", matchesMap().extraOk())
                    .entry("inference_resolution", matchesMap().extraOk())
                    .entry("analysis", matchesMap().extraOk())
                    .entry("query", matchesMap().extraOk())
                    .entry("field_caps_calls", instanceOf(Integer.class))
                    .entry("unmapped_fields", instanceOf(String.class))
                    .entry("minimumTransportVersion", instanceOf(Integer.class))
            ),
            matchesList().item(matchesMap().entry("name", "test").entry("type", anyOf(equalTo("text"), equalTo("keyword")))),
            resultMatcher
        );
        Matcher<String> luceneQueryMatcher = anyOf(
            () -> Iterators.map(luceneQueryOptions.iterator(), (String s) -> queryMatcher(s, value, differentValue))
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

    private Matcher<String> queryMatcher(String queryString, String value, String differentValue) {
        queryString = queryString.replaceAll("%value", value).replaceAll("%different_value", differentValue);
        if (queryString.length() <= LuceneOperator.Status.QUERY_STRING_TRUNCATION) {
            return equalTo(queryString);
        }
        return startsWith(queryString.substring(0, LuceneOperator.Status.QUERY_STRING_TRUNCATION));
    }

    private void indexValue(String value, String differentValue) throws IOException {
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
            case WILDCARD -> justType();
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
        String bulkBody = String.format(Locale.ROOT, """
            {"create":{"_index":"test"}}
            {"test":"%s","foo":1}
            """, value);
        // constant_keyword only accepts the single constant value; do not index differentValue for it.
        if (type != Type.CONSTANT_KEYWORD) {
            bulkBody += String.format(Locale.ROOT, """
                {"create":{"_index":"test"}}
                {"test":"%s","foo":1}
                """, differentValue);
        }
        bulk.setJsonEntity(bulkBody);
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

    static final Pattern TO_NAME = Pattern.compile("\\[.+", Pattern.DOTALL);

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
        Request request = new Request("PUT", "/_inference/text_embedding/test");
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
