/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.CROSS_CLUSTER;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.DATE_MATH;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.INDEX_SELECTOR;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.WILDCARD_PATTERN;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.quote;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.without;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class LookupJoinParserTests extends AbstractStatementParserTests {

    public void testValidJoinPatternFieldJoin() {
        var basePattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinPattern = randomIndexPattern(without(WILDCARD_PATTERN), without(CROSS_CLUSTER), without(INDEX_SELECTOR));
        var numberOfOnFields = randomIntBetween(1, 5);
        List<String> existingIdentifiers = new ArrayList<>();
        StringBuilder onFields = new StringBuilder();
        for (var i = 0; i < numberOfOnFields; i++) {
            if (randomBoolean()) {
                onFields.append(" ");
            }
            String onField = randomValueOtherThanMany(existingIdentifiers::contains, () -> randomIdentifier());
            existingIdentifiers.add(onField);
            onFields.append(onField);
            if (randomBoolean()) {
                onFields.append(" ");
            }
            if (i < numberOfOnFields - 1) {
                onFields.append(", ");
            }

        }
        var plan = statement("FROM " + basePattern + " | LOOKUP JOIN " + joinPattern + " ON " + onFields);

        var join = as(plan, LookupJoin.class);
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(basePattern)));
        assertThat(as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(joinPattern)));

        assertThat(join.config().leftFields(), hasSize(numberOfOnFields));
        for (int i = 0; i < numberOfOnFields; i++) {
            assertThat(as(join.config().leftFields().get(i), UnresolvedAttribute.class).name(), equalTo(existingIdentifiers.get(i)));
        }
        assertThat(join.config().type().joinName(), equalTo("LEFT OUTER"));
    }

    /**
     * Verify that both in snapshot and in release build the feature is enabled and the parsing works
     * without checking for the capability
     */
    public void testExpressionJoinEnabled() {
        var plan = statement("FROM test | LOOKUP JOIN test2 ON left_field >= right_field");
        var join = as(plan, LookupJoin.class);
    }

    public void testValidJoinPatternExpressionJoin() {
        assumeTrue("LOOKUP JOIN requires corresponding capability", EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled());

        var basePattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinPattern = randomIndexPattern(without(WILDCARD_PATTERN), without(CROSS_CLUSTER), without(INDEX_SELECTOR));
        var numberOfExpressions = randomIntBetween(1, 5);

        var expressions = new ArrayList<Tuple<Tuple<String, String>, EsqlBinaryComparison.BinaryComparisonOperation>>();
        StringBuilder onExpressionString = new StringBuilder();

        for (var i = 0; i < numberOfExpressions; i++) {
            var left = randomIdentifier();
            var right = randomIdentifier();
            var op = randomBinaryComparisonOperation();
            expressions.add(new Tuple<>(new Tuple<>(left, right), op));

            onExpressionString.append(left);
            if (randomBoolean()) {
                onExpressionString.append(" ");
            }
            onExpressionString.append(op.symbol());
            if (randomBoolean()) {
                onExpressionString.append(" ");
            }
            onExpressionString.append(right);

            if (i < numberOfExpressions - 1) {
                onExpressionString.append(" AND ");
            }
        }

        String query = "FROM " + basePattern + " | LOOKUP JOIN " + joinPattern + " ON " + onExpressionString;
        var plan = statement(query);

        var join = as(plan, LookupJoin.class);
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(basePattern)));
        assertThat(as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(joinPattern)));

        var joinType = join.config().type();
        assertThat(joinType.joinName(), startsWith("LEFT OUTER"));

        List<Expression> actualExpressions = Predicates.splitAnd(join.config().joinOnConditions());
        assertThat(actualExpressions.size(), equalTo(numberOfExpressions));

        for (int i = 0; i < numberOfExpressions; i++) {
            var expected = expressions.get(i);
            var actual = actualExpressions.get(i);

            assertThat(actual, instanceOf(EsqlBinaryComparison.class));
            var actualComp = (EsqlBinaryComparison) actual;

            assertThat(((UnresolvedAttribute) actualComp.left()).name(), equalTo(expected.v1().v1()));
            assertThat(((UnresolvedAttribute) actualComp.right()).name(), equalTo(expected.v1().v2()));
            assertThat(actualComp.getFunctionType(), equalTo(expected.v2()));
        }
    }

    private EsqlBinaryComparison.BinaryComparisonOperation randomBinaryComparisonOperation() {
        return randomFrom(EsqlBinaryComparison.BinaryComparisonOperation.values());
    }

    public void testValidJoinPatternWithRemoteFieldJoin() {
        testValidJoinPatternWithRemote(randomIdentifier());
    }

    public void testValidJoinPatternWithRemoteExpressionJoin() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        testValidJoinPatternWithRemote(singleExpressionJoinClause());
    }

    private void testValidJoinPatternWithRemote(String onClause) {
        var fromPatterns = randomIndexPatterns(CROSS_CLUSTER);
        var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(INDEX_SELECTOR));
        var plan = statement("FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause);

        var join = as(plan, LookupJoin.class);
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(fromPatterns)));
        assertThat(as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(joinPattern)));
    }

    public void testInvalidJoinPatternsFieldJoin() {
        testInvalidJoinPatterns(randomIdentifier());
    }

    public void testInvalidJoinPatternsFieldJoinTwo() {
        testInvalidJoinPatterns(randomIdentifier() + ", " + randomIdentifier());
    }

    public void testInvalidJoinPatternsExpressionJoin() {
        testInvalidJoinPatterns(singleExpressionJoinClause());
    }

    public void testInvalidJoinPatternsExpressionJoinTwo() {
        testInvalidJoinPatterns(singleExpressionJoinClause() + " AND " + singleExpressionJoinClause());
    }

    public void testInvalidJoinPatternsExpressionJoinMix() {
        testInvalidJoinPatterns(randomIdentifier() + ", " + singleExpressionJoinClause());
    }

    public void testInvalidJoinPatternsExpressionJoinMixTwo() {
        testInvalidJoinPatterns(singleExpressionJoinClause() + " AND " + randomIdentifier());
    }

    public void testInvalidLookupJoinOnClause() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        expectError(
            "FROM test  | LOOKUP JOIN test2 ON " + randomIdentifier() + " , " + singleExpressionJoinClause(),
            "JOIN ON clause must be a comma separated list of fields or a single expression, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON " + singleExpressionJoinClause() + " , " + randomIdentifier(),
            "JOIN ON clause with expressions only supports a single expression, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON " + singleExpressionJoinClause() + " , " + singleExpressionJoinClause(),
            "JOIN ON clause with expressions only supports a single expression, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON " + singleExpressionJoinClause() + " AND " + randomIdentifier(),
            "JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON "
                + singleExpressionJoinClause()
                + " AND ("
                + randomIdentifier()
                + " OR "
                + singleExpressionJoinClause()
                + ")",
            "JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON "
                + singleExpressionJoinClause()
                + " AND ("
                + randomIdentifier()
                + "OR"
                + randomIdentifier()
                + ")",
            "JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON " + randomIdentifier() + " AND " + randomIdentifier(),
            "JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found"
        );

        expectError(
            "FROM test  | LOOKUP JOIN test2 ON " + randomIdentifier() + " AND " + singleExpressionJoinClause(),
            "JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found "
        );
    }

    private String singleExpressionJoinClause() {
        var left = randomIdentifier();
        var right = randomValueOtherThan(left, ESTestCase::randomIdentifier);
        var op = randomBinaryComparisonOperation();
        return left + (randomBoolean() ? " " : "") + op.symbol() + (randomBoolean() ? " " : "") + right;
    }

    private void testInvalidJoinPatterns(String onClause) {
        {
            // wildcard
            var joinPattern = randomIndexPattern(WILDCARD_PATTERN, without(CROSS_CLUSTER), without(INDEX_SELECTOR));
            expectError(
                "FROM " + randomIndexPatterns() + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                "invalid index pattern [" + unquoteIndexPattern(joinPattern) + "], * is not allowed in LOOKUP JOIN"
            );
        }
        {
            // remote cluster on the right
            var fromPatterns = randomIndexPatterns(without(CROSS_CLUSTER));
            var joinPattern = randomIndexPattern(CROSS_CLUSTER, without(WILDCARD_PATTERN), without(INDEX_SELECTOR));
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                "invalid index pattern [" + unquoteIndexPattern(joinPattern) + "], remote clusters are not supported with LOOKUP JOIN"
            );
        }

        {
            // Generate a syntactically invalid (partial quoted) pattern.
            var fromPatterns = quote(randomIdentifier()) + ":" + unquoteIndexPattern(randomIndexPattern(without(CROSS_CLUSTER)));
            var joinPattern = randomIndexPattern();
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                // Since the from pattern is partially quoted, we get an error at the end of the partially quoted string.
                " mismatched input ':'"
            );
        }

        {
            // Generate a syntactically invalid (partial quoted) pattern.
            var fromPatterns = randomIdentifier() + ":" + quote(randomIndexPatterns(without(CROSS_CLUSTER)));
            var joinPattern = randomIndexPattern();
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                // Since the from pattern is partially quoted, we get an error at the beginning of the partially quoted
                // index name that we're expecting an unquoted string.
                "expecting UNQUOTED_SOURCE"
            );
        }

        {
            var fromPatterns = randomIndexPattern();
            // Generate a syntactically invalid (partial quoted) pattern.
            var joinPattern = quote(randomIdentifier()) + ":" + unquoteIndexPattern(randomIndexPattern(without(CROSS_CLUSTER)));
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                // Since the join pattern is partially quoted, we get an error at the end of the partially quoted string.
                "mismatched input ':'"
            );
        }

        {
            var fromPatterns = randomIndexPattern();
            // Generate a syntactically invalid (partially quoted) pattern.
            var joinPattern = randomIdentifier() + ":" + quote(randomIndexPattern(without(CROSS_CLUSTER)));
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                // Since the from pattern is partially quoted, we get an error at the beginning of the partially quoted
                // index name that we're expecting an unquoted string.
                "no viable alternative at input"
            );
        }

        if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
            {
                // Selectors are not yet supported in join patterns on the right.
                // Unquoted case: The language specification does not allow mixing `:` and `::` characters in an index expression
                var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(DATE_MATH), INDEX_SELECTOR);
                // We do different validation based on the quotation of the pattern, so forcefully unquote the expression instead of leaving
                // it to chance.
                joinPattern = unquoteIndexPattern(joinPattern);
                expectError(
                    "FROM " + randomIndexPatterns(without(CROSS_CLUSTER)) + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                    "no viable alternative at input "
                );
            }
            {
                // Selectors are not yet supported in join patterns on the right.
                // Quoted case: The language specification allows `::` characters in a quoted expression, but this usage
                // must cause a validation exception in the non-generated code.
                var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(DATE_MATH), INDEX_SELECTOR);
                // We do different validation based on the quotation of the pattern, so forcefully quote the expression instead of leaving
                // it to chance.
                joinPattern = "\"" + unquoteIndexPattern(joinPattern) + "\"";
                expectError(
                    "FROM " + randomIndexPatterns(without(CROSS_CLUSTER)) + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                    "invalid index pattern ["
                        + unquoteIndexPattern(joinPattern)
                        + "], index pattern selectors are not supported in LOOKUP JOIN"
                );
            }

            {
                // Although we don't support selector strings for remote indices, it's alright.
                // The parser error message takes precedence.
                var fromPatterns = randomIndexPatterns();
                var joinPattern = quote(randomIdentifier()) + "::" + randomFrom("data", "failures");
                // After the end of the partially quoted string, i.e. the index name, parser now expects "ON..." and not a selector string.
                expectError(
                    "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause,
                    "mismatched input ':' expecting 'on'"
                );
            }

            {
                // Although we don't support selector strings for remote indices, it's alright.
                // The parser error message takes precedence.
                var fromPatterns = randomIndexPatterns();
                var joinPattern = randomIdentifier() + "::" + quote(randomFrom("data", "failures"));
                // After the index name and "::", parser expects an unquoted string, i.e. the selector string should not be
                // partially quoted.
                expectError("FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + onClause, "no viable alternative at input");
            }
        }
    }

    public void testLookupJoinOnExpressionWithNamedQueryParameters() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );

        // Test LOOKUP JOIN ON expression with named query parameters and MATCH function
        var plan = statement(
            "FROM test | LOOKUP JOIN test2 ON left_field >= right_field AND match(left_field, ?search_term)",
            new QueryParams(List.of(paramAsConstant("search_term", "elasticsearch")))
        );

        var join = as(plan, LookupJoin.class);
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("test"));
        assertThat(as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("test2"));

        // Verify the join condition contains both the comparison and MATCH function
        var condition = join.config().joinOnConditions();
        assertThat(condition, instanceOf(And.class));
        var andCondition = (And) condition;

        // Check that we have both conditions in the correct order
        assertThat(andCondition.children().size(), equalTo(2));

        // First child should be a binary comparison (left_field >= right_field)
        var firstChild = andCondition.children().get(0);
        assertThat("First condition should be binary comparison", firstChild, instanceOf(EsqlBinaryComparison.class));

        // Second child should be a MATCH function (match(left_field, ?search_term))
        var secondChild = andCondition.children().get(1);
        assertThat("Second condition should be UnresolvedFunction", secondChild, instanceOf(UnresolvedFunction.class));
        var function = (UnresolvedFunction) secondChild;
        assertThat("Second condition should be MATCH function", function.name(), equalTo("match"));
    }

    public void testLookupJoinOnExpressionWithPositionalQueryParameters() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );

        // Test LOOKUP JOIN ON expression with positional query parameters and MATCH function
        var plan = statement(
            "FROM test | LOOKUP JOIN test2 ON left_field >= right_field AND match(left_field, ?2)",
            new QueryParams(List.of(paramAsConstant(null, "dummy"), paramAsConstant(null, "elasticsearch")))
        );

        var join = as(plan, LookupJoin.class);
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("test"));
        assertThat(as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("test2"));

        // Verify the join condition contains both the comparison and MATCH function
        var condition = join.config().joinOnConditions();
        assertThat(condition, instanceOf(And.class));
        var andCondition = (And) condition;

        // Check that we have both conditions in the correct order
        assertThat(andCondition.children().size(), equalTo(2));

        // First child should be a binary comparison (left_field >= right_field)
        var firstChild = andCondition.children().get(0);
        assertThat("First condition should be binary comparison", firstChild, instanceOf(EsqlBinaryComparison.class));

        // Second child should be a MATCH function (match(left_field, ?))
        var secondChild = andCondition.children().get(1);
        assertThat("Second condition should be UnresolvedFunction", secondChild, instanceOf(UnresolvedFunction.class));
        var function = (UnresolvedFunction) secondChild;
        assertThat("Second condition should be MATCH function", function.name(), equalTo("match"));
        assertEquals(2, function.children().size());
        assertEquals("elasticsearch", function.children().get(1).toString());
    }

    public void testJoinOnConstant() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        expectError(
            "row languages = 1, gender = \"f\" | lookup join test on 123",
            "1:55: JOIN ON clause must be a comma separated list of fields or a single expression, found [123]"
        );
        expectError(
            "row languages = 1, gender = \"f\" | lookup join test on \"abc\"",
            "1:55: JOIN ON clause must be a comma separated list of fields or a single expression, found [\"abc\"]"
        );
        expectError(
            "row languages = 1, gender = \"f\" | lookup join test on false",
            "1:55: JOIN ON clause must be a comma separated list of fields or a single expression, found [false]"
        );
    }

    public void testLookupJoinExpressionMixed() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON languages_left == language_code or salary > 1000
            """;

        expectError(
            queryString,
            "3:32: JOIN ON clause with expressions must contain at least one condition relating the left index and the lookup index"
        );
    }

    public void testLookupJoinExpressionOnlyRightFilter() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON salary > 1000
            """;

        expectError(
            queryString,
            "3:32: JOIN ON clause with expressions must contain at least one condition relating the left index and the lookup index"
        );
    }

    public void testLookupJoinExpressionFieldBasePlusRightFilterAnd() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | lookup join languages_lookup ON languages and salary > 1000
            """;

        expectError(queryString, "2:32: JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found [languages]");
    }

    public void testLookupJoinExpressionFieldBasePlusRightFilterComma() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | lookup join languages_lookup ON languages, salary > 1000
            """;

        expectError(
            queryString,
            "2:46: JOIN ON clause must be a comma separated list of fields or a single expression, found [salary > 1000]"
        );
    }

    public void testJoinTwiceOnTheSameField() {
        expectError(
            "row languages = 1, gender = \"f\" | lookup join test on languages, languages",
            "1:66: JOIN ON clause does not support multiple fields with the same name, found multiple instances of [languages]"
        );
    }

    public void testJoinTwiceOnTheSameField_TwoLookups() {
        expectError(
            "row languages = 1, gender = \"f\" | lookup join test on languages | eval x = 1 | lookup join test on gender, gender",
            "1:108: JOIN ON clause does not support multiple fields with the same name, found multiple instances of [gender]"
        );
    }
}
