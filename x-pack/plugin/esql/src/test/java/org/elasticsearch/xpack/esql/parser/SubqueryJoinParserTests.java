/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.IndependentSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SubqueryJoinParserTests extends AbstractStatementParserTests {

    public void testSubqueryTargetGrammar() {
        EsqlConfig config = new EsqlConfig(TEST_FUNCTION_REGISTRY);
        assumeTrue("Requires a snapshot build", config.isDevVersion());
        for (String subquery : List.of(
            "FROM right | KEEP key",
            "ROW key = 1 | KEEP key",
            "TS metrics | KEEP key",
            "FROM (FROM nested) | KEEP key",
            "FROM right /* nested comment */ | WHERE key == ?_key | KEEP key",
            "FROM right | LOOKUP JOIN nested ON key"
        )) {
            assertGrammarAccepts("FROM left | LEFT OUTER JOIN (" + subquery + ") ON key", config);
            assertGrammarAccepts("FROM left | INNER JOIN (" + subquery + ") ON key", config);
        }
        for (String target : List.of("right", "right*", "\"right\"", "remote:right")) {
            assertGrammarAccepts("FROM left | LEFT OUTER JOIN " + target + " ON key", config);
            assertGrammarAccepts("FROM left | INNER JOIN " + target + " ON key", config);
        }
    }

    public void testIndexPatternTargetIsShorthandForFromSubquery() {
        assumeTrue("requires JOIN subquery capability", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        for (String joinType : List.of("LEFT OUTER JOIN", "INNER JOIN")) {
            for (String target : List.of("sub", "sub*", "remote:sub", "\"sub\"")) {
                var join = as(query("FROM main | " + joinType + " " + target + " ON key"), SubqueryJoin.class);
                assertThat(join, instanceOf(IndependentSubqueryJoin.class));
                assertThat(join.config().type().joinName(), equalTo(joinType.replace(" JOIN", "")));
                var right = as(join.right(), UnresolvedRelation.class);
                var fromSubquery = as(
                    as(query("FROM main | " + joinType + " (FROM " + target + ") ON key"), SubqueryJoin.class).right(),
                    UnresolvedRelation.class
                );
                assertThat(right.indexPattern().indexPattern(), equalTo(target.replace("\"", "")));
                assertThat(right.indexPattern().indexPattern(), equalTo(fromSubquery.indexPattern().indexPattern()));
                assertThat(right.indexMode(), equalTo(fromSubquery.indexMode()));
                assertThat(right.metadataFields(), equalTo(fromSubquery.metadataFields()));
                assertThat(right.telemetryLabel(), equalTo(SourceCommand.FROM.name()));
            }
        }
    }

    public void testSubqueryJoinRejectsLookupSyntax() {
        expectError("FROM main | LOOKUP JOIN (FROM sub) ON key", "mismatched input '('");
    }

    public void testIndexPatternTargetRejectsExpressionOn() {
        expectError("FROM main | INNER JOIN sub ON left_key == right_key", "JOIN on a subquery only supports a list of fields in ON");
    }

    public void testSubqueryTargetBuildsIndependentJoin() {
        assumeTrue("requires JOIN subquery capability", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        var join = as(query("FROM main | LEFT OUTER JOIN (FROM sub | KEEP key, value) ON key"), SubqueryJoin.class);
        assertThat(join, instanceOf(IndependentSubqueryJoin.class));
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("main"));
        assertThat(as(as(join.right(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("sub"));
        assertThat(join.config().type().joinName(), equalTo("LEFT OUTER"));
        assertThat(join.config().leftFields(), hasSize(1));
        assertThat(((UnresolvedAttribute) join.config().leftFields().getFirst()).name(), equalTo("key"));
        assertThat(join.config().rightFields(), equalTo(join.config().leftFields()));
        assertNull(join.config().joinOnConditions());
    }

    public void testMultipleJoinKeys() {
        assumeTrue("requires JOIN subquery capability", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        for (String target : List.of("(FROM sub | KEEP a, b, value)", "sub")) {
            for (String joinType : List.of("LEFT OUTER JOIN", "INNER JOIN")) {
                var join = as(query("FROM main | " + joinType + " " + target + " ON a, b"), SubqueryJoin.class);
                List<String> keys = join.config().leftFields().stream().map(f -> ((UnresolvedAttribute) f).name()).toList();
                assertThat(keys, equalTo(List.of("a", "b")));
                assertThat(join.config().rightFields(), equalTo(join.config().leftFields()));
            }
        }
    }

    public void testInnerSubqueryJoinBuildsInnerJoin() {
        assumeTrue("requires JOIN subquery capability", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());

        var join = as(query("FROM main | INNER JOIN (FROM sub | KEEP key, value) ON key"), SubqueryJoin.class);
        assertThat(join.config().type(), equalTo(JoinTypes.INNER));
    }

    public void testSubqueryTargetRejectsExpressionOn() {
        expectError(
            "FROM main | LEFT OUTER JOIN (FROM sub) ON left_key == right_key",
            "JOIN on a subquery only supports a list of fields in ON"
        );
    }

    private void assertGrammarAccepts(String query, EsqlConfig config) {
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));
        lexer.setEsqlConfig(config);
        EsqlBaseParser parser = new EsqlBaseParser(new CommonTokenStream(lexer));
        parser.setEsqlConfig(config);
        parser.removeErrorListeners();
        parser.setErrorHandler(new BailErrorStrategy());
        parser.singleStatement();
        assertEquals(0, parser.getNumberOfSyntaxErrors());
    }
}
