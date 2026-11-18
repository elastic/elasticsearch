/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexSyntax;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * With the {@code regex_syntax} query setting at {@code re2} {@code RLIKE} reads its patterns in RE2 syntax; by default it
 * reads them in Lucene's.
 */
public class ApplyRegexSyntaxTests extends ESTestCase {

    public void testRe2() {
        Re2Like re2Like = as(condition(RegexSyntax.RE2, "WHERE first_name RLIKE \"(?i)geor.*\""), Re2Like.class);
        assertThat(re2Like.pattern().pattern(), equalTo("(?i)geor.*"));
    }

    public void testLucene() {
        as(condition(RegexSyntax.LUCENE, "WHERE first_name RLIKE \"(?i)geor.*\""), RLike.class);
        as(condition(RegexSyntax.LUCENE, "WHERE first_name RLIKE (\"a.*\", \"b.*\")"), RLikeList.class);
    }

    public void testRe2Not() {
        Not not = as(condition(RegexSyntax.RE2, "WHERE first_name NOT RLIKE \"(?i)a.*\""), Not.class);
        as(not.field(), Re2Like.class);
    }

    /** A list of RE2 patterns matches any of them. */
    public void testRe2List() {
        Or or = as(condition(RegexSyntax.RE2, "WHERE first_name RLIKE (\"(?i)a.*\", \"\\\\pL{3}\")"), Or.class);
        assertThat(as(or.left(), Re2Like.class).pattern().pattern(), equalTo("(?i)a.*"));
        assertThat(as(or.right(), Re2Like.class).pattern().pattern(), equalTo("\\pL{3}"));
    }

    public void testInvalidSyntax() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> analyzer().addEmployees().statement("SET regex_syntax=\"perl\"; FROM employees | WHERE first_name RLIKE \"a.*\"")
        );
        assertThat(e.getMessage(), containsString("Invalid regex_syntax [perl], must be one of [lucene, re2]"));
    }

    private static Expression condition(RegexSyntax syntax, String where) {
        Configuration configuration = new ConfigurationBuilder(TEST_CFG).setting(QuerySettings.REGEX_SYNTAX, syntax).build();
        LogicalPlan plan = analyzer().configuration(configuration).addEmployees().query("FROM employees | " + where + " | LIMIT 10");
        return as(plan.collect(Filter.class::isInstance).getFirst(), Filter.class).condition();
    }
}
