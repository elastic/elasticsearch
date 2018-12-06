/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class QuotingTests extends ESTestCase {

    private static UnresolvedAttribute from(String s) {
        return new UnresolvedAttribute(Location.EMPTY, s);
    }

    public void testBasicString() {
        String s = "someField";
        UnresolvedAttribute ua = from(s);
        assertThat(ua.name(), equalTo(s));
        assertThat(ua.qualifiedName(), equalTo(s));
        assertThat(ua.qualifier(), nullValue());
    }

    public void testSingleQuoteLiteral() {
        String name = "@timestamp";
        Expression exp = new SqlParser().createExpression("'" + name + "'");
        assertThat(exp, instanceOf(Literal.class));
        Literal l = (Literal) exp;
        assertThat(l.value(), equalTo(name));
    }

    public void testMultiSingleQuotedLiteral() {
        String first = "bucket";
        String second = "head";
        Expression exp = new SqlParser().createExpression(String.format(Locale.ROOT, "'%s' '%s'", first, second));
        assertThat(exp, instanceOf(Literal.class));
        Literal l = (Literal) exp;
        assertThat(l.value(), equalTo(first + second));
    }

    public void testQuotedAttribute() {
        String quote = "\"";
        String name = "@timestamp";
        Expression exp = new SqlParser().createExpression(quote + name + quote);
        assertThat(exp, instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute ua = (UnresolvedAttribute) exp;
        assertThat(ua.name(), equalTo(name));
        assertThat(ua.qualifiedName(), equalTo(name));
        assertThat(ua.qualifier(), nullValue());
    }

    public void testBackQuotedAttribute() {
        String quote = "`";
        String name = "@timestamp";
        ParsingException ex = expectThrows(ParsingException.class, () ->
            new SqlParser().createExpression(quote + name + quote));
        assertThat(ex.getMessage(), equalTo("line 1:1: backquoted identifiers not supported; please use double quotes instead"));
    }

    public void testQuotedAttributeAndQualifier() {
        String quote = "\"";
        String qualifier = "table";
        String name = "@timestamp";
        Expression exp = new SqlParser().createExpression(quote + qualifier + quote + "." + quote + name + quote);
        assertThat(exp, instanceOf(UnresolvedAttribute.class));
        UnresolvedAttribute ua = (UnresolvedAttribute) exp;
        assertThat(ua.name(), equalTo(qualifier + "." + name));
        assertThat(ua.qualifiedName(), equalTo(qualifier + "." + name));
        assertThat(ua.qualifier(), is(nullValue()));
    }


    public void testBackQuotedAttributeAndQualifier() {
        String quote = "`";
        String qualifier = "table";
        String name = "@timestamp";
        ParsingException ex = expectThrows(ParsingException.class, () ->
            new SqlParser().createExpression(quote + qualifier + quote + "." + quote + name + quote));
        assertThat(ex.getMessage(), equalTo("line 1:1: backquoted identifiers not supported; please use double quotes instead"));
    }

    public void testGreedyQuoting() {
        LogicalPlan plan = new SqlParser().createStatement("SELECT * FROM \"table\" ORDER BY \"field\"");
        final List<LogicalPlan> plans = new ArrayList<>();
        plan.forEachDown(plans::add);
        assertThat(plans, hasSize(4));
        assertThat(plans.get(1), instanceOf(OrderBy.class));
    }
}
