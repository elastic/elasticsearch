/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.AnyParam;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.junit.BeforeClass;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A {@code STATS ... BY ??g} key whose parameter stands for "Any" is dropped, as no grouping is what "any group" means, as long
 * as the command keeps something else. These tests are about what is left of the {@code BY}, and not about what the
 * analysis makes of what the parser keeps for it to report.
 */
public class AnyParamGroupingParserTests extends ESTestCase {

    private static final String ANY = QuerySettings.PARAM_ANY_VALUE_DEFAULT;

    @BeforeClass
    public static void requireAnyParam() {
        assumeTrue("requires the Any controls", EsqlCapabilities.Cap.PARAM_ANY_VALUE.isEnabled());
    }

    private static QueryParams params(QueryParam... params) {
        return new QueryParams(List.of(params));
    }

    private static Aggregate aggregate(String query, QueryParam... params) {
        return as(TEST_PARSER.parseQuery(query, params(params)), Aggregate.class);
    }

    private static List<String> groupingNames(Aggregate aggregate) {
        return aggregate.groupings().stream().map(g -> ((NamedExpression) g).name()).toList();
    }

    private static List<String> aggregateNames(Aggregate aggregate) {
        return aggregate.aggregates().stream().map(NamedExpression::name).toList();
    }

    private static List<AnyParam> keptMarkers(Aggregate aggregate) {
        return aggregate.groupings().stream().map(Alias.class::cast).map(a -> a.child()).map(AnyParam.class::cast).toList();
    }

    public void testKeyIsDroppedWhenThereIsAnAggregate() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY ??g", paramAsConstant("g", ANY));
        assertThat(aggregate.groupings(), empty());
        assertThat(aggregateNames(aggregate), contains("c"));
    }

    public void testIdentifierParameterKeyIsDropped() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY ?g", paramAsIdentifier("g", ANY));
        assertThat(aggregate.groupings(), empty());
        assertThat(aggregateNames(aggregate), contains("c"));
    }

    public void testKeyIsDroppedWhenThereIsAnotherKey() {
        Aggregate aggregate = aggregate("FROM t | STATS BY ??g, a", paramAsConstant("g", ANY));
        assertThat(groupingNames(aggregate), contains("a"));
        assertThat(aggregateNames(aggregate), contains("a"));
        aggregate = aggregate("FROM t | STATS BY a, ??g", paramAsConstant("g", ANY));
        assertThat(groupingNames(aggregate), contains("a"));
    }

    public void testOtherKeysAreKept() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY b = BUCKET(a, 10), ??g, d", paramAsConstant("g", ANY));
        assertThat(groupingNames(aggregate), contains("b", "d"));
        assertThat(aggregateNames(aggregate), contains("c", "b", "d"));
    }

    public void testEveryAnyKeyIsDroppedWhenThereIsAnAggregate() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY ??g, ??h", paramAsConstant("g", ANY), paramAsConstant("h", ANY));
        assertThat(aggregate.groupings(), empty());
    }

    public void testKeyIsKeptWhenItIsTheOnlyThing() {
        Aggregate aggregate = aggregate("FROM t | STATS BY ??g", paramAsConstant("g", ANY));
        List<AnyParam> markers = keptMarkers(aggregate);
        assertThat(markers.stream().map(AnyParam::parameterText).toList(), contains("??g"));
        assertThat(markers.getFirst().unresolvedMessage(), containsString("would have no aggregate and no other key without it"));
    }

    public void testKeysAreKeptWhenTheyAreAllThatIsThere() {
        Aggregate aggregate = aggregate("FROM t | STATS BY ??g, ??h", paramAsConstant("g", ANY), paramAsConstant("h", ANY));
        assertThat(keptMarkers(aggregate).stream().map(AnyParam::parameterText).toList(), contains("??g", "??h"));
    }

    public void testNamedKeyIsKept() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY k = ??g", paramAsConstant("g", ANY));
        assertThat(groupingNames(aggregate), contains("k"));
        List<AnyParam> markers = keptMarkers(aggregate);
        assertThat(markers.stream().map(AnyParam::parameterText).toList(), contains("??g"));
        assertThat(markers.getFirst().unresolvedMessage(), containsString("in a named BY key, as only an unnamed key can be dropped"));
    }

    public void testKeyThatStandsForAValueIsKept() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY ?g", paramAsConstant("g", ANY));
        List<AnyParam> markers = keptMarkers(aggregate);
        assertThat(markers.stream().map(AnyParam::parameterText).toList(), contains("?g"));
        assertThat(markers.getFirst().field(), equalTo(false));
        assertThat(markers.getFirst().unresolvedMessage(), containsString("as a BY key that is a value"));
    }

    public void testKeyThatIsNotAnyIsKept() {
        Aggregate aggregate = aggregate("FROM t | STATS c = COUNT(*) BY ??g", paramAsConstant("g", "other"));
        assertThat(groupingNames(aggregate), contains("other"));
        Expression key = aggregate.groupings().getFirst();
        assertThat(key, instanceOf(UnresolvedAttribute.class));
    }

    public void testInlineStatsKeyIsDropped() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM t | INLINE STATS c = COUNT(*) BY ??g", params(paramAsConstant("g", ANY)));
        Aggregate aggregate = as(as(plan, InlineStats.class).child(), Aggregate.class);
        assertThat(aggregate.groupings(), empty());
        assertThat(aggregateNames(aggregate), contains("c"));
    }

    public void testInlineStatsNamedKeyIsKept() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM t | INLINE STATS c = COUNT(*) BY k = ??g", params(paramAsConstant("g", ANY)));
        Aggregate aggregate = as(as(plan, InlineStats.class).child(), Aggregate.class);
        assertThat(groupingNames(aggregate), contains("k"));
        assertThat(keptMarkers(aggregate).stream().map(AnyParam::parameterText).toList(), contains("??g"));
    }

    public void testTimeSeriesKeyIsDropped() {
        LogicalPlan plan = TEST_PARSER.parseQuery(
            "TS t | STATS c = COUNT(*) BY b = TBUCKET(1h), ??g",
            params(paramAsConstant("g", ANY))
        );
        Aggregate aggregate = as(plan, TimeSeriesAggregate.class);
        assertThat(groupingNames(aggregate), contains("b"));
    }
}
