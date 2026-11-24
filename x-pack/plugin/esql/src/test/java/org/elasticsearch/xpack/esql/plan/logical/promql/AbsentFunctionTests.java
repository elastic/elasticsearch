/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests.randomEsRelation;
import static org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests.randomLocalRelation;
import static org.hamcrest.Matchers.contains;

/**
 * Needed to override the reflective tests in {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} because the
 * public constructor of {@link AbsentFunction} mints its label attributes (with new {@code NameId}s) from the child. Copies
 * and mutations go through the node's own lifecycle ({@code replaceChild}), which threads the labels through unchanged.
 * <p>
 * Provides {@link #randomAbsentFunction()} for {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} to delegate to
 * when it needs to build this node (directly or as a nested child). Also covers the Prometheus label derivation rules
 * ({@code createLabelsForAbsentFunction}) in {@link AbsentFunction#absentLabels}.
 */
public class AbsentFunctionTests extends AbstractNodeTestCase<AbsentFunction, LogicalPlan> {

    public static AbsentFunction randomAbsentFunction() {
        return new AbsentFunction(randomSource(), randomChildWithOutput(), PromqlBuiltinFunctionDefinitions.ABSENT, List.of());
    }

    /**
     * Enclosing nodes (e.g. {@code ResolvingProject}) call {@code output()} at construction, which a generic random child such
     * as a {@code Lookup} with an unresolved table cannot always compute, so the child is restricted to relations.
     */
    private static LogicalPlan randomChildWithOutput() {
        return randomBoolean() ? randomEsRelation() : randomLocalRelation();
    }

    @Override
    protected AbsentFunction randomInstance() {
        return randomAbsentFunction();
    }

    @Override
    protected AbsentFunction mutate(AbsentFunction instance) {
        return instance.replaceChild(randomValueOtherThan(instance.child(), AbsentFunctionTests::randomChildWithOutput));
    }

    @Override
    protected AbsentFunction copy(AbsentFunction instance) {
        return instance.replaceChild(instance.child());
    }

    @Override
    public void testTransform() {
        AbsentFunction node = new AbsentFunction(
            randomSource(),
            selector("x{job=\"a\"}"),
            PromqlBuiltinFunctionDefinitions.ABSENT,
            List.of()
        );
        AbsentFunction transformed = (AbsentFunction) node.transformPropertiesOnly(Object.class, p -> p);
        assertEquals(node, transformed);
        assertEquals(node.output(), transformed.output());
    }

    @Override
    public void testReplaceChildren() {
        AbsentFunction node = new AbsentFunction(
            randomSource(),
            selector("x{job=\"a\"}"),
            PromqlBuiltinFunctionDefinitions.ABSENT,
            List.of()
        );
        LogicalPlan newChild = randomChildWithOutput();

        AbsentFunction replaced = node.replaceChild(newChild);
        assertEquals(node.source(), replaced.source());
        assertEquals(newChild, replaced.child());
        assertEquals(node.definition(), replaced.definition());
        assertEquals(node.output(), replaced.output());
    }

    public void testOutputIsDerivedLabels() {
        AbsentFunction node = new AbsentFunction(
            Source.EMPTY,
            selector("x{job=\"a\", instance=\"b\"}"),
            PromqlBuiltinFunctionDefinitions.ABSENT,
            List.of()
        );
        assertThat(node.output().stream().map(a -> a.name()).toList(), contains("job", "instance"));
    }

    public void testSingleEqualityMatchers() {
        assertEquals(Map.of("job", "a", "foo", "bar"), labels("x{job=\"a\", foo=\"bar\"}"));
    }

    public void testRepeatedEqualityDropsLabel() {
        assertEquals(Map.of("foo", "bar"), labels("x{job=\"a\", job=\"b\", foo=\"bar\"}"));
        assertEquals(Map.of(), labels("x{job=\"a\", job=\"a\"}"));
    }

    public void testNonEqualityMatcherDropsLabel() {
        assertEquals(Map.of("foo", "bar"), labels("x{job=\"a\", job=~\"b\", foo=\"bar\"}"));
        assertEquals(Map.of(), labels("x{job!=\"a\"}"));
        assertEquals(Map.of(), labels("x{job!~\"a\"}"));
    }

    public void testEqualityAfterNonEqualityKeepsLabel() {
        assertEquals(Map.of("job", "a"), labels("x{job=~\"b\", job=\"a\"}"));
    }

    public void testEmptyValueSetsNoLabel() {
        assertEquals(Map.of(), labels("x{job=\"\"}"));
        assertEquals(Map.of(), labels("x{job=\"\", job=\"a\"}"));
    }

    public void testMetadataLabelsAreSkipped() {
        assertEquals(Map.of(), labels("x"));
        assertEquals(Map.of("job", "a"), labels("{__name__=\"x\", job=\"a\"}"));
        assertEquals(Map.of("job", "a"), labels("x{__type__=\"counter\", __unit__=\"seconds\", job=\"a\"}"));
        assertEquals(
            Map.of("job", "a"),
            AbsentFunction.absentLabels(withMatchers(new LabelMatcher("labels.__name__", "x", LabelMatcher.Matcher.EQ), eq("job", "a")))
        );
    }

    public void testPassthroughPrefixIsCanonicalized() {
        assertEquals(Map.of("job", "a"), AbsentFunction.absentLabels(withMatchers(eq("labels.job", "a"))));
        assertEquals(Map.of(), AbsentFunction.absentLabels(withMatchers(eq("labels.job", "a"), eq("job", "b"))));
    }

    public void testLabelsFollowMatcherOrder() {
        assertThat(List.copyOf(labels("x{c=\"1\", a=\"2\", b=\"3\"}").keySet()), contains("c", "a", "b"));
    }

    public void testOffsetSelectorKeepsLabels() {
        assertEquals(Map.of("job", "a"), labels("x{job=\"a\"} offset 5m"));
    }

    public void testParenthesizedSelectorKeepsLabels() {
        assertEquals(Map.of("job", "a"), labels("(x{job=\"a\"})"));
    }

    public void testNonSelectorArgumentHasNoLabels() {
        assertEquals(Map.of(), labels("sum(x{job=\"a\"})"));
        assertEquals(Map.of(), labels("rate(x{job=\"a\"}[5m])"));
        assertEquals(Map.of(), labels("x{job=\"a\"} > 1"));
        assertEquals(Map.of(), labels("x{job=\"a\"}[5m]"));
    }

    public void testMultiValuedEqualityDropsLabel() {
        LabelMatcher multi = new LabelMatcher("host", List.of("a", "b"), LabelMatcher.Matcher.EQ);
        assertEquals(Map.of("job", "a"), AbsentFunction.absentLabels(withMatchers(multi, eq("job", "a"))));
        assertEquals(Map.of(), AbsentFunction.absentLabels(withMatchers(eq("host", "a"), multi)));
        assertEquals(Map.of("host", "a"), AbsentFunction.absentLabels(withMatchers(multi, eq("host", "a"))));
    }

    private static Map<String, String> labels(String expression) {
        return AbsentFunction.absentLabels(parse(expression));
    }

    private static LogicalPlan parse(String expression) {
        return new PromqlParser().createStatement(expression);
    }

    private static InstantSelector selector(String expression) {
        return (InstantSelector) parse(expression);
    }

    private static LabelMatcher eq(String name, String value) {
        return new LabelMatcher(name, value, LabelMatcher.Matcher.EQ);
    }

    /** A selector for metric {@code x} whose non-name matchers are replaced by {@code matchers}. */
    private static InstantSelector withMatchers(LabelMatcher... matchers) {
        InstantSelector base = selector("x");
        List<LabelMatcher> all = new ArrayList<>(base.labelMatchers().matchers());
        all.addAll(List.of(matchers));
        return new InstantSelector(base.source(), base.series(), base.labels(), new LabelMatchers(all), base.evaluation());
    }
}
