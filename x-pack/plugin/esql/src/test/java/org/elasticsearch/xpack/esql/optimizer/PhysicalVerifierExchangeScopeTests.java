/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * The coordinator checks the node reduce stage it planned, because the data node splits it off without planning.
 */
public class PhysicalVerifierExchangeScopeTests extends ESTestCase {

    private final FieldAttribute a = field("a", DataType.LONG);
    private final FieldAttribute b = field("b", DataType.KEYWORD);

    public void testNodeExchangeBelowClusterExchange() {
        FragmentExec fragment = fragment(List.of(a, b), List.of(a, b));
        PhysicalPlan plan = cluster(node(fragment.output(), fragment));
        assertFalse(verify(plan).toString(), verify(plan).hasFailures());
    }

    public void testNodeExchangeWithoutClusterExchange() {
        FragmentExec fragment = fragment(List.of(a, b), List.of(a, b));
        PhysicalPlan plan = new ProjectExec(Source.EMPTY, node(fragment.output(), fragment), List.of(a, b));
        assertThat(verify(plan).toString(), containsString("a NODE exchange must be below a CLUSTER exchange"));
    }

    public void testNodeExchangeOutputDiffersFromFragment() {
        FragmentExec fragment = fragment(List.of(a, b), List.of(a, b));
        PhysicalPlan plan = cluster(node(List.of(a), fragment));
        assertThat(verify(plan).toString(), containsString("does not match its fragment output"));
    }

    public void testNodeExchangeOverNonFragment() {
        FragmentExec fragment = fragment(List.of(a, b), List.of(a, b));
        PhysicalPlan plan = cluster(node(List.of(a, b), new ProjectExec(Source.EMPTY, fragment, List.of(a, b))));
        assertThat(verify(plan).toString(), containsString("a NODE exchange must wrap a fragment"));
    }

    /** An inconsistent fragment fails on the coordinator rather than after it was sent to the data nodes. */
    public void testInconsistentFragment() {
        // the project reads b, which the relation does not produce
        FragmentExec fragment = fragment(List.of(a), List.of(a, b));
        PhysicalPlan plan = cluster(node(fragment.output(), fragment));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> verify(plan));
        assertThat(e.getMessage(), containsString("missing references"));
    }

    public void testDocCannotCrossClusterExchange() {
        Attribute doc = new FieldAttribute(Source.EMPTY, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        FragmentExec fragment = fragment(List.of(doc, a), List.of(doc, a));
        PhysicalPlan plan = new ExchangeExec(Source.EMPTY, fragment.output(), false, fragment);
        assertThat(verify(plan).toString(), containsString("document identity [_doc] cannot cross a cluster exchange"));
    }

    private static Failures verify(PhysicalPlan plan) {
        return PhysicalVerifier.INSTANCE.verify(plan, plan.output());
    }

    private static PhysicalPlan cluster(ExchangeExec node) {
        return new ExchangeExec(Source.EMPTY, node.output(), false, ExchangeExec.Scope.CLUSTER, node);
    }

    private static ExchangeExec node(List<Attribute> output, PhysicalPlan child) {
        return new ExchangeExec(Source.EMPTY, output, false, ExchangeExec.Scope.NODE, child);
    }

    private static FragmentExec fragment(List<Attribute> relationOutput, List<Attribute> projections) {
        EsRelation relation = new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), relationOutput);
        return new FragmentExec(new Project(Source.EMPTY, relation, projections));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
