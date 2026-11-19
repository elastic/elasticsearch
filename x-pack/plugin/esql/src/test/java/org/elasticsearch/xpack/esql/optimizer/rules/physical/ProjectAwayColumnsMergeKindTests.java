/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * {@link ProjectAwayColumns} rebuilds a {@link MergeExec} with a new output, so it must
 * copy {@link MergeExec#kind()} instead of defaulting the rewrite to UNION.
 */
public class ProjectAwayColumnsMergeKindTests extends ESTestCase {

    public void testForkKindSurvivesChildRewrite() {
        PhysicalPlan child = exchangeOverRelation();
        MergeExec merge = new MergeExec(Source.EMPTY, List.of(child, child), child.output(), MergeExec.Kind.FORK);

        PhysicalPlan rewritten = new ProjectAwayColumns().apply(merge);

        MergeExec result = as(rewritten, MergeExec.class);
        assertEquals(MergeExec.Kind.FORK, result.kind());
        assertNotSame(merge, result);
    }

    private static PhysicalPlan exchangeOverRelation() {
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "name",
                new EsField("name", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );
        EsRelation relation = new EsRelation(Source.EMPTY, "test", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), attributes);
        return new ExchangeExec(Source.EMPTY, new FragmentExec(relation));
    }
}
