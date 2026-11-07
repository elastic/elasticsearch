/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.fuse;

import org.elasticsearch.compute.operator.fuse.RrfConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;

/**
 * Equality tests for {@link FuseScoreEval}.
 *
 * <p>{@code equals} must take every field into account: the logical plan optimizer decides whether a
 * rule changed anything by comparing the plan before and after the rule ran ({@code RuleExecutor}),
 * so a field that {@code equals} ignores makes a rewrite of that field invisible. {@code hashCode}
 * already hashes all of them, so an under-comparing {@code equals} would additionally break the
 * equals/hashCode contract.
 *
 * <p>Note the shared {@link #child} below: {@code EsqlTestUtils.relation()} builds a relation over a
 * random index name, so two separate calls are not equal to each other. Every instance built here
 * therefore reuses one child, leaving the field under test as the only difference.
 */
public class FuseScoreEvalTests extends ESTestCase {

    private final LogicalPlan child = relation();
    private final Attribute score = referenceAttribute("_score", DataType.DOUBLE);
    private final Attribute discriminator = referenceAttribute("_fork", DataType.KEYWORD);

    private static MapExpression options(double rankConstant) {
        return new MapExpression(Source.EMPTY, List.of(of(RrfConfig.RANK_CONSTANT), of(rankConstant)));
    }

    private FuseScoreEval fuse(Attribute scoreAttr, Attribute discriminatorAttr, Fuse.FuseType type, MapExpression options) {
        return new FuseScoreEval(Source.EMPTY, child, scoreAttr, discriminatorAttr, type, options);
    }

    public void testEqualsForIdenticalFields() {
        FuseScoreEval a = fuse(score, discriminator, Fuse.FuseType.RRF, options(60));
        FuseScoreEval b = fuse(score, discriminator, Fuse.FuseType.RRF, options(60));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * The discriminator used to be compared against {@code this} rather than against the other
     * instance, which made the clause unconditionally true.
     */
    public void testDiscriminatorIsCompared() {
        Attribute otherDiscriminator = referenceAttribute("_other_fork", DataType.KEYWORD);
        assertNotEquals(
            fuse(score, discriminator, Fuse.FuseType.RRF, options(60)),
            fuse(score, otherDiscriminator, Fuse.FuseType.RRF, options(60))
        );
    }

    public void testScoreIsCompared() {
        Attribute otherScore = referenceAttribute("_other_score", DataType.DOUBLE);
        assertNotEquals(
            fuse(score, discriminator, Fuse.FuseType.RRF, options(60)),
            fuse(otherScore, discriminator, Fuse.FuseType.RRF, options(60))
        );
    }

    public void testFuseTypeIsCompared() {
        assertNotEquals(
            fuse(score, discriminator, Fuse.FuseType.RRF, options(60)),
            fuse(score, discriminator, Fuse.FuseType.LINEAR, options(60))
        );
    }

    public void testOptionsAreCompared() {
        assertNotEquals(
            fuse(score, discriminator, Fuse.FuseType.RRF, options(60)),
            fuse(score, discriminator, Fuse.FuseType.RRF, options(10))
        );
    }

    /**
     * {@code options} is nullable - FUSE without an options map produces a null here.
     */
    public void testNullOptionsAreCompared() {
        assertNotEquals(fuse(score, discriminator, Fuse.FuseType.RRF, null), fuse(score, discriminator, Fuse.FuseType.RRF, options(60)));
        assertEquals(fuse(score, discriminator, Fuse.FuseType.RRF, null), fuse(score, discriminator, Fuse.FuseType.RRF, null));
    }

    /**
     * Every field that participates in {@code hashCode} must also participate in {@code equals},
     * otherwise instances that compare equal can land in different hash buckets.
     */
    public void testEqualInstancesShareHashCode() {
        FuseScoreEval a = fuse(score, discriminator, Fuse.FuseType.LINEAR, options(42));
        FuseScoreEval b = fuse(score, discriminator, Fuse.FuseType.LINEAR, options(42));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
