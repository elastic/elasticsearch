/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.operator.fuse.FuseConfig;
import org.elasticsearch.compute.operator.fuse.LinearConfig;
import org.elasticsearch.compute.operator.fuse.RrfConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Equality tests for {@link FuseScoreEvalExec}.
 *
 * <p>{@code UnaryExec} compares the child only, so a subclass carrying extra state has to override
 * {@code equals}/{@code hashCode} - as {@code EvalExec}, {@code FilterExec}, {@code LimitExec} and
 * {@code TopNExec} all do. Without the override two FUSE nodes differing only in their fuse config
 * (RRF vs LINEAR, different rank constants or weights) compare equal, which hides a rewrite from the
 * physical plan optimizer's before/after comparison.
 */
public class FuseScoreEvalExecTests extends ESTestCase {

    private final PhysicalPlan child = AbstractPhysicalPlanSerializationTests.randomChild(0);
    private final Attribute score = referenceAttribute("_score", DataType.DOUBLE);
    private final Attribute discriminator = referenceAttribute("_fork", DataType.KEYWORD);

    private FuseScoreEvalExec fuse(Attribute scoreAttr, Attribute discriminatorAttr, FuseConfig config) {
        return new FuseScoreEvalExec(Source.EMPTY, child, scoreAttr, discriminatorAttr, config);
    }

    public void testEqualsForIdenticalFields() {
        FuseScoreEvalExec a = fuse(score, discriminator, RrfConfig.DEFAULT_CONFIG);
        FuseScoreEvalExec b = fuse(score, discriminator, RrfConfig.DEFAULT_CONFIG);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testFuseConfigIsCompared() {
        assertNotEquals(fuse(score, discriminator, RrfConfig.DEFAULT_CONFIG), fuse(score, discriminator, LinearConfig.DEFAULT_CONFIG));
    }

    public void testRankConstantIsCompared() {
        assertNotEquals(fuse(score, discriminator, new RrfConfig(60d, Map.of())), fuse(score, discriminator, new RrfConfig(10d, Map.of())));
    }

    public void testWeightsAreCompared() {
        assertNotEquals(
            fuse(score, discriminator, new RrfConfig(60d, Map.of("a", 1.0))),
            fuse(score, discriminator, new RrfConfig(60d, Map.of("a", 2.0)))
        );
    }

    public void testScoreIsCompared() {
        Attribute otherScore = referenceAttribute("_other_score", DataType.DOUBLE);
        assertNotEquals(fuse(score, discriminator, RrfConfig.DEFAULT_CONFIG), fuse(otherScore, discriminator, RrfConfig.DEFAULT_CONFIG));
    }

    public void testDiscriminatorIsCompared() {
        Attribute otherDiscriminator = referenceAttribute("_other_fork", DataType.KEYWORD);
        assertNotEquals(fuse(score, discriminator, RrfConfig.DEFAULT_CONFIG), fuse(score, otherDiscriminator, RrfConfig.DEFAULT_CONFIG));
    }
}
