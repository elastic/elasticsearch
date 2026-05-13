/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class PushLimitToExternalSourceTests extends ESTestCase {

    public void testLimitPushedToExternalSource() {
        ExternalSourceExec ext = externalSource();
        assertEquals(FormatReader.NO_LIMIT, ext.pushedLimit());

        LimitExec limitExec = new LimitExec(Source.EMPTY, ext, literal(10), null);
        PhysicalPlan result = applyRule(limitExec);

        assertThat(result, instanceOf(LimitExec.class));
        LimitExec resultLimit = (LimitExec) result;
        assertThat(resultLimit.child(), instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) resultLimit.child();
        assertEquals(10, resultExt.pushedLimit());
    }

    public void testLimitNotPushedToNonExternalSource() {
        ExternalSourceExec ext = externalSource();
        LimitExec innerLimit = new LimitExec(Source.EMPTY, ext, literal(100), null);
        LimitExec outerLimit = new LimitExec(Source.EMPTY, innerLimit, literal(10), null);

        PhysicalPlan result = applyRule(outerLimit);

        // The rule only looks at direct child; inner LimitExec is not ExternalSourceExec
        assertThat(result, instanceOf(LimitExec.class));
        LimitExec resultLimit = (LimitExec) result;
        assertThat(resultLimit.child(), instanceOf(LimitExec.class));
    }

    public void testNoLimitDefault() {
        ExternalSourceExec ext = externalSource();
        assertEquals(FormatReader.NO_LIMIT, ext.pushedLimit());
    }

    public void testWithPushedLimitReturnsNewInstance() {
        ExternalSourceExec ext = externalSource();
        ExternalSourceExec withLimit = ext.withPushedLimit(42);

        assertNotSame(ext, withLimit);
        assertEquals(FormatReader.NO_LIMIT, ext.pushedLimit());
        assertEquals(42, withLimit.pushedLimit());
        assertEquals(ext.sourcePath(), withLimit.sourcePath());
        assertEquals(ext.sourceType(), withLimit.sourceType());
        assertEquals(ext.output(), withLimit.output());
    }

    public void testEqualsIncludesLimit() {
        ExternalSourceExec base = externalSource();
        ExternalSourceExec ext1 = base.withPushedLimit(10);
        ExternalSourceExec ext2 = base.withPushedLimit(10);
        ExternalSourceExec ext3 = base.withPushedLimit(20);

        assertEquals(ext1, ext2);
        assertNotEquals(ext1, ext3);
        assertEquals(ext1.hashCode(), ext2.hashCode());
    }

    private static ExternalSourceExec externalSource() {
        List<Attribute> attrs = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER));
        return new ExternalSourceExec(Source.EMPTY, "file:///test.csv", "file", attrs, Map.of(), Map.of(), null);
    }

    private static Literal literal(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static PhysicalPlan applyRule(LimitExec limitExec) {
        PushLimitToExternalSource rule = new PushLimitToExternalSource();
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null
        );
        return rule.apply(limitExec, ctx);
    }
}
