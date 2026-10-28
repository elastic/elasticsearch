/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Verifies that the sampling hint survives planning and remains compatible with older peers. */
public class MetricsInfoExecSerializationTests extends AbstractPhysicalPlanSerializationTests<MetricsInfoExec> {
    @Override
    protected MetricsInfoExec createTestInstance() {
        return new MetricsInfoExec(
            randomSource(),
            randomChild(0),
            attributes(),
            attributes(),
            randomFrom(MetricsInfoExec.Mode.values()),
            randomBoolean() ? null : between(0, Integer.MAX_VALUE)
        );
    }

    @Override
    protected MetricsInfoExec mutateInstance(MetricsInfoExec instance) {
        PhysicalPlan child = instance.child();
        List<Attribute> output = instance.outputAttrs();
        List<Attribute> intermediate = instance.intermediateAttributes();
        MetricsInfoExec.Mode mode = instance.mode();
        Integer limit = instance.limit();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> output = attributes();
            case 2 -> intermediate = attributes();
            case 3 -> mode = randomValueOtherThan(mode, () -> randomFrom(MetricsInfoExec.Mode.values()));
            case 4 -> limit = randomValueOtherThan(limit, () -> randomBoolean() ? null : between(0, Integer.MAX_VALUE));
            default -> throw new AssertionError("unexpected mutation");
        }
        return new MetricsInfoExec(instance.source(), child, output, intermediate, mode, limit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testOlderVersionOmitsLimit() throws IOException {
        MetricsInfoExec instance = new MetricsInfoExec(
            Source.EMPTY,
            randomChild(0),
            attributes(),
            attributes(),
            MetricsInfoExec.Mode.INITIAL,
            10
        );
        MetricsInfoExec copy = copyInstance(instance, TransportVersionUtils.getPreviousVersion(MetricsInfo.ESQL_METRICS_INFO_LIMIT));
        assertNull(copy.limit());
        assertEquals(instance.outputAttrs(), copy.outputAttrs());
        assertEquals(instance.intermediateAttributes(), copy.intermediateAttributes());
        assertEquals(instance.child(), copy.child());
        assertEquals(instance.mode(), copy.mode());
    }

    private static List<Attribute> attributes() {
        List<Attribute> attributes = new ArrayList<>();
        for (String name : MetricsInfo.ATTRIBUTES) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
        }
        return attributes;
    }
}
