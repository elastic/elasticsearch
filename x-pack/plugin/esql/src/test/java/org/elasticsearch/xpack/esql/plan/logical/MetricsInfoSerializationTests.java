/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;

public class MetricsInfoSerializationTests extends AbstractLogicalPlanSerializationTests<MetricsInfo> {
    @Override
    protected MetricsInfo createTestInstance() {
        return new MetricsInfo(randomSource(), randomChild(0));
    }

    @Override
    protected MetricsInfo mutateInstance(MetricsInfo instance) throws IOException {
        return new MetricsInfo(instance.source(), randomValueOtherThan(instance.child(), () -> randomChild(0)));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testAttributeIdsPreservedAcrossSerialization() throws IOException {
        MetricsInfo original = createTestInstance();
        MetricsInfo deserialized = copyInstance(original);

        List<Attribute> originalAttrs = original.output();
        List<Attribute> deserializedAttrs = deserialized.output();

        assertEquals(originalAttrs.size(), deserializedAttrs.size());
        for (int i = 0; i < originalAttrs.size(); i++) {
            assertEquals(originalAttrs.get(i).name(), deserializedAttrs.get(i).name());
            assertEquals(originalAttrs.get(i).id(), deserializedAttrs.get(i).id());
        }
    }
}
