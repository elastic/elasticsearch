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

public class TsInfoSerializationTests extends AbstractLogicalPlanSerializationTests<TsInfo> {
    @Override
    protected TsInfo createTestInstance() {
        return new TsInfo(randomSource(), randomChild(0));
    }

    @Override
    protected TsInfo mutateInstance(TsInfo instance) throws IOException {
        return new TsInfo(instance.source(), randomValueOtherThan(instance.child(), () -> randomChild(0)));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testAttributeIdsPreservedAcrossSerialization() throws IOException {
        TsInfo original = createTestInstance();
        TsInfo deserialized = copyInstance(original);

        List<Attribute> originalAttrs = original.output();
        List<Attribute> deserializedAttrs = deserialized.output();

        assertEquals(originalAttrs.size(), deserializedAttrs.size());
        for (int i = 0; i < originalAttrs.size(); i++) {
            assertEquals(originalAttrs.get(i).name(), deserializedAttrs.get(i).name());
            assertEquals(originalAttrs.get(i).id(), deserializedAttrs.get(i).id());
        }
    }
}
