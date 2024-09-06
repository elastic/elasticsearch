/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FieldExtractExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FieldExtractExec> {
    public static FieldExtractExec randomFieldExtractExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        List<Attribute> attributesToExtract = randomFieldAttributes(1, 4, false);
        Set<Attribute> docValuesAttributes = new HashSet<>(randomFieldAttributes(1, 4, false));
        return new FieldExtractExec(source, child, attributesToExtract, docValuesAttributes);
    }

    @Override
    protected FieldExtractExec createTestInstance() {
        return randomFieldExtractExec(0);
    }

    @Override
    protected FieldExtractExec mutateInstance(FieldExtractExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Attribute> attributesToExtract = instance.attributesToExtract();
        Set<Attribute> docValuesAttributes = instance.docValuesAttributes();
        switch (between(0, 2)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> attributesToExtract = randomValueOtherThan(attributesToExtract, () -> randomFieldAttributes(1, 4, false));
            case 2 -> docValuesAttributes = randomValueOtherThan(
                docValuesAttributes,
                () -> new HashSet<>(randomFieldAttributes(1, 4, false))
            );
        }
        return new FieldExtractExec(instance.source(), child, attributesToExtract, docValuesAttributes);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
