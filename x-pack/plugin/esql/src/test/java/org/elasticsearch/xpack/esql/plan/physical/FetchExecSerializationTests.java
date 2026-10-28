/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;

import java.io.IOException;
import java.util.List;

public class FetchExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FetchExec> {
    public static FetchExec randomFetchExec(int depth) {
        return new FetchExec(
            randomSource(),
            randomChild(depth),
            randomDoc(),
            randomFieldAttributes(1, 5, false),
            randomAlphaOfLength(5),
            randomList(1, 3, () -> randomAlphaOfLength(5)),
            randomEstimatedRowSize()
        );
    }

    static Attribute randomDoc() {
        return new FieldAttribute(randomSource(), null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
    }

    @Override
    protected FetchExec createTestInstance() {
        return randomFetchExec(0);
    }

    @Override
    protected FetchExec mutateInstance(FetchExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Attribute doc = instance.doc();
        List<Attribute> fetched = instance.fetchedAttributes();
        String indexPattern = instance.indexPattern();
        List<String> originalIndices = instance.originalIndices();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> doc = randomValueOtherThan(doc, FetchExecSerializationTests::randomDoc);
            case 2 -> fetched = randomValueOtherThan(fetched, () -> randomFieldAttributes(1, 5, false));
            case 3 -> indexPattern = randomValueOtherThan(indexPattern, () -> randomAlphaOfLength(5));
            case 4 -> originalIndices = randomValueOtherThan(originalIndices, () -> randomList(1, 3, () -> randomAlphaOfLength(5)));
            case 5 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            default -> throw new AssertionError("Unexpected case");
        }
        return new FetchExec(instance.source(), child, doc, fetched, indexPattern, originalIndices, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
