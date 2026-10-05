/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class FetchExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FetchExec> {
    public static FetchExec randomFetchExec(int depth) {
        List<Attribute> fetched = randomFieldAttributes(1, 5, false);
        return new FetchExec(
            randomSource(),
            randomChild(depth),
            randomFetchPlan(fetched),
            DocRefEncodeExecSerializationTests.randomDocRef(),
            fetched,
            between(1, 3),
            randomAlphaOfLength(5),
            randomList(1, 3, () -> randomAlphaOfLength(5)),
            randomEstimatedRowSize()
        );
    }

    /** The shape the planner builds: the fetched columns loaded over the fetch source. */
    static PhysicalPlan randomFetchPlan(List<Attribute> fetched) {
        FetchSourceExec source = FetchSourceExecSerializationTests.randomFetchSourceExec();
        return new ProjectExec(
            Source.EMPTY,
            new FieldExtractExec(Source.EMPTY, source, fetched, MappedFieldType.FieldExtractPreference.NONE),
            fetched
        );
    }

    @Override
    protected FetchExec createTestInstance() {
        return randomFetchExec(0);
    }

    @Override
    protected FetchExec mutateInstance(FetchExec instance) throws IOException {
        PhysicalPlan left = instance.left();
        PhysicalPlan fetchPlan = instance.fetchPlan();
        ReferenceAttribute docRef = instance.docRef();
        List<Attribute> fetched = instance.fetchedAttributes();
        int stage = instance.stage();
        String indexPattern = instance.indexPattern();
        List<String> originalIndices = instance.originalIndices();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 7)) {
            case 0 -> left = randomValueOtherThan(left, () -> randomChild(0));
            case 1 -> {
                fetched = randomValueOtherThan(fetched, () -> randomFieldAttributes(1, 5, false));
                fetchPlan = randomFetchPlan(fetched);
            }
            case 2 -> fetchPlan = randomValueOtherThan(fetchPlan, () -> randomFetchPlan(instance.fetchedAttributes()));
            case 3 -> docRef = randomValueOtherThan(docRef, DocRefEncodeExecSerializationTests::randomDocRef);
            case 4 -> stage = randomValueOtherThan(stage, () -> between(1, 3));
            case 5 -> indexPattern = randomValueOtherThan(indexPattern, () -> randomAlphaOfLength(5));
            case 6 -> originalIndices = randomValueOtherThan(originalIndices, () -> randomList(1, 3, () -> randomAlphaOfLength(5)));
            case 7 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            default -> throw new AssertionError("Unexpected case");
        }
        return new FetchExec(instance.source(), left, fetchPlan, docRef, fetched, stage, indexPattern, originalIndices, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
