/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FetchDocsSourceOperatorStatusTests extends AbstractWireSerializingTestCase<FetchDocsSourceOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new FetchDocsSourceOperator.Status(3, 120, 2)), equalTo("""
            {"pages_emitted":3,"docs_emitted":120,"segment_runs":2}"""));
    }

    /**
     * The query phase already counted these documents, so the totals of a profile must not count them again.
     */
    public void testAddsNothingToProfileTotals() {
        FetchDocsSourceOperator.Status status = createTestInstance();
        assertThat(status.documentsFound(), equalTo(0L));
        assertThat(status.valuesLoaded(), equalTo(0L));
        assertThat(status.rowsEmitted(), equalTo(0L));
    }

    /**
     * A profile for a node that doesn't know the fetch phase keeps the operator and leaves its status out.
     */
    public void testAProfileForAnOlderNodeLeavesTheStatusOut() throws IOException {
        FetchDocsSourceOperator.Status status = createTestInstance();
        OperatorStatus operator = new OperatorStatus("FetchDocsSourceOperator", status);
        TransportVersion older = TransportVersionUtils.getPreviousVersion(status.getMinimalSupportedVersion());
        NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FetchDocsSourceOperator.Status.ENTRY));

        OperatorStatus copy = copyWriteable(operator, registry, OperatorStatus::readFrom, older);
        assertThat(copy.operator(), equalTo(operator.operator()));
        assertThat(copy.status(), nullValue());
    }

    @Override
    protected Writeable.Reader<FetchDocsSourceOperator.Status> instanceReader() {
        return FetchDocsSourceOperator.Status::new;
    }

    @Override
    protected FetchDocsSourceOperator.Status createTestInstance() {
        return new FetchDocsSourceOperator.Status(randomNonNegativeInt(), randomNonNegativeLong(), randomNonNegativeInt());
    }

    @Override
    protected FetchDocsSourceOperator.Status mutateInstance(FetchDocsSourceOperator.Status instance) {
        int pagesEmitted = instance.pagesEmitted();
        long docsEmitted = instance.docsEmitted();
        int segmentRuns = instance.segmentRuns();
        switch (between(0, 2)) {
            case 0 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 1 -> docsEmitted = randomValueOtherThan(docsEmitted, ESTestCase::randomNonNegativeLong);
            case 2 -> segmentRuns = randomValueOtherThan(segmentRuns, ESTestCase::randomNonNegativeInt);
            default -> throw new IllegalArgumentException();
        }
        return new FetchDocsSourceOperator.Status(pagesEmitted, docsEmitted, segmentRuns);
    }
}
