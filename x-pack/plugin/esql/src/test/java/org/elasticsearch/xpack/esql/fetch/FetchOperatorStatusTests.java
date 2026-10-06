/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class FetchOperatorStatusTests extends AbstractWireSerializingTestCase<FetchOperator.Status> {
    public void testToXContent() {
        FetchOperator.Status status = new FetchOperator.Status(
            10,
            9,
            1,
            12_000,
            800_000,
            30_000,
            List.of(new FetchOperator.NodeRequest("node-0", 2, 4, 700_000, 500_000, 200_000))
        );
        assertThat(Strings.toString(status), equalTo("""
            {"rows_received":10,"documents":9,"pages_emitted":1,"plan_nanos":12000,"wait_nanos":800000,"gather_nanos":30000,\
            "nodes":[{"node":"node-0","shards":2,"documents":4,"request_nanos":700000,"took_nanos":500000,"setup_nanos":200000}]}"""));
    }

    /**
     * The query phase already counted the rows and documents, so the totals of a profile must not count them again.
     */
    public void testAddsNothingToProfileTotals() {
        FetchOperator.Status status = createTestInstance();
        assertThat(status.documentsFound(), equalTo(0L));
        assertThat(status.valuesLoaded(), equalTo(0L));
        assertThat(status.rowsEmitted(), equalTo(0L));
    }

    @Override
    protected Writeable.Reader<FetchOperator.Status> instanceReader() {
        return FetchOperator.Status::new;
    }

    @Override
    protected FetchOperator.Status createTestInstance() {
        return new FetchOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomList(0, 4, FetchOperatorStatusTests::randomNodeRequest)
        );
    }

    @Override
    protected FetchOperator.Status mutateInstance(FetchOperator.Status instance) {
        long rowsReceived = instance.rowsReceived();
        long documents = instance.documents();
        int pagesEmitted = instance.pagesEmitted();
        long planNanos = instance.planNanos();
        long waitNanos = instance.waitNanos();
        long gatherNanos = instance.gatherNanos();
        List<FetchOperator.NodeRequest> nodes = instance.nodes();
        switch (between(0, 6)) {
            case 0 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 1 -> documents = randomValueOtherThan(documents, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 3 -> planNanos = randomValueOtherThan(planNanos, ESTestCase::randomNonNegativeLong);
            case 4 -> waitNanos = randomValueOtherThan(waitNanos, ESTestCase::randomNonNegativeLong);
            case 5 -> gatherNanos = randomValueOtherThan(gatherNanos, ESTestCase::randomNonNegativeLong);
            case 6 -> {
                nodes = new ArrayList<>(nodes);
                nodes.add(randomNodeRequest());
            }
            default -> throw new IllegalArgumentException();
        }
        return new FetchOperator.Status(rowsReceived, documents, pagesEmitted, planNanos, waitNanos, gatherNanos, nodes);
    }

    private static FetchOperator.NodeRequest randomNodeRequest() {
        return new FetchOperator.NodeRequest(
            randomAlphaOfLength(6),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }
}
