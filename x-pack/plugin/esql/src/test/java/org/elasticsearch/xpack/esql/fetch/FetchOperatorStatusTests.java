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
            1,
            1,
            800_000,
            10,
            9,
            1,
            12_000,
            30_000,
            List.of(new FetchOperator.NodeRequest("node-0", 2, 4, 700_000, 500_000, 200_000))
        );
        assertThat(Strings.toString(status), equalTo("""
            {"process_nanos":800000,"pages_received":1,"pages_completed":1,"rows_received":10,"documents":9,"pages_emitted":1,\
            "plan_nanos":12000,"gather_nanos":30000,"requests":[{"node":"node-0","shards":2,"documents":4,"request_nanos":700000,\
            "took_nanos":500000,"setup_nanos":200000}]}"""));
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
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomList(0, 4, FetchOperatorStatusTests::randomNodeRequest)
        );
    }

    @Override
    protected FetchOperator.Status mutateInstance(FetchOperator.Status instance) {
        long receivedPages = instance.receivedPages();
        long completedPages = instance.completedPages();
        long processNanos = instance.processNanos();
        long rowsReceived = instance.rowsReceived();
        long documents = instance.documents();
        int pagesEmitted = instance.pagesEmitted();
        long planNanos = instance.planNanos();
        long gatherNanos = instance.gatherNanos();
        List<FetchOperator.NodeRequest> requests = instance.requests();
        switch (between(0, 8)) {
            case 0 -> receivedPages = randomValueOtherThan(receivedPages, ESTestCase::randomNonNegativeLong);
            case 1 -> completedPages = randomValueOtherThan(completedPages, ESTestCase::randomNonNegativeLong);
            case 2 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> documents = randomValueOtherThan(documents, ESTestCase::randomNonNegativeLong);
            case 5 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 6 -> planNanos = randomValueOtherThan(planNanos, ESTestCase::randomNonNegativeLong);
            case 7 -> gatherNanos = randomValueOtherThan(gatherNanos, ESTestCase::randomNonNegativeLong);
            case 8 -> {
                requests = new ArrayList<>(requests);
                requests.add(randomNodeRequest());
            }
            default -> throw new IllegalArgumentException();
        }
        return new FetchOperator.Status(
            receivedPages,
            completedPages,
            processNanos,
            rowsReceived,
            documents,
            pagesEmitted,
            planNanos,
            gatherNanos,
            requests
        );
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
