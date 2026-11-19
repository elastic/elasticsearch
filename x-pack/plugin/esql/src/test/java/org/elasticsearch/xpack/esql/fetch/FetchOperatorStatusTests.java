/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FetchOperatorStatusTests extends AbstractWireSerializingTestCase<FetchOperator.Status> {
    public void testToXContent() {
        FetchOperator.Status status = new FetchOperator.Status(
            2,
            10,
            9,
            2,
            12_000,
            30_000,
            List.of(new FetchOperator.NodeFetchStats("node-0", 2, 4, 700_000, 500_000, 200_000))
        );
        assertThat(Strings.toString(status), equalTo("""
            {"pages_received":2,"rows_received":10,"documents":9,"pages_emitted":2,"plan_nanos":12000,"gather_nanos":30000,\
            "nodes":[{"node":"node-0","shards":2,"documents":4,"request_nanos":700000,"took_nanos":500000,\
            "setup_nanos":200000}]}"""));
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

    /**
     * A profile for a node that doesn't know the fetch phase keeps the operator and leaves its status out.
     */
    public void testAProfileForAnOlderNodeLeavesTheStatusOut() throws IOException {
        OperatorStatus operator = new OperatorStatus("FetchOperator", createTestInstance());
        TransportVersion older = TransportVersionUtils.getPreviousVersion(FetchRequest.ESQL_FETCH);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(FetchOperator.Status.ENTRY));

        OperatorStatus copy = copyWriteable(operator, registry, OperatorStatus::readFrom, older);
        assertThat(copy.operator(), equalTo(operator.operator()));
        assertThat(copy.status(), nullValue());
    }

    @Override
    protected Writeable.Reader<FetchOperator.Status> instanceReader() {
        return FetchOperator.Status::new;
    }

    @Override
    protected FetchOperator.Status createTestInstance() {
        return new FetchOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomList(0, 4, FetchOperatorStatusTests::randomNodeFetchStats)
        );
    }

    @Override
    protected FetchOperator.Status mutateInstance(FetchOperator.Status instance) {
        int pagesReceived = instance.pagesReceived();
        long rowsReceived = instance.rowsReceived();
        long documents = instance.documents();
        int pagesEmitted = instance.pagesEmitted();
        long planNanos = instance.planNanos();
        long gatherNanos = instance.gatherNanos();
        List<FetchOperator.NodeFetchStats> nodes = instance.nodes();
        switch (between(0, 6)) {
            case 0 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 1 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 2 -> documents = randomValueOtherThan(documents, ESTestCase::randomNonNegativeLong);
            case 3 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 4 -> planNanos = randomValueOtherThan(planNanos, ESTestCase::randomNonNegativeLong);
            case 5 -> gatherNanos = randomValueOtherThan(gatherNanos, ESTestCase::randomNonNegativeLong);
            case 6 -> {
                nodes = new ArrayList<>(nodes);
                nodes.add(randomNodeFetchStats());
            }
            default -> throw new IllegalArgumentException();
        }
        return new FetchOperator.Status(pagesReceived, rowsReceived, documents, pagesEmitted, planNanos, gatherNanos, nodes);
    }

    private static FetchOperator.NodeFetchStats randomNodeFetchStats() {
        return new FetchOperator.NodeFetchStats(
            randomAlphaOfLength(6),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }
}
