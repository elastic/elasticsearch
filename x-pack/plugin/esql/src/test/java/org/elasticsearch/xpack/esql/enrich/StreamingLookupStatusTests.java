/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.enrich.StreamingLookupFromIndexOperator.StreamingLookupStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StreamingLookupStatusTests extends AbstractWireSerializingTestCase<StreamingLookupStatus> {
    @Override
    protected Writeable.Reader<StreamingLookupStatus> instanceReader() {
        return StreamingLookupStatus::new;
    }

    @Override
    protected StreamingLookupStatus createTestInstance() {
        return new StreamingLookupStatus(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomPlanToWorkers(),
            randomNonNegativeLong()
        );
    }

    private static Map<String, Set<String>> randomPlanToWorkers() {
        int planCount = randomIntBetween(0, 2);
        Map<String, Set<String>> map = new HashMap<>();
        for (int i = 0; i < planCount; i++) {
            Set<String> workers = new HashSet<>();
            int workerCount = randomIntBetween(1, 3);
            for (int j = 0; j < workerCount; j++) {
                workers.add(randomAlphaOfLength(8));
            }
            map.put(randomAlphaOfLength(10), workers);
        }
        return map;
    }

    @Override
    protected StreamingLookupStatus mutateInstance(StreamingLookupStatus in) throws IOException {
        long pagesReceived = in.pagesReceived();
        long pagesEmitted = in.pagesEmitted();
        long rowsReceived = in.rowsReceived();
        long rowsEmitted = in.rowsEmitted();
        long planningNanos = in.planningNanos();
        long processNanos = in.processNanos();
        Map<String, Set<String>> planToWorkers = in.planToWorkers();
        long bytesRead = in.bytesRead();
        switch (randomIntBetween(0, 7)) {
            case 0 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeLong);
            case 1 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeLong);
            case 2 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 4 -> planningNanos = randomValueOtherThan(planningNanos, ESTestCase::randomNonNegativeLong);
            case 5 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 6 -> planToWorkers = randomValueOtherThan(planToWorkers, StreamingLookupStatusTests::randomPlanToWorkers);
            case 7 -> bytesRead = randomValueOtherThan(bytesRead, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new StreamingLookupStatus(
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            planningNanos,
            processNanos,
            planToWorkers,
            bytesRead
        );
    }
}
