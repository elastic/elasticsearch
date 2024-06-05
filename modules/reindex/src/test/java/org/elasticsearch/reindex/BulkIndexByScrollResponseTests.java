/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class BulkIndexByScrollResponseTests extends ESTestCase {
    public void testMergeConstructor() {
        int mergeCount = between(2, 10);
        List<BulkByScrollResponse> responses = new ArrayList<>(mergeCount);
        int took = between(1000, 10000);
        int tookIndex = between(0, mergeCount - 1);
        List<BulkItemResponse.Failure> allBulkFailures = new ArrayList<>();
        List<SearchFailure> allSearchFailures = new ArrayList<>();
        boolean timedOut = false;
        String reasonCancelled = rarely() ? randomAlphaOfLength(5) : null;

        for (int i = 0; i < mergeCount; i++) {
            // One of the merged responses gets the expected value for took, the others get a smaller value
            TimeValue thisTook = timeValueMillis(i == tookIndex ? took : between(0, took));
            // The actual status doesn't matter too much - we test merging those elsewhere
            String thisReasonCancelled = rarely() ? randomAlphaOfLength(5) : null;
            BulkByScrollTask.Status status = new BulkByScrollTask.Status(
                i,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                timeValueMillis(0),
                0f,
                thisReasonCancelled,
                timeValueMillis(0)
            );
            List<BulkItemResponse.Failure> bulkFailures = frequently()
                ? emptyList()
                : IntStream.range(0, between(1, 3))
                    .mapToObj(j -> new BulkItemResponse.Failure("idx", "id", new Exception()))
                    .collect(Collectors.toList());
            allBulkFailures.addAll(bulkFailures);
            List<SearchFailure> searchFailures = frequently()
                ? emptyList()
                : IntStream.range(0, between(1, 3)).mapToObj(j -> new SearchFailure(new Exception())).collect(Collectors.toList());
            allSearchFailures.addAll(searchFailures);
            boolean thisTimedOut = rarely();
            timedOut |= thisTimedOut;
            responses.add(new BulkByScrollResponse(thisTook, status, bulkFailures, searchFailures, thisTimedOut));
        }

        BulkByScrollResponse merged = new BulkByScrollResponse(responses, reasonCancelled);

        assertEquals(timeValueMillis(took), merged.getTook());
        assertEquals(allBulkFailures, merged.getBulkFailures());
        assertEquals(allSearchFailures, merged.getSearchFailures());
        assertEquals(timedOut, merged.isTimedOut());
        assertEquals(reasonCancelled, merged.getReasonCancelled());
    }
}
