/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.equalTo;

public class BulkIndexByPaginatedSearchResponseTests extends ESTestCase {
    public void testMergeConstructor() {
        int mergeCount = between(2, 10);
        List<BulkByPaginatedSearchResponse> responses = new ArrayList<>(mergeCount);
        int took = between(1000, 10000);
        int tookIndex = between(0, mergeCount - 1);
        List<BulkItemResponse.Failure> allBulkFailures = new ArrayList<>();
        List<PaginatedSearchFailure> allSearchFailures = new ArrayList<>();
        boolean timedOut = false;
        String reasonCancelled = rarely() ? randomAlphaOfLength(5) : null;

        for (int i = 0; i < mergeCount; i++) {
            // One of the merged responses gets the expected value for took, the others get a smaller value
            TimeValue thisTook = timeValueMillis(i == tookIndex ? took : between(0, took));
            // The actual status doesn't matter too much - we test merging those elsewhere
            String thisReasonCancelled = rarely() ? randomAlphaOfLength(5) : null;
            BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
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
            List<PaginatedSearchFailure> searchFailures = frequently()
                ? emptyList()
                : IntStream.range(0, between(1, 3)).mapToObj(j -> new PaginatedSearchFailure(new Exception())).collect(Collectors.toList());
            allSearchFailures.addAll(searchFailures);
            boolean thisTimedOut = rarely();
            timedOut |= thisTimedOut;
            responses.add(new BulkByPaginatedSearchResponse(thisTook, status, bulkFailures, searchFailures, thisTimedOut));
        }

        BulkByPaginatedSearchResponse merged = new BulkByPaginatedSearchResponse(responses, reasonCancelled, null, 0f);

        assertEquals(timeValueMillis(took), merged.getTook());
        assertEquals(allBulkFailures, merged.getBulkFailures());
        assertEquals(allSearchFailures, merged.getSearchFailures());
        assertEquals(timedOut, merged.isTimedOut());
        assertEquals(reasonCancelled, merged.getReasonCancelled());
    }

    /** Verifies the merge constructor with pitId preserves the pitId in the merged response. */
    public void testMergeConstructorWithPitId() {
        BytesReference pitId = new BytesArray("merged-pit-id".getBytes(StandardCharsets.UTF_8));
        List<BulkByPaginatedSearchResponse> responses = new ArrayList<>();
        for (int i = 0; i < between(2, 5); i++) {
            BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
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
                null,
                timeValueMillis(0)
            );
            responses.add(new BulkByPaginatedSearchResponse(timeValueMillis(100), status, emptyList(), emptyList(), false));
        }

        BulkByPaginatedSearchResponse merged = new BulkByPaginatedSearchResponse(responses, null, pitId, 0f);

        assertTrue(merged.getPitId().isPresent());
        assertThat(merged.getPitId().get(), equalTo(pitId));
    }
}
