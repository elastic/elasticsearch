/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

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
            BulkByScrollTask.Status status = new BulkByScrollTask.Status(i, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f,
                    thisReasonCancelled, timeValueMillis(0));
            List<BulkItemResponse.Failure> bulkFailures = frequently() ? emptyList()
                    : IntStream.range(0, between(1, 3)).mapToObj(j -> new BulkItemResponse.Failure("idx", "type", "id", new Exception()))
                            .collect(Collectors.toList());
            allBulkFailures.addAll(bulkFailures);
            List<SearchFailure> searchFailures = frequently() ? emptyList()
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
