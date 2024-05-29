/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class ErrorEntryTests extends ESTestCase {

    public void testIncrementRetryCount() {
        long now = System.currentTimeMillis();
        ErrorEntry existingRecord = new ErrorEntry(now, "error message", now, 0);
        long newOccurenceTimestamp = now + 2L;
        ErrorEntry newEntry = ErrorEntry.incrementRetryCount(existingRecord, () -> newOccurenceTimestamp);

        assertThat(newEntry.firstOccurrenceTimestamp(), is(existingRecord.firstOccurrenceTimestamp()));
        assertThat(newEntry.error(), is(existingRecord.error()));
        assertThat(newEntry.recordedTimestamp(), is(newOccurenceTimestamp));
        assertThat(newEntry.retryCount(), is(1));
    }

}
