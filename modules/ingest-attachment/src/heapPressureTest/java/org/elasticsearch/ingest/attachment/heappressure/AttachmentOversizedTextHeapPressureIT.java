/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment.heappressure;

import java.util.Map;

/**
 * A document whose extracted text exceeds the heap (150M chars, 300 MB as UTF-16) must be rejected by the request circuit breaker rather
 * than run the node out of memory, and the node must keep processing attachments afterwards.
 */
public class AttachmentOversizedTextHeapPressureIT extends AttachmentHeapPressureTestCase {

    private static final long TEXT_CHARS = 150_000_000;

    public void testTextExceedingHeapTripsBreaker() throws Exception {
        assertSmallSpreadsheetIndexed("small-before");

        Map<String, Object> body = expectFailure(indexRequest("oversized", Spreadsheet.withTextChars(TEXT_CHARS)));
        assertTrue("expected a circuit_breaking_exception, but got: " + body, containsCircuitBreakingException(body));
        // An out-of-memory node would not answer.
        assertNodeAlive();

        assertSmallSpreadsheetIndexed("small-after");
    }
}
