/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentMultipartHelperTests extends ESTestCase {

    public void testAllPartsExecutedWhenExecutorRejects() throws IOException {
        final long partSize = 10L;
        final int nbParts = randomIntBetween(2, 10);
        final long blobSize = partSize * nbParts;
        final AtomicInteger partsExecuted = new AtomicInteger(0);

        ConcurrentMultipartHelper.runConcurrentParts(
            blobSize,
            partSize,
            command -> { throw new RejectedExecutionException("executor is full"); },
            (partNum, offset, size, lastPart) -> partsExecuted.incrementAndGet()
        );

        assertEquals(nbParts, partsExecuted.get());
    }
}
