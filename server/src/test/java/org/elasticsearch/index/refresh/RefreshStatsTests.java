/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.refresh;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RefreshStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        RefreshStats stats = new RefreshStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
            randomNonNegativeLong(), between(0, Integer.MAX_VALUE));
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        RefreshStats read = new RefreshStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getTotal(), read.getTotal());
        assertEquals(stats.getExternalTotal(), read.getExternalTotal());
        assertEquals(stats.getListeners(), read.getListeners());
        assertEquals(stats.getTotalTimeInMillis(), read.getTotalTimeInMillis());
        assertEquals(stats.getExternalTotalTimeInMillis(), read.getExternalTotalTimeInMillis());
    }
}
