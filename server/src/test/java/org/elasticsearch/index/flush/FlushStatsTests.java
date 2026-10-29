/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.flush;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlushStatsTests extends AbstractWireSerializingTestCase<FlushStats> {

    @Override
    protected Writeable.Reader<FlushStats> instanceReader() {
        return FlushStats::new;
    }

    @Override
    protected FlushStats createTestInstance() {
        return new FlushStats(
            randomNonNegativeLong() / 2,
            randomNonNegativeLong() / 2,
            randomNonNegativeLong() / 2,
            randomNonNegativeLong() / 2,
            randomLongBetween(0, Integer.MAX_VALUE)
        );
    }

    @Override
    protected FlushStats mutateInstance(FlushStats instance) {
        return new FlushStats(
            instance.getTotal(),
            instance.getPeriodic(),
            instance.getTotalTimeInMillis(),
            instance.getTotalTimeExcludingWaitingOnLockMillis(),
            instance.getFailed() + 1
        );
    }

    public void testFailureCountIsNotSentToOldNodes() throws IOException {
        FlushStats stats = new FlushStats(1, 2, 3, 4, 5);

        FlushStats readByOldNode = copyInstance(stats, TransportVersion.minimumCompatible());

        assertThat(readByOldNode, equalTo(new FlushStats(1, 2, 3, 4, 0)));
    }

    public void testAddTotalsSumsFailureCounts() {
        FlushStats sum = new FlushStats(1, 1, 1, 1, 6);

        sum.addTotals(new FlushStats(1, 1, 1, 1, 10));

        assertThat(sum.getFailed(), equalTo(16L));
    }

    public void testFailureCountIsRenderedAsXContent() {
        String json = Strings.toString(new FlushStats(1, 2, 3, 4, 5), false, false);

        assertThat(json, containsString("\"failed\":5"));
    }
}
