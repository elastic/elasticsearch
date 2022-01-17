/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class SequenceNumbersTests extends ESTestCase {

    public void testMin() {
        final long seqNo = randomNonNegativeLong();
        assertThat(SequenceNumbers.min(SequenceNumbers.NO_OPS_PERFORMED, seqNo), equalTo(seqNo));
        assertThat(
            SequenceNumbers.min(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.UNASSIGNED_SEQ_NO),
            equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO)
        );
        assertThat(SequenceNumbers.min(SequenceNumbers.UNASSIGNED_SEQ_NO, seqNo), equalTo(seqNo));
        final long minSeqNo = randomNonNegativeLong();
        assertThat(SequenceNumbers.min(minSeqNo, seqNo), equalTo(Math.min(minSeqNo, seqNo)));

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SequenceNumbers.min(minSeqNo, SequenceNumbers.UNASSIGNED_SEQ_NO)
        );
        assertThat(e, hasToString(containsString("sequence number must be assigned")));
    }

    public void testMax() {
        final long seqNo = randomNonNegativeLong();
        assertThat(SequenceNumbers.max(SequenceNumbers.NO_OPS_PERFORMED, seqNo), equalTo(seqNo));
        assertThat(
            SequenceNumbers.max(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.UNASSIGNED_SEQ_NO),
            equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO)
        );
        assertThat(SequenceNumbers.max(SequenceNumbers.UNASSIGNED_SEQ_NO, seqNo), equalTo(seqNo));
        final long maxSeqNo = randomNonNegativeLong();
        assertThat(SequenceNumbers.min(maxSeqNo, seqNo), equalTo(Math.min(maxSeqNo, seqNo)));

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SequenceNumbers.min(maxSeqNo, SequenceNumbers.UNASSIGNED_SEQ_NO)
        );
        assertThat(e, hasToString(containsString("sequence number must be assigned")));
    }

    public void testSeqNoStatsEqualsAndHashCode() {
        final long maxSeqNo = randomLongBetween(SequenceNumbers.UNASSIGNED_SEQ_NO, Long.MAX_VALUE);
        final long localCheckpoint = randomLongBetween(SequenceNumbers.UNASSIGNED_SEQ_NO, maxSeqNo);
        final long globalCheckpoint = randomLongBetween(SequenceNumbers.UNASSIGNED_SEQ_NO, localCheckpoint);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint),
            stats -> new SeqNoStats(stats.getMaxSeqNo(), stats.getLocalCheckpoint(), stats.getGlobalCheckpoint())
        );
    }
}
