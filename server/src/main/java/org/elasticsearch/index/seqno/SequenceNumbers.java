/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import java.util.Map;

/**
 * A utility class for handling sequence numbers.
 */
public class SequenceNumbers {

    public static final String LOCAL_CHECKPOINT_KEY = "local_checkpoint";
    public static final String MAX_SEQ_NO = "max_seq_no";
    /**
     * Represents an unassigned sequence number (e.g., can be used on primary operations before they are executed).
     */
    public static final long UNASSIGNED_SEQ_NO = -2L;
    /**
     * Represents no operations have been performed on the shard. Initial value of a sequence number.
     */
    public static final long NO_OPS_PERFORMED = -1L;

    /**
     * Represents an unassigned primary term (e.g., when a primary shard was not yet allocated)
     */
    public static final long UNASSIGNED_PRIMARY_TERM = 0L;

    /**
     * Reads the sequence number stats from the commit data (maximum sequence number and local checkpoint).
     *
     * @param commitData       the commit data
     * @return the sequence number stats
     */
    public static CommitInfo loadSeqNoInfoFromLuceneCommit(
        final Iterable<Map.Entry<String, String>> commitData) {
        long maxSeqNo = NO_OPS_PERFORMED;
        long localCheckpoint = NO_OPS_PERFORMED;

        for (final Map.Entry<String, String> entry : commitData) {
            final String key = entry.getKey();
            if (key.equals(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) {
                assert localCheckpoint == NO_OPS_PERFORMED : localCheckpoint;
                localCheckpoint = Long.parseLong(entry.getValue());
            } else if (key.equals(SequenceNumbers.MAX_SEQ_NO)) {
                assert maxSeqNo == NO_OPS_PERFORMED : maxSeqNo;
                maxSeqNo = Long.parseLong(entry.getValue());
            }
        }

        return new CommitInfo(maxSeqNo, localCheckpoint);
    }

    /**
     * Compute the minimum of the given current minimum sequence number and the specified sequence number, accounting for the fact that the
     * current minimum sequence number could be {@link SequenceNumbers#NO_OPS_PERFORMED} or
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO}. When the current minimum sequence number is not
     * {@link SequenceNumbers#NO_OPS_PERFORMED} nor {@link SequenceNumbers#UNASSIGNED_SEQ_NO}, the specified sequence number
     * must not be {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param minSeqNo the current minimum sequence number
     * @param seqNo the specified sequence number
     * @return the new minimum sequence number
     */
    public static long min(final long minSeqNo, final long seqNo) {
        if (minSeqNo == NO_OPS_PERFORMED) {
            return seqNo;
        } else if (minSeqNo == UNASSIGNED_SEQ_NO) {
            return seqNo;
        } else {
            if (seqNo == UNASSIGNED_SEQ_NO) {
                throw new IllegalArgumentException("sequence number must be assigned");
            }
            return Math.min(minSeqNo, seqNo);
        }
    }

    /**
     * Compute the maximum of the given current maximum sequence number and the specified sequence number, accounting for the fact that the
     * current maximum sequence number could be {@link SequenceNumbers#NO_OPS_PERFORMED} or
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO}. When the current maximum sequence number is not
     * {@link SequenceNumbers#NO_OPS_PERFORMED} nor {@link SequenceNumbers#UNASSIGNED_SEQ_NO}, the specified sequence number
     * must not be {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param maxSeqNo the current maximum sequence number
     * @param seqNo the specified sequence number
     * @return the new maximum sequence number
     */
    public static long max(final long maxSeqNo, final long seqNo) {
        if (maxSeqNo == NO_OPS_PERFORMED) {
            return seqNo;
        } else if (maxSeqNo == UNASSIGNED_SEQ_NO) {
            return seqNo;
        } else {
            if (seqNo == UNASSIGNED_SEQ_NO) {
                throw new IllegalArgumentException("sequence number must be assigned");
            }
            return Math.max(maxSeqNo, seqNo);
        }
    }

    public static final class CommitInfo {
        public final long maxSeqNo;
        public final long localCheckpoint;

        public CommitInfo(long maxSeqNo, long localCheckpoint) {
            this.maxSeqNo = maxSeqNo;
            this.localCheckpoint = localCheckpoint;
        }

        @Override
        public String toString() {
            return "CommitInfo{" +
                "maxSeqNo=" + maxSeqNo +
                ", localCheckpoint=" + localCheckpoint +
                '}';
        }
    }
}
