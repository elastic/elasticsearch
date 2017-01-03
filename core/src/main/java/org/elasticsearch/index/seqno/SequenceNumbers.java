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

package org.elasticsearch.index.seqno;

import java.util.Map;

/**
 * A utility class for handling sequence numbers.
 */
public class SequenceNumbers {

    public static final String LOCAL_CHECKPOINT_KEY = "local_checkpoint";
    public static final String MAX_SEQ_NO = "max_seq_no";

    /**
     * Reads the sequence number stats from the commit data (maximum sequence number and local checkpoint) and uses the specified global
     * checkpoint.
     *
     * @param globalCheckpoint the global checkpoint to use
     * @param commitData       the commit data
     * @return the sequence number stats
     */
    public static SeqNoStats loadSeqNoStatsFromLuceneCommit(
        final long globalCheckpoint,
        final Iterable<Map.Entry<String, String>> commitData) {
        long maxSeqNo = SequenceNumbersService.NO_OPS_PERFORMED;
        long localCheckpoint = SequenceNumbersService.NO_OPS_PERFORMED;

        for (final Map.Entry<String, String> entry : commitData) {
            final String key = entry.getKey();
            if (key.equals(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) {
                assert localCheckpoint == SequenceNumbersService.NO_OPS_PERFORMED : localCheckpoint;
                localCheckpoint = Long.parseLong(entry.getValue());
            } else if (key.equals(SequenceNumbers.MAX_SEQ_NO)) {
                assert maxSeqNo == SequenceNumbersService.NO_OPS_PERFORMED : maxSeqNo;
                maxSeqNo = Long.parseLong(entry.getValue());
            }
        }

        return new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
    }

}
