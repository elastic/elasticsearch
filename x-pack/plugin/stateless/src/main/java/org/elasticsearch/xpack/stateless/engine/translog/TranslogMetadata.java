/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine.translog;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record TranslogMetadata(long offset, long size, long minSeqNo, long maxSeqNo, long totalOps, long shardTranslogGeneration)
    implements
        Writeable {

    public static TranslogMetadata readFromStore(StreamInput streamInput, int version) throws IOException {
        return new TranslogMetadata(
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            version >= CompoundTranslogHeader.VERSION_WITH_SHARD_TRANSLOG_GENERATION ? streamInput.readLong() : -1
        );

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(offset);
        out.writeLong(size);
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(totalOps);
        out.writeLong(shardTranslogGeneration);
    }

    @Override
    public String toString() {
        return "TranslogMetadata{"
            + "offset="
            + offset
            + ", size="
            + size
            + ", minSeqNo="
            + minSeqNo
            + ", maxSeqNo="
            + maxSeqNo
            + ", totalOps="
            + totalOps
            + ", shardTranslogGeneration="
            + shardTranslogGeneration
            + '}';
    }
}
