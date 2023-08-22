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

public record TranslogMetadata(long offset, long size, long minSeqNo, long maxSeqNo, long totalOps) implements Writeable {

    public TranslogMetadata(StreamInput streamInput) throws IOException {
        this(streamInput.readLong(), streamInput.readLong(), streamInput.readLong(), streamInput.readLong(), streamInput.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(offset);
        out.writeLong(size);
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(totalOps);
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
            + '}';
    }
}
