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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;

import java.util.Optional;

public record StatelessLease(long currentTerm, long nodeLeftGeneration) implements Comparable<StatelessLease> {
    private static final long UNSUPPORTED = -1L;

    public static final StatelessLease ZERO = new StatelessLease(0, 0);

    BytesReference asBytes() {
        if (currentTerm == 0) {
            return BytesArray.EMPTY;
        }
        final byte[] bytes;
        // If node left generation is unsupported, this lease was written by a cluster with a node whose version is prior to the time
        // when node left generation was introduced. Do not introduce the value until the entire cluster is upgraded and a node-left
        // event occurs.
        if (nodeLeftGeneration == UNSUPPORTED) {
            bytes = new byte[Long.BYTES];
            ByteUtils.writeLongBE(currentTerm, bytes, 0);
        } else {
            bytes = new byte[Long.BYTES * 2];
            ByteUtils.writeLongBE(currentTerm, bytes, 0);
            ByteUtils.writeLongBE(nodeLeftGeneration, bytes, Long.BYTES);
        }
        return new BytesArray(bytes);
    }

    public static Optional<StatelessLease> fromBytes(OptionalBytesReference optionalBytesReference) {
        if (optionalBytesReference.isPresent() == false) {
            return Optional.empty();
        }
        StatelessLease result;
        BytesReference bytesReference = optionalBytesReference.bytesReference();
        if (bytesReference.length() == 0) {
            return Optional.of(StatelessLease.ZERO);
        } else if (bytesReference.length() == 2 * Long.BYTES) {
            result = new StatelessLease(
                Long.reverseBytes(bytesReference.getLongLE(0)),
                Long.reverseBytes(bytesReference.getLongLE(Long.BYTES))
            );
        } else {
            throw new IllegalArgumentException("cannot read terms from BytesReference of length " + bytesReference.length());
        }
        return Optional.of(result);
    }

    @Override
    public int compareTo(StatelessLease that) {
        int result = Long.compare(this.currentTerm, that.currentTerm);
        if (result == 0) {
            result = Long.compare(this.nodeLeftGeneration, that.nodeLeftGeneration);
        }
        return result;
    }
}
