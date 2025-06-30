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
    public static final StatelessLease ZERO = new StatelessLease(0, 0);

    BytesReference asBytes() {
        if (currentTerm == 0) {
            return BytesArray.EMPTY;
        }
        final byte[] bytes;
        assert nodeLeftGeneration >= 0;
        bytes = new byte[Long.BYTES * 2];
        ByteUtils.writeLongBE(currentTerm, bytes, 0);
        ByteUtils.writeLongBE(nodeLeftGeneration, bytes, Long.BYTES);
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
