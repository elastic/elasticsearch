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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A "shard history retention lease" (or "retention lease" for short) is conceptually a marker containing a retaining sequence number such
 * that all operations with sequence number at least that retaining sequence number will be retained during merge operations (which could
 * otherwise merge away operations that have been soft deleted). Each retention lease contains a unique identifier, the retaining sequence
 * number, the timestamp of when the lease was created or renewed, and the source of the retention lease (e.g., "ccr").
 */
public final class RetentionLease implements Writeable {

    private final String id;

    /**
     * The identifier for this retention lease. This identifier should be unique per lease and is set during construction by the caller.
     *
     * @return the identifier
     */
    public String id() {
        return id;
    }

    private final long retainingSequenceNumber;

    /**
     * The retaining sequence number of this retention lease. The retaining sequence number is the minimum sequence number that this
     * retention lease wants to retain during merge operations. The retaining sequence number is set during construction by the caller.
     *
     * @return the retaining sequence number
     */
    public long retainingSequenceNumber() {
        return retainingSequenceNumber;
    }

    private final long timestamp;

    /**
     * The timestamp of when this retention lease was created or renewed.
     *
     * @return the timestamp used as a basis for determining lease expiration
     */
    public long timestamp() {
        return timestamp;
    }

    private final String source;

    /**
     * The source of this retention lease. The source is set during construction by the caller.
     *
     * @return the source
     */
    public String source() {
        return source;
    }

    /**
     * Constructs a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param timestamp               the timestamp of when the retention lease was created or renewed
     * @param source                  the source of the retention lease
     */
    public RetentionLease(final String id, final long retainingSequenceNumber, final long timestamp, final String source) {
        Objects.requireNonNull(id);
        if (id.isEmpty()) {
            throw new IllegalArgumentException("retention lease ID can not be empty");
        }
        if (id.contains(":") || id.contains(";") || id.contains(",")) {
            // retention lease IDs can not contain these characters because they are used in encoding retention leases
            throw new IllegalArgumentException("retention lease ID can not contain any of [:;,] but was [" + id + "]");
        }
        if (retainingSequenceNumber < SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalArgumentException("retention lease retaining sequence number [" + retainingSequenceNumber + "] out of range");
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("retention lease timestamp [" + timestamp + "] out of range");
        }
        Objects.requireNonNull(source);
        if (source.isEmpty()) {
            throw new IllegalArgumentException("retention lease source can not be empty");
        }
        if (source.contains(":") || source.contains(";") || source.contains(",")) {
            // retention lease sources can not contain these characters because they are used in encoding retention leases
            throw new IllegalArgumentException("retention lease source can not contain any of [:;,] but was [" + source + "]");
        }
        this.id = id;
        this.retainingSequenceNumber = retainingSequenceNumber;
        this.timestamp = timestamp;
        this.source = source;
    }

    /**
     * Constructs a new retention lease from a stream. The retention lease should have been written via {@link #writeTo(StreamOutput)}.
     *
     * @param in the stream to construct the retention lease from
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public RetentionLease(final StreamInput in) throws IOException {
        id = in.readString();
        retainingSequenceNumber = in.readZLong();
        timestamp = in.readVLong();
        source = in.readString();
    }

    /**
     * Writes a retention lease to a stream in a manner suitable for later reconstruction via {@link #RetentionLease(StreamInput)}.
     *
     * @param out the stream to write the retention lease to
     * @throws IOException if an I/O exception occurs writing to the stream
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeZLong(retainingSequenceNumber);
        out.writeVLong(timestamp);
        out.writeString(source);
    }

    /**
     * Encodes a retention lease as a string. This encoding can be decoded by {@link #decodeRetentionLease(String)}. The retention lease is
     * encoded in the format <code>id:{id};retaining_seq_no:{retainingSequenecNumber};timestamp:{timestamp};source:{source}</code>.
     *
     * @param retentionLease the retention lease
     * @return the encoding of the retention lease
     */
    static String encodeRetentionLease(final RetentionLease retentionLease) {
        Objects.requireNonNull(retentionLease);
        return String.format(
                Locale.ROOT,
                "id:%s;retaining_seq_no:%d;timestamp:%d;source:%s",
                retentionLease.id(),
                retentionLease.retainingSequenceNumber(),
                retentionLease.timestamp(),
                retentionLease.source());
    }

    /**
     * Decodes a retention lease encoded by {@link #encodeRetentionLease(RetentionLease)}.
     *
     * @param encodedRetentionLease an encoded retention lease
     * @return the decoded retention lease
     */
    static RetentionLease decodeRetentionLease(final String encodedRetentionLease) {
        Objects.requireNonNull(encodedRetentionLease);
        final String[] fields = encodedRetentionLease.split(";");
        assert fields.length == 4 : Arrays.toString(fields);
        assert fields[0].matches("id:[^:;,]+") : fields[0];
        final String id = fields[0].substring("id:".length());
        assert fields[1].matches("retaining_seq_no:\\d+") : fields[1];
        final long retainingSequenceNumber = Long.parseLong(fields[1].substring("retaining_seq_no:".length()));
        assert fields[2].matches("timestamp:\\d+") : fields[2];
        final long timestamp = Long.parseLong(fields[2].substring("timestamp:".length()));
        assert fields[3].matches("source:[^:;,]+") : fields[3];
        final String source = fields[3].substring("source:".length());
        return new RetentionLease(id, retainingSequenceNumber, timestamp, source);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RetentionLease that = (RetentionLease) o;
        return Objects.equals(id, that.id) &&
                retainingSequenceNumber == that.retainingSequenceNumber &&
                timestamp == that.timestamp &&
                Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, retainingSequenceNumber, timestamp, source);
    }

    @Override
    public String toString() {
        return "RetentionLease{" +
                "id='" + id + '\'' +
                ", retainingSequenceNumber=" + retainingSequenceNumber +
                ", timestamp=" + timestamp +
                ", source='" + source + '\'' +
                '}';
    }

}
