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

/**
 * A "shard history retention lease" (or "retention lease" for short) is conceptually a marker containing a retaining sequence number such
 * that all operations with sequence number at least that retaining sequence number will be retained during merge operations (which could
 * otherwise merge away operations that have been soft deleted). Each retention lease contains a unique identifier, the retaining sequence
 * number, the timestamp of when the lease was created or renewed, and the source of the retention lease (e.g., "ccr").
 */
public final class RetentionLease {

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
        this.id = id;
        this.retainingSequenceNumber = retainingSequenceNumber;
        this.timestamp = timestamp;
        this.source = source;
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
