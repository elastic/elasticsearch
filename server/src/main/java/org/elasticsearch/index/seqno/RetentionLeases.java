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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Represents a versioned collection of retention leases. We version the collection of retention leases to ensure that sync requests that
 * arrive out of order on the replica, using the version to ensure that older sync requests are rejected.
 */
public class RetentionLeases implements Writeable {

    private final long version;

    /**
     * The version of this retention lease collection. The version is managed on the primary and incremented any time that a retention lease
     * is added, renewed, or when retention leases expire.
     *
     * @return the version of this retention lease collection
     */
    public long version() {
        return version;
    }

    private final Collection<RetentionLease> retentionLeases;

    /**
     * The underlying collection of retention leases
     *
     * @return the retention leases
     */
    public Collection<RetentionLease> retentionLeases() {
        return retentionLeases;
    }

    /**
     * Represents an empty an un-versioned retention lease collection. This is used when no retention lease collection is found in the
     * commit point
     */
    public static RetentionLeases EMPTY = new RetentionLeases(0, Collections.emptyList());

    /**
     * Constructs a new retention lease collection with the specified version and underlying collection of retention leases.
     *
     * @param version         the version of this retention lease collection
     * @param retentionLeases the retention leases
     */
    public RetentionLeases(final long version, final Collection<RetentionLease> retentionLeases) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative but was [" + version + "]");
        }
        Objects.requireNonNull(retentionLeases);
        this.version = version;
        this.retentionLeases = Collections.unmodifiableCollection(new ArrayList<>(retentionLeases));
    }

    /**
     * Constructs a new retention lease collection from a stream. The retention lease collection should have been written via
     * {@link #writeTo(StreamOutput)}.
     *
     * @param in the stream to construct the retention lease collection from
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public RetentionLeases(final StreamInput in) throws IOException {
        version = in.readVLong();
        retentionLeases = in.readList(RetentionLease::new);
    }

    /**
     * Writes a retention lease collection to a stream in a manner suitable for later reconstruction via
     * {@link #RetentionLeases(StreamInput)} (StreamInput)}.
     *
     * @param out the stream to write the retention lease collection to
     * @throws IOException if an I/O exception occurs writing to the stream
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVLong(version);
        out.writeCollection(retentionLeases);
    }

    @Override
    public String toString() {
        return "RetentionLeases{" +
                "version=" + version +
                ", retentionLeases=" + retentionLeases +
                '}';
    }

}
