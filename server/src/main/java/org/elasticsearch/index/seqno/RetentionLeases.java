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
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a versioned collection of retention leases. We version the collection of retention leases to ensure that sync requests that
 * arrive out of order on the replica, using the version to ensure that older sync requests are rejected.
 */
public class RetentionLeases implements Writeable {

    private final long primaryTerm;

    /**
     * The primary term of this retention lease collection.
     *
     * @return the primary term
     */
    public long primaryTerm() {
        return primaryTerm;
    }

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

    public boolean supersedes(final RetentionLeases that) {
        return primaryTerm() > that.primaryTerm() || primaryTerm() <= that.primaryTerm() && version() > that.version();
    }

    private final Map<String, RetentionLease> leases;

    /**
     * The underlying collection of retention leases
     *
     * @return the retention leases
     */
    public Collection<RetentionLease> leases() {
        return Collections.unmodifiableCollection(leases.values());
    }

    /**
     * Checks if this retention lease collection contains a retention lease with the specified {@link RetentionLease#id()}.
     *
     * @param id the retention lease ID
     * @return true if this retention lease collection contains a retention lease with the specified ID, otherwise false
     */
    public boolean contains(final String id) {
        return leases.containsKey(id);
    }

    /**
     * Returns the retention lease with the specified ID, or null if no such retention lease exists.
     *
     * @param id the retention lease ID
     * @return the retention lease, or null if no retention lease with the specified ID exists
     */
    public RetentionLease get(final String id) {
        return leases.get(id);
    }

    /**
     * Represents an empty an un-versioned retention lease collection. This is used when no retention lease collection is found in the
     * commit point
     */
    public static RetentionLeases EMPTY = new RetentionLeases(0, 0, Collections.emptyList());

    /**
     * Constructs a new retention lease collection with the specified version and underlying collection of retention leases.
     *
     * @param primaryTerm the primary term under which this retention lease collection was created
     * @param version the version of this retention lease collection
     * @param leases  the retention leases
     */
    public RetentionLeases(final long primaryTerm, final long version, final Collection<RetentionLease> leases) {
        if (primaryTerm < 0) {
            throw new IllegalArgumentException("primary term must be non-negative but was [" + primaryTerm + "]");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative but was [" + version + "]");
        }
        Objects.requireNonNull(leases);
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.leases = Collections.unmodifiableMap(toMap(leases));
    }

    /**
     * Constructs a new retention lease collection from a stream. The retention lease collection should have been written via
     * {@link #writeTo(StreamOutput)}.
     *
     * @param in the stream to construct the retention lease collection from
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public RetentionLeases(final StreamInput in) throws IOException {
        primaryTerm = in.readVLong();
        version = in.readVLong();
        leases = Collections.unmodifiableMap(toMap(in.readList(RetentionLease::new)));
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
        out.writeVLong(primaryTerm);
        out.writeVLong(version);
        out.writeCollection(leases.values());
    }

    /**
     * Encodes a retention lease collection as a string. This encoding can be decoded by
     * {@link RetentionLeases#decodeRetentionLeases(String)}. The encoding is a comma-separated encoding of each retention lease as encoded
     * by {@link RetentionLease#encodeRetentionLease(RetentionLease)}, prefixed by the version of the retention lease collection.
     *
     * @param retentionLeases the retention lease collection
     * @return the encoding of the retention lease collection
     */
    public static String encodeRetentionLeases(final RetentionLeases retentionLeases) {
        Objects.requireNonNull(retentionLeases);
        return String.format(
                Locale.ROOT,
                "primary_term:%d;version:%d;%s",
                retentionLeases.primaryTerm(),
                retentionLeases.version(),
                retentionLeases.leases().stream().map(RetentionLease::encodeRetentionLease).collect(Collectors.joining(",")));
    }

    /**
     * Decodes retention leases encoded by {@link #encodeRetentionLeases(RetentionLeases)}.
     *
     * @param encodedRetentionLeases an encoded retention lease collection
     * @return the decoded retention lease collection
     */
    public static RetentionLeases decodeRetentionLeases(final String encodedRetentionLeases) {
        Objects.requireNonNull(encodedRetentionLeases);
        if (encodedRetentionLeases.isEmpty()) {
            return EMPTY;
        }
        assert encodedRetentionLeases.matches("primary_term:\\d+;version:\\d+;.*") : encodedRetentionLeases;
        final int firstSemicolon = encodedRetentionLeases.indexOf(";");
        final long primaryTerm = Long.parseLong(encodedRetentionLeases.substring("primary_term:".length(), firstSemicolon));
        final int secondSemicolon = encodedRetentionLeases.indexOf(";", firstSemicolon + 1);
        final long version = Long.parseLong(encodedRetentionLeases.substring(firstSemicolon + 1 + "version:".length(), secondSemicolon));
        final Collection<RetentionLease> retentionLeases;
        if (secondSemicolon + 1 == encodedRetentionLeases.length()) {
            retentionLeases = Collections.emptyList();
        } else {
            assert Arrays.stream(encodedRetentionLeases.substring(secondSemicolon + 1).split(","))
                    .allMatch(s -> s.matches("id:[^:;,]+;retaining_seq_no:\\d+;timestamp:\\d+;source:[^:;,]+"))
                    : encodedRetentionLeases;
            retentionLeases = Arrays.stream(encodedRetentionLeases.substring(secondSemicolon + 1).split(","))
                    .map(RetentionLease::decodeRetentionLease)
                    .collect(Collectors.toList());
        }

        return new RetentionLeases(primaryTerm, version, retentionLeases);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RetentionLeases that = (RetentionLeases) o;
        return primaryTerm == that.primaryTerm &&
                version == that.version &&
                Objects.equals(leases, that.leases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, version, leases);
    }

    @Override
    public String toString() {
        return "RetentionLeases{" +
                "primaryTerm=" + primaryTerm +
                ", version=" + version +
                ", leases=" + leases +
                '}';
    }

    /**
     * A utility method to convert retention leases to a map from retention lease ID to retention lease.
     *
     * @param leases the retention leases
     * @return the map from retention lease ID to retention lease
     */
    private static Map<String, RetentionLease> toMap(final Collection<RetentionLease> leases) {
        return leases.stream().collect(Collectors.toMap(RetentionLease::id, Function.identity()));
    }

    /**
     * A utility method to convert a retention lease collection to a map from retention lease ID to retention lease.
     *
     * @param retentionLeases the retention lease collection
     * @return the map from retention lease ID to retention lease
     */
    static Map<String, RetentionLease> toMap(final RetentionLeases retentionLeases) {
        return retentionLeases.leases;
    }

}
