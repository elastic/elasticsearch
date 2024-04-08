/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a versioned collection of retention leases. We version the collection of retention leases to ensure that sync requests that
 * arrive out of order on the replica, using the version to ensure that older sync requests are rejected.
 */
public class RetentionLeases implements ToXContentFragment, Writeable {

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

    /**
     * Checks if this retention leases collection supersedes the specified retention leases collection. A retention leases collection
     * supersedes another retention leases collection if its primary term is higher, or if for equal primary terms its version is higher.
     *
     * @param that the retention leases collection to test against
     * @return true if this retention leases collection supercedes the specified retention lease collection, otherwise false
     */
    boolean supersedes(final RetentionLeases that) {
        return supersedes(that.primaryTerm, that.version);
    }

    /**
     * Checks if this retention leases collection would supersede a retention leases collection with the specified primary term and version.
     * A retention leases collection supersedes another retention leases collection if its primary term is higher, or if for equal primary
     * terms its version is higher.
     *
     * @param primaryTerm the primary term
     * @param version     the version
     * @return true if this retention leases collection would supercedes a retention lease collection with the specified primary term and
     * version
     */
    boolean supersedes(final long primaryTerm, final long version) {
        return this.primaryTerm > primaryTerm || this.primaryTerm == primaryTerm && this.version > version;
    }

    private final Map<String, RetentionLease> leases;

    /**
     * The underlying collection of retention leases
     *
     * @return the retention leases
     */
    public Collection<RetentionLease> leases() {
        return leases.values();
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
    public static RetentionLeases EMPTY = new RetentionLeases(1, 0, Collections.emptyList());

    /**
     * Constructs a new retention lease collection with the specified version and underlying collection of retention leases.
     *
     * @param primaryTerm the primary term under which this retention lease collection was created
     * @param version the version of this retention lease collection
     * @param leases  the retention leases
     */
    public RetentionLeases(final long primaryTerm, final long version, final List<RetentionLease> leases) {
        if (primaryTerm <= 0) {
            throw new IllegalArgumentException("primary term must be positive but was [" + primaryTerm + "]");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative but was [" + version + "]");
        }
        Objects.requireNonNull(leases);
        this.primaryTerm = primaryTerm;
        this.version = version;
        final int leasesCount = leases.size();
        switch (leasesCount) {
            case 0 -> this.leases = Map.of();
            case 1 -> {
                final RetentionLease lease = leases.get(0);
                this.leases = Map.of(lease.id(), lease);
            }
            default -> {
                // use a linked hash map to preserve order
                final LinkedHashMap<String, RetentionLease> map = Maps.newLinkedHashMapWithExpectedSize(leasesCount);
                for (RetentionLease lease : leases) {
                    final RetentionLease existing = map.put(lease.id(), lease);
                    if (existing != null) {
                        throwOnIdCollision(existing, lease);
                    }
                }
                this.leases = Collections.unmodifiableMap(map);
            }
        }
    }

    private static void throwOnIdCollision(RetentionLease existing, RetentionLease added) {
        assert existing.id().equals(added.id()) : "expected [" + existing.id() + "] to equal [" + added.id() + "]";
        throw new IllegalStateException("duplicate retention lease ID [" + existing.id() + "]");
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
        final int leasesCount = in.readVInt();
        switch (leasesCount) {
            case 0 -> leases = Map.of();
            case 1 -> {
                final RetentionLease lease = new RetentionLease(in);
                leases = Map.of(lease.id(), lease);
            }
            default -> {
                final Map<String, RetentionLease> m = Maps.newLinkedHashMapWithExpectedSize(leasesCount);
                for (int i = 0; i < leasesCount; i++) {
                    final RetentionLease lease = new RetentionLease(in);
                    m.put(lease.id(), lease);
                }
                leases = Collections.unmodifiableMap(m);
            }
        }
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

    private static final ParseField PRIMARY_TERM_FIELD = new ParseField("primary_term");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField LEASES_FIELD = new ParseField("leases");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RetentionLeases, Void> PARSER = new ConstructingObjectParser<>(
        "retention_leases",
        (a) -> new RetentionLeases((Long) a[0], (Long) a[1], (List<RetentionLease>) a[2])
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PRIMARY_TERM_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> RetentionLease.fromXContent(p), LEASES_FIELD);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.field(PRIMARY_TERM_FIELD.getPreferredName(), primaryTerm);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.startArray(LEASES_FIELD.getPreferredName());
        {
            for (final RetentionLease retentionLease : leases.values()) {
                retentionLease.toXContent(builder, params);
            }
        }
        builder.endArray();
        return builder;
    }

    /**
     * Parses a retention leases collection from {@link org.elasticsearch.xcontent.XContent}. This method assumes that the retention
     * leases were converted to {@link org.elasticsearch.xcontent.XContent} via {@link #toXContent(XContentBuilder, Params)}.
     *
     * @param parser the parser
     * @return a retention leases collection
     */
    public static RetentionLeases fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static final MetadataStateFormat<RetentionLeases> FORMAT = new MetadataStateFormat<>("retention-leases-") {

        @Override
        public void toXContent(final XContentBuilder builder, final RetentionLeases retentionLeases) throws IOException {
            retentionLeases.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        @Override
        public RetentionLeases fromXContent(final XContentParser parser) {
            return RetentionLeases.fromXContent(parser);
        }

    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RetentionLeases that = (RetentionLeases) o;
        return primaryTerm == that.primaryTerm && version == that.version && Objects.equals(leases, that.leases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, version, leases);
    }

    @Override
    public String toString() {
        return "RetentionLeases{primaryTerm=" + primaryTerm + ", version=" + version + ", leases=" + leases + '}';
    }

}
