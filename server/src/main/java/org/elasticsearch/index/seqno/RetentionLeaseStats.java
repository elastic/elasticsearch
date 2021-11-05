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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents retention lease stats.
 */
public final class RetentionLeaseStats implements ToXContentFragment, Writeable {

    private final RetentionLeases retentionLeases;

    /**
     * The underlying retention lease collection backing this stats object.
     *
     * @return the retention lease collection
     */
    public RetentionLeases retentionLeases() {
        return retentionLeases;
    }

    /**
     * Constructs a new retention lease stats object from the specified retention lease collection.
     *
     * @param retentionLeases the retention lease collection
     */
    public RetentionLeaseStats(final RetentionLeases retentionLeases) {
        this.retentionLeases = Objects.requireNonNull(retentionLeases);
    }

    /**
     * Constructs a new retention lease stats object from a stream. The retention lease stats should have been written via
     * {@link #writeTo(StreamOutput)}.
     *
     * @param in the stream to construct the retention lease stats from
     * @throws IOException if an I/O exception occurs reading from the stream
     */
    public RetentionLeaseStats(final StreamInput in) throws IOException {
        retentionLeases = new RetentionLeases(in);
    }

    /**
     * Writes a retention lease stats object to a stream in a manner suitable for later reconstruction via
     * {@link #RetentionLeaseStats(StreamInput)} (StreamInput)}.
     *
     * @param out the stream to write the retention lease stats to
     * @throws IOException if an I/O exception occurs writing to the stream
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        retentionLeases.writeTo(out);
    }

    /**
     * Converts the retention lease stats to {@link org.elasticsearch.xcontent.XContent} using the specified builder and pararms.
     *
     * @param builder the builder
     * @param params  the params
     * @return the builder that this retention lease collection was converted to {@link org.elasticsearch.xcontent.XContent} into
     * @throws IOException if an I/O exception occurs writing to the builder
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject("retention_leases");
        {
            builder.field("primary_term", retentionLeases.primaryTerm());
            builder.field("version", retentionLeases.version());
            builder.startArray("leases");
            {
                for (final RetentionLease retentionLease : retentionLeases.leases()) {
                    builder.startObject();
                    {
                        builder.field("id", retentionLease.id());
                        builder.field("retaining_seq_no", retentionLease.retainingSequenceNumber());
                        builder.field("timestamp", retentionLease.timestamp());
                        builder.field("source", retentionLease.source());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RetentionLeaseStats that = (RetentionLeaseStats) o;
        return Objects.equals(retentionLeases, that.retentionLeases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(retentionLeases);
    }

}
