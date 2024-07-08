/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * SnapshotId - snapshot name + snapshot UUID
 */
public final class SnapshotId implements Comparable<SnapshotId>, Writeable, ToXContentObject {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField UUID = new ParseField("uuid");

    public static final ConstructingObjectParser<SnapshotId, String> PARSER = new ConstructingObjectParser<>(
        "snapshot_id",
        true,
        a -> new SnapshotId((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), UUID);
    }

    private final String name;
    private final String uuid;

    // Caching hash code
    private final int hashCode;

    /**
     * Constructs a new snapshot
     *
     * @param name   snapshot name
     * @param uuid   snapshot uuid
     */
    public SnapshotId(final String name, final String uuid) {
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a new snapshot from a input stream
     *
     * @param in  input stream
     */
    public SnapshotId(final StreamInput in) throws IOException {
        name = in.readString();
        uuid = in.readString();
        hashCode = computeHashCode();
    }

    public static SnapshotId parse(XContentParser parser, String text) {
        return PARSER.apply(parser, text);
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the snapshot UUID
     *
     * @return snapshot uuid
     */
    public String getUUID() {
        return uuid;
    }

    @Override
    public String toString() {
        return name + "/" + uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SnapshotId that = (SnapshotId) o;
        return name.equals(that.name) && uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public int compareTo(final SnapshotId other) {
        return this.name.compareTo(other.name);
    }

    private int computeHashCode() {
        return Objects.hash(name, uuid);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(UUID.getPreferredName(), uuid);
        builder.endObject();
        return builder;
    }
}
