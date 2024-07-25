/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class RegisteredSnapshots implements Metadata.Custom {

    public static final String TYPE = "registered_snapshots";
    private static final ParseField SNAPSHOTS = new ParseField("snapshots");
    public static final RegisteredSnapshots EMPTY = new RegisteredSnapshots(Map.of());
    public static final int MAX_REGISTERED_SNAPSHOTS = 100;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RegisteredSnapshots, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        a -> new RegisteredSnapshots((Map<String, List<SnapshotId>>) a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.list(), SNAPSHOTS);
    }

    private final Map<String, List<SnapshotId>> snapshots;

    public RegisteredSnapshots(Map<String, List<SnapshotId>> snapshots) {
        this.snapshots = Collections.unmodifiableMap(snapshots);
    }

    public RegisteredSnapshots(StreamInput in) throws IOException {
        this.snapshots = in.readMapOfLists(SnapshotId::new);
    }

    public Map<String, List<SnapshotId>> getSnapshots() {
        return snapshots;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new RegisteredSnapshotsDiff((RegisteredSnapshots) previousState, this);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.PRE_REGISTER_SLM_STATS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(snapshots, StreamOutput::writeCollection);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((builder, params) -> {
                builder.field(SNAPSHOTS.getPreferredName(), snapshots);
                return builder;
            })
        );
    }

    @Override
    public String toString() {
        return "RegisteredSnapshots{" +
            "snapshots=" + snapshots +
            '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RegisteredSnapshots other = (RegisteredSnapshots) obj;
        return Objects.equals(snapshots, other.snapshots);
    }

    public static class RegisteredSnapshotsDiff implements NamedDiff<Metadata.Custom> {
        final Map<String, List<SnapshotId>> snapshots;
        RegisteredSnapshotsDiff(RegisteredSnapshots before, RegisteredSnapshots after) {
            this.snapshots = after.snapshots;
        }
        public RegisteredSnapshotsDiff(StreamInput in) throws IOException {
            this.snapshots = new RegisteredSnapshots(in).snapshots;
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new RegisteredSnapshots(snapshots);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(snapshots, StreamOutput::writeCollection);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.PRE_REGISTER_SLM_STATS;
        }

    }
}
