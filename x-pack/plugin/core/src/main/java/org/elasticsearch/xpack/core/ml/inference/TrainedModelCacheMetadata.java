/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

public class TrainedModelCacheMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
    public static final String NAME = "trained_model_cache_metadata";
    public static final TrainedModelCacheMetadata EMPTY = new TrainedModelCacheMetadata(0L);
    private static final ParseField VERSION_FIELD = new ParseField("version");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelCacheMetadata, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new TrainedModelCacheMetadata((long) args[0])
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
    }

    public static TrainedModelCacheMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static TrainedModelCacheMetadata fromState(ClusterState clusterState) {
        TrainedModelCacheMetadata cacheMetadata = clusterState.getMetadata().getProject().custom(NAME);
        return cacheMetadata == null ? EMPTY : cacheMetadata;
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, NAME, streamInput);
    }

    private final long version;

    public TrainedModelCacheMetadata(long version) {
        this.version = version;
    }

    public TrainedModelCacheMetadata(StreamInput in) throws IOException {
        this.version = in.readVLong();
    }

    public long version() {
        return version;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single(((builder, params) -> { return builder.field(VERSION_FIELD.getPreferredName(), version); }));
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelCacheMetadata that = (TrainedModelCacheMetadata) o;
        return Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version);
    }
}
