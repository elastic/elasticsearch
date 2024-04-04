/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TrainedModelCacheMetadata implements Metadata.Custom {

    public static final String NAME = "trained_model_cache_metadata";

    public static final TrainedModelCacheMetadata EMPTY = new TrainedModelCacheMetadata(new HashMap<>());
    private static final ParseField ENTRIES = new ParseField("entries");
    private static final ParseField MODEL_ID = new ParseField("model_id");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelCacheMetadata, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new TrainedModelCacheMetadata((Map<String, TrainedModelCustomMetadataEntry>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, TrainedModelCustomMetadataEntry> entries = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String modelId = p.currentName();
                entries.put(modelId, TrainedModelCustomMetadataEntry.fromXContent(p));
            }
            return entries;
        }, ENTRIES);
    }

    public static TrainedModelCacheMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static TrainedModelCacheMetadata fromState(ClusterState clusterState) {
        TrainedModelCacheMetadata cacheMetadata = clusterState.getMetadata().custom(NAME);
        return cacheMetadata == null ? EMPTY : cacheMetadata;
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new TrainedModelCacheMetadataDiff(in);
    }

    public static Set<String> getUpdatedModelIds(ClusterChangedEvent event) {
        if (event.changedCustomMetadataSet().contains(TrainedModelCacheMetadata.NAME) == false) {
            return Collections.emptySet();
        }

        Map<String, TrainedModelCustomMetadataEntry> oldCacheMetadataEntries = TrainedModelCacheMetadata.fromState(event.previousState()).entries();
        Map<String, TrainedModelCustomMetadataEntry> newCacheMetadataEntries = TrainedModelCacheMetadata.fromState(event.state()).entries();

        return Sets.union(oldCacheMetadataEntries.keySet(), newCacheMetadataEntries.keySet()).stream()
            .filter(modelId -> {
                if ((oldCacheMetadataEntries.containsKey(modelId) && newCacheMetadataEntries.containsKey(modelId)) == false) {
                    return true;
                }

                return Objects.equals(oldCacheMetadataEntries.get(modelId), newCacheMetadataEntries.get(modelId)) == false;
            })
            .collect(Collectors.toSet());
    }

    private final Map<String, TrainedModelCustomMetadataEntry> entries;

    public TrainedModelCacheMetadata(Map<String, TrainedModelCustomMetadataEntry> entries) {
        this.entries = entries;
    }

    public TrainedModelCacheMetadata(StreamInput in) throws IOException {
        this.entries = in.readImmutableMap(TrainedModelCustomMetadataEntry::new);
    }

    public Map<String, TrainedModelCustomMetadataEntry> entries() {
        return entries;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentValuesMap(ENTRIES.getPreferredName(), entries);
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
        // TODO: Add a new entry in TransportVersions before merge.
        return TransportVersion.current();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.entries, StreamOutput::writeWriteable);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new TrainedModelCacheMetadataDiff((TrainedModelCacheMetadata) previousState, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelCacheMetadata that = (TrainedModelCacheMetadata) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    public static class TrainedModelCacheMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, TrainedModelCustomMetadataEntry>> entriesDiff;

        TrainedModelCacheMetadataDiff(TrainedModelCacheMetadata before, TrainedModelCacheMetadata after) {
            this.entriesDiff = DiffableUtils.diff(before.entries, after.entries, DiffableUtils.getStringKeySerializer());
        }

        TrainedModelCacheMetadataDiff(StreamInput in) throws IOException {
            this.entriesDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                TrainedModelCustomMetadataEntry::new,
                TrainedModelCustomMetadataEntry::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new TrainedModelCacheMetadata(entriesDiff.apply(((TrainedModelCacheMetadata) part).entries));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            entriesDiff.writeTo(out);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            // TODO: Add a new entry in TransportVersions before merge.
            return TransportVersion.current();
        }
    }
    public static class TrainedModelCustomMetadataEntry implements SimpleDiffable<TrainedModelCustomMetadataEntry>, ToXContentObject {
        private static final ConstructingObjectParser<TrainedModelCustomMetadataEntry, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_cache_metadata_entry",
            true,
            args -> new TrainedModelCustomMetadataEntry((String) args[0])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        }

        private static Diff<TrainedModelCustomMetadataEntry> readDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(TrainedModelCustomMetadataEntry::new, in);
        }

        private static TrainedModelCustomMetadataEntry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String modelId;

        public TrainedModelCustomMetadataEntry(String modelId) {
            this.modelId = modelId;
        }

        TrainedModelCustomMetadataEntry(StreamInput in) throws IOException {
            this.modelId = in.readString();
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID.getPreferredName(), modelId);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrainedModelCustomMetadataEntry that = (TrainedModelCustomMetadataEntry) o;
            return Objects.equals(modelId, that.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
        }

        @Override
        public String toString() {
            return "TrainedModelCacheMetadataEntry{modelId='" + modelId + "'}";
        }
    }
}
