/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TrainedModelMetadata implements Metadata.Custom {

    public static final String NAME = "trained_model_metadata";

    public static final TrainedModelMetadata EMPTY = new TrainedModelMetadata(new HashMap<>());
    private static final ParseField MODELS = new ParseField("models");
    private static final ParseField MODEL_ID = new ParseField("model_id");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelMetadata, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new TrainedModelMetadata((Map<String, TrainedModelMetadataEntry>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            List<TrainedModelMetadataEntry> models = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                models.add(TrainedModelMetadataEntry.fromXContent(p));
            }
            return models.stream().collect(Collectors.toMap(TrainedModelMetadataEntry::getModelId, Function.identity()));
        }, MODELS);
    }

    public static TrainedModelMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new TrainedModelMetadataDiff(in);
    }

    private final Map<String, TrainedModelMetadataEntry> models;

    public TrainedModelMetadata(Map<String, TrainedModelMetadataEntry> models) {
        this.models = models;
    }

    public TrainedModelMetadata(StreamInput in) throws IOException {
        this.models = in.readImmutableMap(TrainedModelMetadataEntry::new);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentValuesMap(MODELS.getPreferredName(), models);
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
        out.writeMap(this.models, StreamOutput::writeWriteable);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new TrainedModelMetadataDiff((TrainedModelMetadata) previousState, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelMetadata that = (TrainedModelMetadata) o;
        return Objects.equals(models, that.models);
    }

    @Override
    public int hashCode() {
        return Objects.hash(models);
    }

    public static class TrainedModelMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, TrainedModelMetadataEntry>> modelsDiff;

        TrainedModelMetadataDiff(TrainedModelMetadata before, TrainedModelMetadata after) {
            this.modelsDiff = DiffableUtils.diff(before.models, after.models, DiffableUtils.getStringKeySerializer());
        }

        TrainedModelMetadataDiff(StreamInput in) throws IOException {
            this.modelsDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                TrainedModelMetadataEntry::new,
                TrainedModelMetadataEntry::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new TrainedModelMetadata(modelsDiff.apply(((TrainedModelMetadata) part).models));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelsDiff.writeTo(out);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            // TODO: Add a new entry in TransportVersions before merge.
            return TransportVersion.current();
        }
    }
    public static class TrainedModelMetadataEntry implements SimpleDiffable<TrainedModelMetadataEntry>, ToXContentObject {
        private static final ConstructingObjectParser<TrainedModelMetadataEntry, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_metadata_entry",
            true,
            args -> new TrainedModelMetadataEntry((String) args[0])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        }

        private static Diff<TrainedModelMetadataEntry> readDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(TrainedModelMetadataEntry::new, in);
        }

        private static TrainedModelMetadataEntry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String modelId;

        public TrainedModelMetadataEntry(String modelId) {
            this.modelId = modelId;
        }

        TrainedModelMetadataEntry(StreamInput in) throws IOException {
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
            TrainedModelMetadataEntry that = (TrainedModelMetadataEntry) o;
            return Objects.equals(modelId, that.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
        }

        @Override
        public String toString() {
            return "TrainedModelMetadataEntry{modelId='" + modelId + "'}";
        }
    }
}
