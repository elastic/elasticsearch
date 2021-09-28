/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Custom {@link Metadata} implementation for storing a map of model aliases that point to model IDs
 */
public class ModelAliasMetadata implements Metadata.Custom {

    public static final String NAME = "trained_model_alias";

    public static final ModelAliasMetadata EMPTY = new ModelAliasMetadata(new HashMap<>());

    public static ModelAliasMetadata fromState(ClusterState cs) {
        ModelAliasMetadata modelAliasMetadata = cs.metadata().custom(NAME);
        return modelAliasMetadata == null ? EMPTY : modelAliasMetadata;
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ModelAliasMetadataDiff(in);
    }

    private static final ParseField MODEL_ALIASES = new ParseField("model_aliases");
    private static final ParseField MODEL_ID = new ParseField("model_id");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ModelAliasMetadata, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        // to protect BWC serialization
        true,
        args -> new ModelAliasMetadata((Map<String, ModelAliasEntry>)args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, ModelAliasEntry> modelAliases = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String modelAlias = p.currentName();
                modelAliases.put(modelAlias, ModelAliasEntry.fromXContent(p));
            }
            return modelAliases;
        }, MODEL_ALIASES);
    }

    public static ModelAliasMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Map<String, ModelAliasEntry> modelAliases;

    public ModelAliasMetadata(Map<String, ModelAliasEntry> modelAliases) {
        this.modelAliases = Collections.unmodifiableMap(modelAliases);
    }

    public ModelAliasMetadata(StreamInput in) throws IOException {
        this.modelAliases = Collections.unmodifiableMap(in.readMap(StreamInput::readString, ModelAliasEntry::new));
    }

    public Map<String, ModelAliasEntry> modelAliases() {
        return modelAliases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(MODEL_ALIASES.getPreferredName());
        for (Map.Entry<String, ModelAliasEntry> modelAliasEntry : modelAliases.entrySet()) {
            builder.field(modelAliasEntry.getKey(), modelAliasEntry.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new ModelAliasMetadataDiff((ModelAliasMetadata) previousState, this);
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
    public Version getMinimalSupportedVersion() {
        return Version.V_7_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.modelAliases, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public String getModelId(String modelAlias) {
        ModelAliasEntry entry = this.modelAliases.get(modelAlias);
        if (entry == null) {
            return null;
        }
        return entry.modelId;
    }

    static class ModelAliasMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, ModelAliasEntry>> modelAliasesDiff;

        ModelAliasMetadataDiff(ModelAliasMetadata before, ModelAliasMetadata after) {
            this.modelAliasesDiff = DiffableUtils.diff(before.modelAliases, after.modelAliases, DiffableUtils.getStringKeySerializer());
        }

        ModelAliasMetadataDiff(StreamInput in) throws IOException {
            this.modelAliasesDiff = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                ModelAliasEntry::new, ModelAliasEntry::readDiffFrom);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ModelAliasMetadata(modelAliasesDiff.apply(((ModelAliasMetadata) part).modelAliases));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelAliasesDiff.writeTo(out);
        }
    }

    public static class ModelAliasEntry extends AbstractDiffable<ModelAliasEntry> implements ToXContentObject {
        private static final ConstructingObjectParser<ModelAliasEntry, Void> PARSER = new ConstructingObjectParser<>(
            "model_alias_metadata_alias_entry",
            // to protect BWC serialization
            true,
            args -> new ModelAliasEntry((String)args[0])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        }

        private static Diff<ModelAliasEntry> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(ModelAliasEntry::new, in);
        }

        private static ModelAliasEntry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String modelId;

        public ModelAliasEntry(String modelId) {
            this.modelId = modelId;
        }

        ModelAliasEntry(StreamInput in) throws IOException {
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
            ModelAliasEntry modelAliasEntry = (ModelAliasEntry) o;
            return Objects.equals(modelId, modelAliasEntry.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
        }
    }
}
