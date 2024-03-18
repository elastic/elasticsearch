/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Contains field inference information. This is necessary to add to cluster state as inference can be calculated in the coordinator
 * node, which not necessarily has mapping information.
 */
public class FieldInferenceMetadata implements Diffable<FieldInferenceMetadata>, ToXContentFragment {

    private final ImmutableOpenMap<String, FieldInferenceOptions> fieldInferenceOptions;

    public static final FieldInferenceMetadata EMPTY = new FieldInferenceMetadata(ImmutableOpenMap.of());

    public FieldInferenceMetadata(MappingLookup mappingLookup) {
        ImmutableOpenMap.Builder<String, FieldInferenceOptions> builder = ImmutableOpenMap.builder();
        mappingLookup.getInferenceIdsForFields().entrySet().forEach(entry -> {
            builder.put(entry.getKey(), new FieldInferenceOptions(entry.getValue(), mappingLookup.sourcePaths(entry.getKey())));
        });
        fieldInferenceOptions = builder.build();
    }

    public FieldInferenceMetadata(StreamInput in) throws IOException {
        fieldInferenceOptions = in.readImmutableOpenMap(StreamInput::readString, FieldInferenceOptions::new);
    }

    public FieldInferenceMetadata(Map<String, FieldInferenceOptions> fieldsToInferenceMap) {
        fieldInferenceOptions = ImmutableOpenMap.builder(fieldsToInferenceMap).build();
    }

    public ImmutableOpenMap<String, FieldInferenceOptions> getFieldInferenceOptions() {
        return fieldInferenceOptions;
    }

    public boolean isEmpty() {
        return fieldInferenceOptions.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(fieldInferenceOptions, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(fieldInferenceOptions);
        return builder;
    }

    public static FieldInferenceMetadata fromXContent(XContentParser parser) throws IOException {
        return new FieldInferenceMetadata(parser.map(HashMap::new, FieldInferenceOptions::fromXContent));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldInferenceMetadata that = (FieldInferenceMetadata) o;
        return Objects.equals(fieldInferenceOptions, that.fieldInferenceOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldInferenceOptions);
    }

    @Override
    public Diff<FieldInferenceMetadata> diff(FieldInferenceMetadata previousState) {
        if (previousState == null) {
            previousState = EMPTY;
        }
        return new FieldInferenceMetadataDiff(previousState, this);
    }

    static class FieldInferenceMetadataDiff implements Diff<FieldInferenceMetadata> {

        public static final FieldInferenceMetadataDiff EMPTY = new FieldInferenceMetadataDiff(
            FieldInferenceMetadata.EMPTY,
            FieldInferenceMetadata.EMPTY
        );

        private final Diff<ImmutableOpenMap<String, FieldInferenceOptions>> fieldInferenceMapDiff;

        private static final DiffableUtils.DiffableValueReader<String, FieldInferenceOptions> FIELD_INFERENCE_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(FieldInferenceOptions::new, FieldInferenceMetadataDiff::readDiffFrom);

        FieldInferenceMetadataDiff(FieldInferenceMetadata before, FieldInferenceMetadata after) {
            fieldInferenceMapDiff = DiffableUtils.diff(
                before.fieldInferenceOptions,
                after.fieldInferenceOptions,
                DiffableUtils.getStringKeySerializer(),
                FIELD_INFERENCE_DIFF_VALUE_READER
            );
        }

        FieldInferenceMetadataDiff(StreamInput in) throws IOException {
            fieldInferenceMapDiff = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                FIELD_INFERENCE_DIFF_VALUE_READER
            );
        }

        public static Diff<FieldInferenceOptions> readDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(FieldInferenceOptions::new, in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            fieldInferenceMapDiff.writeTo(out);
        }

        @Override
        public FieldInferenceMetadata apply(FieldInferenceMetadata part) {
            return new FieldInferenceMetadata(fieldInferenceMapDiff.apply(part.fieldInferenceOptions));
        }
    }

    public record FieldInferenceOptions(String inferenceId, Set<String> sourceFields)
        implements
            SimpleDiffable<FieldInferenceOptions>,
            ToXContentFragment {

        public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
        public static final ParseField SOURCE_FIELDS_FIELD = new ParseField("source_fields");

        FieldInferenceOptions(StreamInput in) throws IOException {
            this(in.readString(), in.readCollectionAsImmutableSet(StreamInput::readString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(inferenceId);
            out.writeStringCollection(sourceFields);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
            builder.field(SOURCE_FIELDS_FIELD.getPreferredName(), sourceFields);
            builder.endObject();
            return builder;
        }

        public static FieldInferenceOptions fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FieldInferenceOptions, Void> PARSER = new ConstructingObjectParser<>(
            "field_inference_parser",
            false,
            (args, unused) -> new FieldInferenceOptions((String) args[0], new HashSet<>((List<String>) args[1]))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INFERENCE_ID_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), SOURCE_FIELDS_FIELD);
        }
    }
}
