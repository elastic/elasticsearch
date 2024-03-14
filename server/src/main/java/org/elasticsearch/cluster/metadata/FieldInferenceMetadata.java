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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Contains field inference information. This is necessary to add to cluster state as inference can be calculated in the coordinator
 * node, which not necessarily has mapping information.
 */
public class FieldInferenceMetadata implements Diffable<FieldInferenceMetadata>, ToXContentFragment {

    private final ImmutableOpenMap<String, FieldInference> fieldInferenceMap;

    // Contains a lazily cached, reversed map of inferenceId -> fields
    private volatile Map<String, Set<String>> fieldsForInferenceIds;

    public static final FieldInferenceMetadata EMPTY = new FieldInferenceMetadata(ImmutableOpenMap.of());

    public FieldInferenceMetadata(MappingLookup mappingLookup) {
        ImmutableOpenMap.Builder<String, FieldInference> builder = ImmutableOpenMap.builder();
        mappingLookup.getInferenceIdsForFields().entrySet().forEach(entry -> {
            builder.put(entry.getKey(), new FieldInference(entry.getValue(), mappingLookup.sourcePaths(entry.getKey())));
        });
        fieldInferenceMap = builder.build();
    }

    public FieldInferenceMetadata(StreamInput in) throws IOException {
        fieldInferenceMap = in.readImmutableOpenMap(StreamInput::readString, FieldInference::new);
    }

    public FieldInferenceMetadata(Map<String, FieldInference> fieldsToInferenceMap) {
        fieldInferenceMap = ImmutableOpenMap.builder(fieldsToInferenceMap).build();
    }

    public boolean isEmpty() {
        return fieldInferenceMap.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(fieldInferenceMap, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(fieldInferenceMap);
        return builder;
    }

    public static FieldInferenceMetadata fromXContent(XContentParser parser) throws IOException {
        return new FieldInferenceMetadata(parser.map(HashMap::new, FieldInference::fromXContent));
    }

    public String getInferenceIdForField(String field) {
        return getInferenceSafe(field, FieldInference::inferenceId);
    }

    private <T> T getInferenceSafe(String field, Function<FieldInference, T> fieldInferenceFunction) {
        FieldInference fieldInference = fieldInferenceMap.get(field);
        if (fieldInference == null) {
            return null;
        }
        return fieldInferenceFunction.apply(fieldInference);
    }

    public Set<String> getSourceFields(String field) {
        return getInferenceSafe(field, FieldInference::sourceFields);
    }

    public Map<String, Set<String>> getFieldsForInferenceIds() {
        if (fieldsForInferenceIds != null) {
            return fieldsForInferenceIds;
        }

        // Cache the result as a field
        Map<String, Set<String>> fieldsForInferenceIdsMap = new HashMap<>();
        for (Map.Entry<String, FieldInference> entry : fieldInferenceMap.entrySet()) {
            String fieldName = entry.getKey();
            String inferenceId = entry.getValue().inferenceId();

            // Get or create the set associated with the inferenceId
            Set<String> fields = fieldsForInferenceIdsMap.computeIfAbsent(inferenceId, k -> new HashSet<>());
            fields.add(fieldName);
        }

        fieldsForInferenceIds = Collections.unmodifiableMap(fieldsForInferenceIdsMap);
        return fieldsForInferenceIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldInferenceMetadata that = (FieldInferenceMetadata) o;
        return Objects.equals(fieldInferenceMap, that.fieldInferenceMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldInferenceMap);
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

        private final Diff<ImmutableOpenMap<String, FieldInference>> fieldInferenceMapDiff;

        private static final DiffableUtils.DiffableValueReader<String, FieldInference> FIELD_INFERENCE_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(FieldInference::new, FieldInferenceMetadataDiff::readDiffFrom);

        FieldInferenceMetadataDiff(FieldInferenceMetadata before, FieldInferenceMetadata after) {
            fieldInferenceMapDiff = DiffableUtils.diff(
                before.fieldInferenceMap,
                after.fieldInferenceMap,
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

        public static Diff<FieldInference> readDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(FieldInference::new, in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            fieldInferenceMapDiff.writeTo(out);
        }

        @Override
        public FieldInferenceMetadata apply(FieldInferenceMetadata part) {
            return new FieldInferenceMetadata(fieldInferenceMapDiff.apply(part.fieldInferenceMap));
        }
    }

    public record FieldInference(String inferenceId, Set<String> sourceFields)
        implements
            SimpleDiffable<FieldInference>,
            ToXContentFragment {

        public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
        public static final ParseField SOURCE_FIELDS_FIELD = new ParseField("source_fields");

        FieldInference(StreamInput in) throws IOException {
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

        public static FieldInference fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FieldInference, Void> PARSER = new ConstructingObjectParser<>(
            "field_inference_parser",
            false,
            (args, unused) -> new FieldInference((String) args[0], new HashSet<>((List<String>) args[1]))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INFERENCE_ID_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), SOURCE_FIELDS_FIELD);
        }
    }
}
