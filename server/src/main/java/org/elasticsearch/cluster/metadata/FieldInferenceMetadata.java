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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains field inference information. This is necessary to add to cluster state as inference can be calculated in the coordinator
 * node, which not necessarily has mapping information.
 */
public class FieldInferenceMetadata implements Diffable<FieldInferenceMetadata>, ToXContentFragment {

    // Keys: field names. Values: Inference ID associated to the field name for calculating inference
    private final ImmutableOpenMap<String, String> inferenceIdForField;

    // Keys: field names. Values: Field names that provide source for this field (either as copy_to or multifield sources)
    private final ImmutableOpenMap<String, Set<String>> sourceFields;

    // Keys: inference IDs. Values: Field names that use the inference id for calculating inference. Reverse of inferenceForFields.
    private Map<String, Set<String>> fieldsForInferenceIds;

    public static final FieldInferenceMetadata EMPTY = new FieldInferenceMetadata(ImmutableOpenMap.of(), ImmutableOpenMap.of());

    public static final ParseField INFERENCE_FOR_FIELDS_FIELD = new ParseField("inference_for_fields");
    public static final ParseField SOURCE_FIELDS_FIELD = new ParseField("source_fields");

    public FieldInferenceMetadata(
        ImmutableOpenMap<String, String> inferenceIdsForFields,
        ImmutableOpenMap<String, Set<String>> sourceFields
    ) {
        this.inferenceIdForField = Objects.requireNonNull(inferenceIdsForFields);
        this.sourceFields = Objects.requireNonNull(sourceFields);
    }

    public FieldInferenceMetadata(
        Map<String, String> inferenceIdsForFields,
        Map<String, Set<String>> sourceFields
    ) {
        this.inferenceIdForField = ImmutableOpenMap.builder(Objects.requireNonNull(inferenceIdsForFields)).build();
        this.sourceFields = ImmutableOpenMap.builder(Objects.requireNonNull(sourceFields)).build();
    }

    public FieldInferenceMetadata(MappingLookup mappingLookup) {
        this.inferenceIdForField = ImmutableOpenMap.builder(mappingLookup.getInferenceIdsForFields()).build();
        ImmutableOpenMap.Builder<String, Set<String>> sourcePathsBuilder = ImmutableOpenMap.builder(inferenceIdForField.size());
        inferenceIdForField.keySet().forEach(fieldName -> sourcePathsBuilder.put(fieldName, mappingLookup.sourcePaths(fieldName)));
        this.sourceFields = sourcePathsBuilder.build();
    }

    public FieldInferenceMetadata(StreamInput in) throws IOException {
        inferenceIdForField = in.readImmutableOpenMap(StreamInput::readString, StreamInput::readString);
        sourceFields = in.readImmutableOpenMap(StreamInput::readString, i -> i.readCollectionAsImmutableSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(inferenceIdForField, StreamOutput::writeString);
        out.writeMap(sourceFields, StreamOutput::writeStringCollection);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INFERENCE_FOR_FIELDS_FIELD.getPreferredName(), inferenceIdForField);
        builder.field(SOURCE_FIELDS_FIELD.getPreferredName(), sourceFields);

        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldInferenceMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "field_inference_metadata_parser",
        false,
        (args, unused) -> new FieldInferenceMetadata((Map<String, String>) args[0], (Map<String, Set<String>>) args[1])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), INFERENCE_FOR_FIELDS_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(
                HashMap::new,
                v -> v.list().stream().map(Object::toString).collect(Collectors.toSet())
            ),
            SOURCE_FIELDS_FIELD
        );
    }

    public static FieldInferenceMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Diff<FieldInferenceMetadata> diff(FieldInferenceMetadata previousState) {
        return new FieldInferenceMetadataDiff(previousState, this);
    }

    public String getInferenceIdForField(String field) {
        return inferenceIdForField.get(field);
    }

    public Map<String, String> getInferenceIdForFields() {
        return inferenceIdForField;
    }

    public Set<String> getSourceFields(String field) {
        return sourceFields.get(field);
    }

    public Map<String, Set<String>> getFieldsForInferenceIds() {
        if (fieldsForInferenceIds != null) {
            return fieldsForInferenceIds;
        }

        // Cache the result as a field
        Map<String, Set<String>> fieldsForInferenceIdsMap = new HashMap<>();
        for (Map.Entry<String, String> entry : inferenceIdForField.entrySet()) {
            String inferenceId = entry.getValue();
            String fieldName = entry.getKey();

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
        return Objects.equals(inferenceIdForField, that.inferenceIdForField) && Objects.equals(sourceFields, that.sourceFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceIdForField, sourceFields);
    }

    public static class FieldInferenceMetadataDiff implements Diff<FieldInferenceMetadata> {

        private final Diff<ImmutableOpenMap<String, String>> inferenceForFields;
        private final Diff<ImmutableOpenMap<String, Set<String>>> copyFromFields;

        private static final DiffableUtils.NonDiffableValueSerializer<String, String> STRING_VALUE_SERIALIZER =
            new DiffableUtils.NonDiffableValueSerializer<>() {
                @Override
                public void write(String value, StreamOutput out) throws IOException {
                    out.writeString(value);
                }

                @Override
                public String read(StreamInput in, String key) throws IOException {
                    return in.readString();
                }
            };

        FieldInferenceMetadataDiff(FieldInferenceMetadata before, FieldInferenceMetadata after) {
            inferenceForFields = DiffableUtils.diff(
                before.inferenceIdForField,
                after.inferenceIdForField,
                DiffableUtils.getStringKeySerializer(),
                STRING_VALUE_SERIALIZER);
            copyFromFields = DiffableUtils.diff(
                before.sourceFields,
                after.sourceFields,
                DiffableUtils.getStringKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance()
            );
        }

        FieldInferenceMetadataDiff(StreamInput in) throws IOException {
            inferenceForFields = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                STRING_VALUE_SERIALIZER
            );
            copyFromFields = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance()
            );
        }

        public static final FieldInferenceMetadataDiff EMPTY = new FieldInferenceMetadataDiff(
            FieldInferenceMetadata.EMPTY,
            FieldInferenceMetadata.EMPTY
        ) {
            @Override
            public FieldInferenceMetadata apply(FieldInferenceMetadata part) {
                return part;
            }
        };
        @Override
        public FieldInferenceMetadata apply(FieldInferenceMetadata part) {
            ImmutableOpenMap<String, String>  modelForFields = this.inferenceForFields.apply(part.inferenceIdForField);
            ImmutableOpenMap<String, Set<String>> copyFromFields = this.copyFromFields.apply(part.sourceFields);
            return new FieldInferenceMetadata(modelForFields, copyFromFields);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            inferenceForFields.writeTo(out);
            copyFromFields.writeTo(out);
        }
    }
}
