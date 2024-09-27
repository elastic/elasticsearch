/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.SEMANTIC_TEXT_SEARCH_INFERENCE_ID;

/**
 * Contains inference field data for fields.
 * As inference is done in the coordinator node to avoid re-doing it at shard / replica level, the coordinator needs to check for the need
 * to perform inference for specific fields in an index.
 * Given that the coordinator node does not necessarily have mapping information for all indices (only for those that have shards
 * in the node), the field inference information must be stored in the IndexMetadata and broadcasted to all nodes.
 */
public final class InferenceFieldMetadata implements SimpleDiffable<InferenceFieldMetadata>, ToXContentFragment {
    private static final String INFERENCE_ID_FIELD = "inference_id";
    private static final String SEARCH_INFERENCE_ID_FIELD = "search_inference_id";
    private static final String SOURCE_FIELDS_FIELD = "source_fields";

    private final String name;
    private final String inferenceId;
    private final String searchInferenceId;
    private final String[] sourceFields;

    public InferenceFieldMetadata(String name, String inferenceId, String[] sourceFields) {
        this(name, inferenceId, inferenceId, sourceFields);
    }

    public InferenceFieldMetadata(String name, String inferenceId, String searchInferenceId, String[] sourceFields) {
        this.name = Objects.requireNonNull(name);
        this.inferenceId = Objects.requireNonNull(inferenceId);
        this.searchInferenceId = Objects.requireNonNull(searchInferenceId);
        this.sourceFields = Objects.requireNonNull(sourceFields);
    }

    public InferenceFieldMetadata(StreamInput input) throws IOException {
        this.name = input.readString();
        this.inferenceId = input.readString();
        if (input.getTransportVersion().onOrAfter(SEMANTIC_TEXT_SEARCH_INFERENCE_ID)) {
            this.searchInferenceId = input.readString();
        } else {
            this.searchInferenceId = this.inferenceId;
        }
        this.sourceFields = input.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(inferenceId);
        if (out.getTransportVersion().onOrAfter(SEMANTIC_TEXT_SEARCH_INFERENCE_ID)) {
            out.writeString(searchInferenceId);
        }
        out.writeStringArray(sourceFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceFieldMetadata that = (InferenceFieldMetadata) o;
        return Objects.equals(name, that.name)
            && Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(searchInferenceId, that.searchInferenceId)
            && Arrays.equals(sourceFields, that.sourceFields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, inferenceId, searchInferenceId);
        result = 31 * result + Arrays.hashCode(sourceFields);
        return result;
    }

    public String getName() {
        return name;
    }

    public String getInferenceId() {
        return inferenceId;
    }

    public String getSearchInferenceId() {
        return searchInferenceId;
    }

    public String[] getSourceFields() {
        return sourceFields;
    }

    public static Diff<InferenceFieldMetadata> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(InferenceFieldMetadata::new, in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(INFERENCE_ID_FIELD, inferenceId);
        if (searchInferenceId.equals(inferenceId) == false) {
            builder.field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId);
        }
        builder.array(SOURCE_FIELDS_FIELD, sourceFields);
        return builder.endObject();
    }

    public static InferenceFieldMetadata fromXContent(XContentParser parser) throws IOException {
        final String name = parser.currentName();

        XContentParser.Token token = parser.nextToken();
        Objects.requireNonNull(token, "Expected InferenceFieldMetadata but got EOF");

        String currentFieldName = null;
        String inferenceId = null;
        String searchInferenceId = null;
        List<String> inputFields = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (INFERENCE_ID_FIELD.equals(currentFieldName)) {
                    inferenceId = parser.text();
                } else if (SEARCH_INFERENCE_ID_FIELD.equals(currentFieldName)) {
                    searchInferenceId = parser.text();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (SOURCE_FIELDS_FIELD.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            inputFields.add(parser.text());
                        } else {
                            parser.skipChildren();
                        }
                    }
                }
            } else {
                parser.skipChildren();
            }
        }
        return new InferenceFieldMetadata(
            name,
            inferenceId,
            searchInferenceId == null ? inferenceId : searchInferenceId,
            inputFields.toArray(String[]::new)
        );
    }
}
