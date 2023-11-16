/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record SparseVectorResults(List<TextExpansionResults> expansionResults) implements InferenceResults {
    public static final String NAME = "sparse_vector_results";
    // TODO Do we want it as sparse_embedding to match the task type or sparse_vector from the client PR?
    public static final String SPARSE_EMBEDDING = TaskType.SPARSE_EMBEDDING.toString();

    public SparseVectorResults(StreamInput in) throws IOException {
        this(in.readNamedWriteableCollectionAsList(TextExpansionResults.class));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(SPARSE_EMBEDDING);
        for (TextExpansionResults result : expansionResults) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(expansionResults);
    }

    @Override
    public String getResultsField() {
        return SPARSE_EMBEDDING;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(getResultsField(), expansionResults.stream().map(TextExpansionResults::asMap).collect(Collectors.toList()));

        return map;
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(outputField, expansionResults.stream().map(TextExpansionResults::asMap).collect(Collectors.toList()));

        return map;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }
}
