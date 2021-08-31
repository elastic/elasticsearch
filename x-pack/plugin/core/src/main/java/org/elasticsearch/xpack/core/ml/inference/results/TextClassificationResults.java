/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TextClassificationResults implements InferenceResults {

    public static final String NAME = "text_classification_result";

    private final List<TopClassEntry> entryList;

    public TextClassificationResults(List<TopClassEntry> entryList) {
        this.entryList = entryList;
    }

    public TextClassificationResults(StreamInput in) throws IOException {
        entryList = in.readList(TopClassEntry::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.mapContents(asMap());
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entryList);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (TopClassEntry entry : entryList) {
            map.put(entry.getClassification().toString(), entry.getScore());
        }
        return map;
    }

    @Override
    public Object predictedValue() {
        return entryList.get(0).getScore();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextClassificationResults that = (TextClassificationResults) o;
        return Objects.equals(that.entryList, entryList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryList);
    }
}
