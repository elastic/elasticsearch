/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class NerResults implements InferenceResults {

    public static final String NAME = "ner_result";

    private final List<EntityGroup> entityGroups;

    public NerResults(List<EntityGroup> entityGroups) {
        this.entityGroups = Objects.requireNonNull(entityGroups);
    }

    public NerResults(StreamInput in) throws IOException {
        entityGroups = in.readList(EntityGroup::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("entities");
        for (EntityGroup entity : entityGroups) {
            entity.toXContent(builder, params);
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
        out.writeList(entityGroups);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FillMaskResults.DEFAULT_RESULTS_FIELD, entityGroups.stream().map(EntityGroup::toMap).collect(Collectors.toList()));
        return map;
    }

    @Override
    public Object predictedValue() {
        // Used by the inference aggregation
        throw new UnsupportedOperationException("Named Entity Recognition does not support a single predicted value");
    }

    public List<EntityGroup> getEntityGroups() {
        return entityGroups;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NerResults that = (NerResults) o;
        return Objects.equals(entityGroups, that.entityGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityGroups);
    }

    public static class EntityGroup implements ToXContentObject, Writeable {

        private static final ParseField LABEL = new ParseField("label");
        private static final ParseField SCORE = new ParseField("score");
        private static final ParseField WORD = new ParseField("word");

        private final String label;
        private final double score;
        private final String word;

        public EntityGroup(String label, double score, String word) {
            this.label = Objects.requireNonNull(label);
            this.score = score;
            this.word = Objects.requireNonNull(word);
        }

        public EntityGroup(StreamInput in) throws IOException {
            label = in.readString();
            score = in.readDouble();
            word = in.readString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LABEL.getPreferredName(), label);
            builder.field(SCORE.getPreferredName(), score);
            builder.field(WORD.getPreferredName(), word);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(label);
            out.writeDouble(score);
            out.writeString(word);
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(LABEL.getPreferredName(), label);
            map.put(SCORE.getPreferredName(), score);
            map.put(WORD.getPreferredName(), word);
            return map;
        }

        public String getLabel() {
            return label;
        }

        public double getScore() {
            return score;
        }

        public String getWord() {
            return word;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EntityGroup that = (EntityGroup) o;
            return Double.compare(that.score, score) == 0 &&
                Objects.equals(label, that.label) &&
                Objects.equals(word, that.word);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, score, word);
        }
    }
}
