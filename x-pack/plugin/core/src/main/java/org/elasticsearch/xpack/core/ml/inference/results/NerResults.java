/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class NerResults extends NlpInferenceResults {

    public static final String NAME = "ner_result";
    public static final String ENTITY_FIELD = "entities";

    private final String resultsField;
    private final String annotatedResult;

    private final List<EntityGroup> entityGroups;

    public NerResults(String resultsField, String annotatedResult, List<EntityGroup> entityGroups, boolean isTruncated) {
        super(isTruncated);
        this.entityGroups = Objects.requireNonNull(entityGroups);
        this.resultsField = Objects.requireNonNull(resultsField);
        this.annotatedResult = Objects.requireNonNull(annotatedResult);
    }

    public NerResults(StreamInput in) throws IOException {
        super(in);
        entityGroups = in.readList(EntityGroup::new);
        resultsField = in.readString();
        annotatedResult = in.readString();
    }

    @Override
    void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, annotatedResult);
        builder.startArray("entities");
        for (EntityGroup entity : entityGroups) {
            entity.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(entityGroups);
        out.writeString(resultsField);
        out.writeString(annotatedResult);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, annotatedResult);
        map.put(ENTITY_FIELD, entityGroups.stream().map(EntityGroup::toMap).collect(Collectors.toList()));
    }

    @Override
    public Object predictedValue() {
        return annotatedResult;
    }

    public List<EntityGroup> getEntityGroups() {
        return entityGroups;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    public String getAnnotatedResult() {
        return annotatedResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        NerResults that = (NerResults) o;
        return Objects.equals(resultsField, that.resultsField)
            && Objects.equals(annotatedResult, that.annotatedResult)
            && Objects.equals(entityGroups, that.entityGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, annotatedResult, entityGroups);
    }

    public static class EntityGroup implements ToXContentObject, Writeable {

        static final String CLASS_NAME = "class_name";
        static final String CLASS_PROBABILITY = "class_probability";
        static final String START_POS = "start_pos";
        static final String END_POS = "end_pos";

        private final String entity;
        private final String className;
        private final double classProbability;
        private final int startPos;
        private final int endPos;

        public EntityGroup(String entity, String className, double classProbability, int startPos, int endPos) {
            this.entity = entity;
            this.className = className;
            this.classProbability = classProbability;
            this.startPos = startPos;
            this.endPos = endPos;
            if (endPos < startPos) {
                throw new IllegalArgumentException("end_pos [" + endPos + "] less than start_pos [" + startPos + "]");
            }
        }

        public EntityGroup(StreamInput in) throws IOException {
            this.entity = in.readString();
            this.className = in.readString();
            this.classProbability = in.readDouble();
            this.startPos = in.readInt();
            this.endPos = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(entity);
            out.writeString(className);
            out.writeDouble(classProbability);
            out.writeInt(startPos);
            out.writeInt(endPos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("entity", entity);
            builder.field(CLASS_NAME, className);
            builder.field(CLASS_PROBABILITY, classProbability);
            if (startPos >= 0) {
                builder.field(START_POS, startPos);
            }
            if (endPos >= 0) {
                builder.field(END_POS, endPos);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("entity", entity);
            map.put(CLASS_NAME, className);
            map.put(CLASS_PROBABILITY, classProbability);
            if (startPos >= 0) {
                map.put(START_POS, startPos);
            }
            if (endPos >= 0) {
                map.put(END_POS, endPos);
            }
            return map;
        }

        public String getEntity() {
            return entity;
        }

        public String getClassName() {
            return className;
        }

        public double getClassProbability() {
            return classProbability;
        }

        public int getStartPos() {
            return startPos;
        }

        public int getEndPos() {
            return endPos;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EntityGroup that = (EntityGroup) o;
            return Double.compare(that.classProbability, classProbability) == 0
                && startPos == that.startPos
                && endPos == that.endPos
                && Objects.equals(entity, that.entity)
                && Objects.equals(className, that.className);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entity, className, classProbability, startPos, endPos);
        }
    }
}
