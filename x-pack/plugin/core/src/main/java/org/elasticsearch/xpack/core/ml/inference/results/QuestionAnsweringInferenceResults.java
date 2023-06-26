/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class QuestionAnsweringInferenceResults extends NlpInferenceResults {

    public static final String NAME = "question_answering";
    public static final ParseField START_OFFSET = new ParseField("start_offset");
    public static final ParseField END_OFFSET = new ParseField("end_offset");

    private final String resultsField;
    private final String answer;
    private final int startOffset;
    private final int endOffset;
    private final double score;
    private final List<TopAnswerEntry> topClasses;

    public QuestionAnsweringInferenceResults(
        String answer,
        int startOffset,
        int endOffset,
        List<TopAnswerEntry> topClasses,
        String resultsField,
        double score,
        boolean isTruncated
    ) {
        super(isTruncated);
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.answer = Objects.requireNonNull(answer);
        this.topClasses = topClasses == null ? Collections.emptyList() : Collections.unmodifiableList(topClasses);
        this.resultsField = resultsField;
        this.score = score;
    }

    public QuestionAnsweringInferenceResults(StreamInput in) throws IOException {
        super(in);
        this.answer = in.readString();
        this.startOffset = in.readVInt();
        this.endOffset = in.readVInt();
        this.topClasses = in.readImmutableList(TopAnswerEntry::fromStream);
        this.resultsField = in.readString();
        this.score = in.readDouble();
    }

    public String getAnswer() {
        return answer;
    }

    public List<TopAnswerEntry> getTopClasses() {
        return topClasses;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(answer);
        out.writeVInt(startOffset);
        out.writeVInt(endOffset);
        out.writeCollection(topClasses);
        out.writeString(resultsField);
        out.writeDouble(score);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        QuestionAnsweringInferenceResults that = (QuestionAnsweringInferenceResults) o;
        return Objects.equals(resultsField, that.resultsField)
            && Objects.equals(answer, that.answer)
            && Objects.equals(startOffset, that.startOffset)
            && Objects.equals(endOffset, that.endOffset)
            && Objects.equals(score, that.score)
            && Objects.equals(topClasses, that.topClasses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, answer, score, topClasses, startOffset, endOffset);
    }

    public double getScore() {
        return score;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public String predictedValue() {
        return answer;
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, answer);
        map.put(START_OFFSET.getPreferredName(), startOffset);
        map.put(END_OFFSET.getPreferredName(), endOffset);
        if (topClasses.isEmpty() == false) {
            map.put(
                NlpConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD,
                topClasses.stream().map(TopAnswerEntry::asValueMap).collect(Collectors.toList())
            );
        }
        map.put(PREDICTION_PROBABILITY, score);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(resultsField, answer);
        builder.field(START_OFFSET.getPreferredName(), startOffset);
        builder.field(END_OFFSET.getPreferredName(), endOffset);
        if (topClasses.size() > 0) {
            builder.field(NlpConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD, topClasses);
        }
        builder.field(PREDICTION_PROBABILITY, score);
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public record TopAnswerEntry(String answer, double score, int startOffset, int endOffset) implements Writeable, ToXContentObject {

        public static final ParseField ANSWER = new ParseField("answer");
        public static final ParseField SCORE = new ParseField("score");

        public static TopAnswerEntry fromStream(StreamInput in) throws IOException {
            return new TopAnswerEntry(in.readString(), in.readDouble(), in.readVInt(), in.readVInt());
        }

        public static final String NAME = "top_answer";

        private static final ConstructingObjectParser<TopAnswerEntry, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new TopAnswerEntry((String) a[0], (Double) a[1], (Integer) a[2], (Integer) a[3])
        );

        static {
            PARSER.declareString(constructorArg(), ANSWER);
            PARSER.declareDouble(constructorArg(), SCORE);
            PARSER.declareInt(constructorArg(), START_OFFSET);
            PARSER.declareInt(constructorArg(), END_OFFSET);
        }

        public static TopAnswerEntry fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public Map<String, Object> asValueMap() {
            Map<String, Object> map = Maps.newMapWithExpectedSize(4);
            map.put(ANSWER.getPreferredName(), answer);
            map.put(START_OFFSET.getPreferredName(), startOffset);
            map.put(END_OFFSET.getPreferredName(), endOffset);
            map.put(SCORE.getPreferredName(), score);
            return map;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ANSWER.getPreferredName(), answer);
            builder.field(START_OFFSET.getPreferredName(), startOffset);
            builder.field(END_OFFSET.getPreferredName(), endOffset);
            builder.field(SCORE.getPreferredName(), score);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(answer);
            out.writeDouble(score);
            out.writeVInt(startOffset);
            out.writeVInt(endOffset);
        }
    }

}
