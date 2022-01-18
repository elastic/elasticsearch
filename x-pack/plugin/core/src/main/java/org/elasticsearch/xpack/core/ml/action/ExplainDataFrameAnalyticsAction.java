/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.dataframe.explain.MemoryEstimation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ExplainDataFrameAnalyticsAction extends ActionType<ExplainDataFrameAnalyticsAction.Response> {

    public static final ExplainDataFrameAnalyticsAction INSTANCE = new ExplainDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/explain";

    private ExplainDataFrameAnalyticsAction() {
        super(NAME, ExplainDataFrameAnalyticsAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField TYPE = new ParseField("explain_data_frame_analytics_response");

        public static final ParseField FIELD_SELECTION = new ParseField("field_selection");
        public static final ParseField MEMORY_ESTIMATION = new ParseField("memory_estimation");

        @SuppressWarnings({ "unchecked" })
        static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            args -> new Response((List<FieldSelection>) args[0], (MemoryEstimation) args[1])
        );

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), FieldSelection.PARSER, FIELD_SELECTION);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), MemoryEstimation.PARSER, MEMORY_ESTIMATION);
        }

        private final List<FieldSelection> fieldSelection;
        private final MemoryEstimation memoryEstimation;

        public Response(List<FieldSelection> fieldSelection, MemoryEstimation memoryEstimation) {
            this.fieldSelection = Objects.requireNonNull(fieldSelection);
            this.memoryEstimation = Objects.requireNonNull(memoryEstimation);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.fieldSelection = in.readList(FieldSelection::new);
            this.memoryEstimation = new MemoryEstimation(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(fieldSelection);
            memoryEstimation.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_SELECTION.getPreferredName(), fieldSelection);
            builder.field(MEMORY_ESTIMATION.getPreferredName(), memoryEstimation);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;

            Response that = (Response) other;
            return Objects.equals(fieldSelection, that.fieldSelection) && Objects.equals(memoryEstimation, that.memoryEstimation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldSelection, memoryEstimation);
        }

        public MemoryEstimation getMemoryEstimation() {
            return memoryEstimation;
        }

        public List<FieldSelection> getFieldSelection() {
            return fieldSelection;
        }
    }
}
