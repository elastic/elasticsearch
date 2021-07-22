/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.client.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ExplainDataFrameAnalyticsResponse implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("explain_data_frame_analytics_response");

    public static final ParseField FIELD_SELECTION = new ParseField("field_selection");
    public static final ParseField MEMORY_ESTIMATION = new ParseField("memory_estimation");

    public static ExplainDataFrameAnalyticsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ExplainDataFrameAnalyticsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            TYPE.getPreferredName(), true,
            args -> new ExplainDataFrameAnalyticsResponse((List<FieldSelection>) args[0], (MemoryEstimation) args[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), FieldSelection.PARSER, FIELD_SELECTION);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), MemoryEstimation.PARSER, MEMORY_ESTIMATION);
    }

    private final List<FieldSelection> fieldSelection;
    private final MemoryEstimation memoryEstimation;

    public ExplainDataFrameAnalyticsResponse(List<FieldSelection> fieldSelection, MemoryEstimation memoryEstimation) {
        this.fieldSelection = Objects.requireNonNull(fieldSelection);
        this.memoryEstimation = Objects.requireNonNull(memoryEstimation);
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

        ExplainDataFrameAnalyticsResponse that = (ExplainDataFrameAnalyticsResponse) other;
        return Objects.equals(fieldSelection, that.fieldSelection)
            && Objects.equals(memoryEstimation, that.memoryEstimation);
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
