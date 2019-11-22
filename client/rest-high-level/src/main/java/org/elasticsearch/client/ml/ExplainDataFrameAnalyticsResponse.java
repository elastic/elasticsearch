/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.client.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.common.ParseField;
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
