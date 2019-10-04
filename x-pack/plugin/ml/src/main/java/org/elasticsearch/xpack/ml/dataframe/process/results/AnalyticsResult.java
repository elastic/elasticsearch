/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AnalyticsResult implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("analytics_result");

    public static final ParseField PROGRESS_PERCENT = new ParseField("progress_percent");

    public static final ConstructingObjectParser<AnalyticsResult, Void> PARSER = new ConstructingObjectParser<>(TYPE.getPreferredName(),
            a -> new AnalyticsResult((RowResults) a[0], (Integer) a[1]));

    static {
        PARSER.declareObject(optionalConstructorArg(), RowResults.PARSER, RowResults.TYPE);
        PARSER.declareInt(optionalConstructorArg(), PROGRESS_PERCENT);
    }

    private final RowResults rowResults;
    private final Integer progressPercent;

    public AnalyticsResult(RowResults rowResults, Integer progressPercent) {
        this.rowResults = rowResults;
        this.progressPercent = progressPercent;
    }

    public RowResults getRowResults() {
        return rowResults;
    }

    public Integer getProgressPercent() {
        return progressPercent;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (rowResults != null) {
            builder.field(RowResults.TYPE.getPreferredName(), rowResults);
        }
        if (progressPercent != null) {
            builder.field(PROGRESS_PERCENT.getPreferredName(), progressPercent);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        AnalyticsResult that = (AnalyticsResult) other;
        return Objects.equals(rowResults, that.rowResults) && Objects.equals(progressPercent, that.progressPercent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowResults, progressPercent);
    }
}
