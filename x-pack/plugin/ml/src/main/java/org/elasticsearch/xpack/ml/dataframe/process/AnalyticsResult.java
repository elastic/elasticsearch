/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.dataframe.process.results.RowResults;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsResult implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("analytics_result");

    static final ConstructingObjectParser<AnalyticsResult, Void> PARSER = new ConstructingObjectParser<>(TYPE.getPreferredName(),
            a -> new AnalyticsResult((RowResults) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), RowResults.PARSER, RowResults.TYPE);
    }

    private final RowResults rowResults;

    public AnalyticsResult(RowResults rowResults) {
        this.rowResults = rowResults;
    }

    public RowResults getRowResults() {
        return rowResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (rowResults != null) {
            builder.field(RowResults.TYPE.getPreferredName(), rowResults);
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
        return Objects.equals(rowResults, that.rowResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowResults);
    }
}
