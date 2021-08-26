/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.common.AucRocResult;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Area under the curve (AUC) of the receiver operating characteristic (ROC).
 * The ROC curve is a plot of the TPR (true positive rate) against
 * the FPR (false positive rate) over a varying threshold.
 */
public class AucRocMetric implements EvaluationMetric {

    public static final String NAME = AucRocResult.NAME;

    public static final ParseField INCLUDE_CURVE = new ParseField("include_curve");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<AucRocMetric, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, args -> new AucRocMetric((Boolean) args[0]));

    static {
        PARSER.declareBoolean(optionalConstructorArg(), INCLUDE_CURVE);
    }

    public static AucRocMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static AucRocMetric withCurve() {
        return new AucRocMetric(true);
    }

    private final Boolean includeCurve;

    public AucRocMetric(Boolean includeCurve) {
        this.includeCurve = includeCurve;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (includeCurve != null) {
            builder.field(INCLUDE_CURVE.getPreferredName(), includeCurve);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AucRocMetric that = (AucRocMetric) o;
        return Objects.equals(includeCurve, that.includeCurve);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeCurve);
    }
}
