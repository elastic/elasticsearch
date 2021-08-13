/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.common.AucRocResult;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Area under the curve (AUC) of the receiver operating characteristic (ROC).
 * The ROC curve is a plot of the TPR (true positive rate) against
 * the FPR (false positive rate) over a varying threshold.
 */
public class AucRocMetric implements EvaluationMetric {

    public static final String NAME = AucRocResult.NAME;

    public static final ParseField CLASS_NAME = new ParseField("class_name");
    public static final ParseField INCLUDE_CURVE = new ParseField("include_curve");

    public static final ConstructingObjectParser<AucRocMetric, Void> PARSER =
        new ConstructingObjectParser<>(NAME, true, args -> new AucRocMetric((String) args[0], (Boolean) args[1]));

    static {
        PARSER.declareString(constructorArg(), CLASS_NAME);
        PARSER.declareBoolean(optionalConstructorArg(), INCLUDE_CURVE);
    }

    public static AucRocMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static AucRocMetric forClass(String className) {
        return new AucRocMetric(className, false);
    }

    public static AucRocMetric forClassWithCurve(String className) {
        return new AucRocMetric(className, true);
    }

    private final String className;
    private final Boolean includeCurve;

    public AucRocMetric(String className, Boolean includeCurve) {
        this.className = Objects.requireNonNull(className);
        this.includeCurve = includeCurve;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_NAME.getPreferredName(), className);
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
        return Objects.equals(className, that.className)
            && Objects.equals(includeCurve, that.includeCurve);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, includeCurve);
    }
}
