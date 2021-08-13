/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.common;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class AucRocPoint implements ToXContentObject {

    public static AucRocPoint fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ParseField TPR = new ParseField("tpr");
    private static final ParseField FPR = new ParseField("fpr");
    private static final ParseField THRESHOLD = new ParseField("threshold");

    private static final ConstructingObjectParser<AucRocPoint, Void> PARSER =
        new ConstructingObjectParser<>(
            "auc_roc_point",
            true,
            args -> new AucRocPoint((double) args[0], (double) args[1], (double) args[2]));

    static {
        PARSER.declareDouble(constructorArg(), TPR);
        PARSER.declareDouble(constructorArg(), FPR);
        PARSER.declareDouble(constructorArg(), THRESHOLD);
    }

    private final double tpr;
    private final double fpr;
    private final double threshold;

    public AucRocPoint(double tpr, double fpr, double threshold) {
        this.tpr = tpr;
        this.fpr = fpr;
        this.threshold = threshold;
    }

    public double getTruePositiveRate() {
        return tpr;
    }

    public double getFalsePositiveRate() {
        return fpr;
    }

    public double getThreshold() {
        return threshold;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(TPR.getPreferredName(), tpr)
            .field(FPR.getPreferredName(), fpr)
            .field(THRESHOLD.getPreferredName(), threshold)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AucRocPoint that = (AucRocPoint) o;
        return tpr == that.tpr && fpr == that.fpr && threshold == that.threshold;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tpr, fpr, threshold);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
