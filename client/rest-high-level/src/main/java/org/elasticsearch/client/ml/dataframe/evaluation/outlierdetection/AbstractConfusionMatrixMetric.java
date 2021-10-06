/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

abstract class AbstractConfusionMatrixMetric implements EvaluationMetric {

    protected static final ParseField AT = new ParseField("at");

    protected final double[] thresholds;

    protected AbstractConfusionMatrixMetric(List<Double> at) {
        this.thresholds = Objects.requireNonNull(at).stream().mapToDouble(Double::doubleValue).toArray();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder
            .startObject()
            .field(AT.getPreferredName(), thresholds)
            .endObject();
    }
}
