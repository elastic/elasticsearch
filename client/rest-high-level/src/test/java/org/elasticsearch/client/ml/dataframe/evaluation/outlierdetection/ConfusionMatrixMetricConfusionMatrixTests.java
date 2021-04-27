/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class ConfusionMatrixMetricConfusionMatrixTests extends AbstractXContentTestCase<ConfusionMatrixMetric.ConfusionMatrix> {

    public static ConfusionMatrixMetric.ConfusionMatrix randomConfusionMatrix() {
        return new ConfusionMatrixMetric.ConfusionMatrix(randomInt(), randomInt(), randomInt(), randomInt());
    }

    @Override
    protected ConfusionMatrixMetric.ConfusionMatrix createTestInstance() {
        return randomConfusionMatrix();
    }

    @Override
    protected ConfusionMatrixMetric.ConfusionMatrix doParseInstance(XContentParser parser) throws IOException {
        return ConfusionMatrixMetric.ConfusionMatrix.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
