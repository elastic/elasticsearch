/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConfusionMatrixTests extends AbstractSerializingTestCase<ConfusionMatrix> {

    @Override
    protected ConfusionMatrix doParseInstance(XContentParser parser) throws IOException {
        return ConfusionMatrix.fromXContent(parser);
    }

    @Override
    protected ConfusionMatrix createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<ConfusionMatrix> instanceReader() {
        return ConfusionMatrix::new;
    }

    public static ConfusionMatrix createRandom() {
        int thresholdsSize = randomIntBetween(1, 3);
        List<Double> thresholds = new ArrayList<>(thresholdsSize);
        for (int i = 0; i < thresholdsSize; i++) {
            thresholds.add(randomDouble());
        }
        return new ConfusionMatrix(thresholds);
    }
}
