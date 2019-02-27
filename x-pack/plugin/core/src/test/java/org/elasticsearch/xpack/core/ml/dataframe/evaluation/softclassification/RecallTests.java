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

public class RecallTests extends AbstractSerializingTestCase<Recall> {

    @Override
    protected Recall doParseInstance(XContentParser parser) throws IOException {
        return Recall.fromXContent(parser);
    }

    @Override
    protected Recall createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Recall> instanceReader() {
        return Recall::new;
    }

    public static Recall createRandom() {
        int thresholdsSize = randomIntBetween(1, 3);
        List<Double> thresholds = new ArrayList<>(thresholdsSize);
        for (int i = 0; i < thresholdsSize; i++) {
            thresholds.add(randomDouble());
        }
        return new Recall(thresholds);
    }
}
