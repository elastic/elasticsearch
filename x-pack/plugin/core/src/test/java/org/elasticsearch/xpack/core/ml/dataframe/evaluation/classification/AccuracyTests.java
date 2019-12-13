/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AccuracyTests extends AbstractSerializingTestCase<Accuracy> {

    @Override
    protected Accuracy doParseInstance(XContentParser parser) throws IOException {
        return Accuracy.fromXContent(parser);
    }

    @Override
    protected Accuracy createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Accuracy> instanceReader() {
        return Accuracy::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static Accuracy createRandom() {
        return new Accuracy();
    }

    public void testComputePerClassAccuracy() {
        assertThat(
            Accuracy.computePerClassAccuracy(
                new MulticlassConfusionMatrix.Result(
                    List.of(
                        new MulticlassConfusionMatrix.ActualClass("A", 14, List.of(
                            new MulticlassConfusionMatrix.PredictedClass("A", 1),
                            new MulticlassConfusionMatrix.PredictedClass("B", 6),
                            new MulticlassConfusionMatrix.PredictedClass("C", 4)
                        ), 3L),
                        new MulticlassConfusionMatrix.ActualClass("B", 20, List.of(
                            new MulticlassConfusionMatrix.PredictedClass("A", 5),
                            new MulticlassConfusionMatrix.PredictedClass("B", 3),
                            new MulticlassConfusionMatrix.PredictedClass("C", 9)
                        ), 3L),
                        new MulticlassConfusionMatrix.ActualClass("C", 17, List.of(
                            new MulticlassConfusionMatrix.PredictedClass("A", 8),
                            new MulticlassConfusionMatrix.PredictedClass("B", 2),
                            new MulticlassConfusionMatrix.PredictedClass("C", 7)
                        ), 0L)),
                    0)),
            equalTo(
                List.of(
                    new Accuracy.PerClassResult("A", 25.0 / 51),  // 13 false positives, 13 false negatives
                    new Accuracy.PerClassResult("B", 26.0 / 51),  //  8 false positives, 17 false negatives
                    new Accuracy.PerClassResult("C", 28.0 / 51))) // 13 false positives, 10 false negatives
        );
    }

    public void testComputePerClassAccuracy_OtherActualClassCountIsNonZero() {
        expectThrows(AssertionError.class, () -> Accuracy.computePerClassAccuracy(new MulticlassConfusionMatrix.Result(List.of(), 1)));
    }
}
