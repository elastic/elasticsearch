/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class OutlierDetectionTests extends AbstractXContentTestCase<OutlierDetection> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static OutlierDetection createRandom() {
        List<EvaluationMetric> metrics = new ArrayList<>();
        if (randomBoolean()) {
            metrics.add(AucRocMetricTests.createRandom());
        }
        if (randomBoolean()) {
            metrics.add(new PrecisionMetric(Arrays.asList(randomArray(1,
                4,
                Double[]::new,
                OutlierDetectionTests::randomDouble))));
        }
        if (randomBoolean()) {
            metrics.add(new RecallMetric(Arrays.asList(randomArray(1,
                4,
                Double[]::new,
                OutlierDetectionTests::randomDouble))));
        }
        if (randomBoolean()) {
            metrics.add(new ConfusionMatrixMetric(Arrays.asList(randomArray(1,
                4,
                Double[]::new,
                OutlierDetectionTests::randomDouble))));
        }
        return randomBoolean() ?
            new OutlierDetection(randomAlphaOfLength(10), randomAlphaOfLength(10)) :
            new OutlierDetection(randomAlphaOfLength(10), randomAlphaOfLength(10), metrics.isEmpty() ? null : metrics);
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return createRandom();
    }

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }

}
