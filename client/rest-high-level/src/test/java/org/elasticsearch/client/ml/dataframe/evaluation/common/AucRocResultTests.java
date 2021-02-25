/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.evaluation.common;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AucRocResultTests extends AbstractXContentTestCase<EvaluationMetric.Result> {

    public static EvaluationMetric.Result randomResult() {
        return new AucRocResult(
            randomDouble(),
            Stream
                .generate(AucRocPointTests::randomPoint)
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.toList()));
    }

    @Override
    protected EvaluationMetric.Result createTestInstance() {
        return randomResult();
    }

    @Override
    protected EvaluationMetric.Result doParseInstance(XContentParser parser) throws IOException {
        return AucRocResult.fromXContent(parser);
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
