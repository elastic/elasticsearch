/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferModelActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        String resultType = randomFrom(ClassificationInferenceResults.RESULT_TYPE, RegressionInferenceResults.RESULT_TYPE);
        return new Response(
            Stream.generate(() -> randomInferenceResult(resultType))
            .limit(randomIntBetween(0, 10))
            .collect(Collectors.toList()),
            resultType);
    }

    private static InferenceResults randomInferenceResult(String resultType) {
        if (resultType.equals(ClassificationInferenceResults.RESULT_TYPE)) {
            return ClassificationInferenceResultsTests.createRandomResults();
        } else if (resultType.equals(RegressionInferenceResults.RESULT_TYPE)) {
            return RegressionInferenceResultsTests.createRandomResults();
        } else {
            fail("unexpected result type [" + resultType + "]");
            return null;
        }
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
