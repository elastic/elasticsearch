/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalInferModelActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        String resultType = randomFrom(ClassificationInferenceResults.NAME, RegressionInferenceResults.NAME);
        return new Response(
            Stream.generate(() -> randomInferenceResult(resultType)).limit(randomIntBetween(0, 10)).collect(Collectors.toList()),
            randomAlphaOfLength(10),
            randomBoolean()
        );
    }

    private static InferenceResults randomInferenceResult(String resultType) {
        if (resultType.equals(ClassificationInferenceResults.NAME)) {
            return ClassificationInferenceResultsTests.createRandomResults();
        } else if (resultType.equals(RegressionInferenceResults.NAME)) {
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

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }

}
