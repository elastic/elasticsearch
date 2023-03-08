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
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.results.NerResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.PyTorchPassThroughResults;
import org.elasticsearch.xpack.core.ml.inference.results.PyTorchPassThroughResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.QuestionAnsweringInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.QuestionAnsweringInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResultsTests;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferModelActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        String resultType = randomFrom(
            ClassificationInferenceResults.NAME,
            RegressionInferenceResults.NAME,
            NerResults.NAME,
            TextEmbeddingResults.NAME,
            PyTorchPassThroughResults.NAME,
            FillMaskResults.NAME,
            WarningInferenceResults.NAME,
            QuestionAnsweringInferenceResults.NAME
        );
        return new Response(
            Stream.generate(() -> randomInferenceResult(resultType)).limit(randomIntBetween(0, 10)).collect(Collectors.toList()),
            randomAlphaOfLength(10),
            randomBoolean()
        );
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    private static InferenceResults randomInferenceResult(String resultType) {
        switch (resultType) {
            case ClassificationInferenceResults.NAME:
                return ClassificationInferenceResultsTests.createRandomResults();
            case RegressionInferenceResults.NAME:
                return RegressionInferenceResultsTests.createRandomResults();
            case NerResults.NAME:
                return NerResultsTests.createRandomResults();
            case TextEmbeddingResults.NAME:
                return TextEmbeddingResultsTests.createRandomResults();
            case PyTorchPassThroughResults.NAME:
                return PyTorchPassThroughResultsTests.createRandomResults();
            case FillMaskResults.NAME:
                return FillMaskResultsTests.createRandomResults();
            case WarningInferenceResults.NAME:
                return WarningInferenceResultsTests.createRandomResults();
            case QuestionAnsweringInferenceResults.NAME:
                return QuestionAnsweringInferenceResultsTests.createRandomResults();
            default:
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
