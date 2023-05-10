/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.Strings;
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
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TextSimilarityInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextSimilarityInferenceResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResultsTests;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;

public class InferModelActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    private static List<String> INFERENCE_RESULT_TYPES = List.of(
        ClassificationInferenceResults.NAME,
        NerResults.NAME,
        FillMaskResults.NAME,
        PyTorchPassThroughResults.NAME,
        QuestionAnsweringInferenceResults.NAME,
        RegressionInferenceResults.NAME,
        TextEmbeddingResults.NAME,
        TextExpansionResults.NAME,
        TextSimilarityInferenceResults.NAME,
        WarningInferenceResults.NAME
    );

    @Override
    protected Response createTestInstance() {
        String resultType = randomFrom(INFERENCE_RESULT_TYPES);

        return new Response(
            Stream.generate(() -> randomInferenceResult(resultType)).limit(randomIntBetween(0, 10)).collect(Collectors.toList()),
            randomAlphaOfLength(10),
            randomBoolean()
        );
    }

    @Override
    protected Response mutateInstance(Response instance) {
        var modelId = instance.getId();
        var isLicensed = instance.isLicensed();
        if (randomBoolean()) {
            modelId = modelId + "foo";
        } else {
            isLicensed = isLicensed == false;
        }
        return new Response(instance.getInferenceResults(), modelId, isLicensed);
    }

    private static InferenceResults randomInferenceResult(String resultType) {
        return switch (resultType) {
            case ClassificationInferenceResults.NAME -> ClassificationInferenceResultsTests.createRandomResults();
            case NerResults.NAME -> NerResultsTests.createRandomResults();
            case FillMaskResults.NAME -> FillMaskResultsTests.createRandomResults();
            case PyTorchPassThroughResults.NAME -> PyTorchPassThroughResultsTests.createRandomResults();
            case QuestionAnsweringInferenceResults.NAME -> QuestionAnsweringInferenceResultsTests.createRandomResults();
            case RegressionInferenceResults.NAME -> RegressionInferenceResultsTests.createRandomResults();
            case TextEmbeddingResults.NAME -> TextEmbeddingResultsTests.createRandomResults();
            case TextExpansionResults.NAME -> TextExpansionResultsTests.createRandomResults();
            case TextSimilarityInferenceResults.NAME -> TextSimilarityInferenceResultsTests.createRandomResults();
            case WarningInferenceResults.NAME -> WarningInferenceResultsTests.createRandomResults();
            default -> throw new AssertionError("unexpected result type [" + resultType + "]");
        };
    }

    public void testToXContentString() {
        // assert that the toXContent method does not error
        for (var inferenceType : INFERENCE_RESULT_TYPES) {
            var s = Strings.toString(randomInferenceResult(inferenceType));
            assertNotNull(s);
            assertThat(s, not(containsString("error")));
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
