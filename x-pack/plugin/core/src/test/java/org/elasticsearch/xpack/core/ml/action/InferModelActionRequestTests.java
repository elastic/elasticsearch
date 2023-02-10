/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Request;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.SlimConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.SlimConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdateTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferModelActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return randomBoolean()
            ? Request.forDocs(
                randomAlphaOfLength(10),
                Stream.generate(InferModelActionRequestTests::randomMap).limit(randomInt(10)).collect(Collectors.toList()),
                randomInferenceConfigUpdate(),
                randomBoolean()
            )
            : Request.forTextInput(
                randomAlphaOfLength(10),
                randomInferenceConfigUpdate(),
                Arrays.asList(generateRandomStringArray(3, 5, false))
            );
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    private static InferenceConfigUpdate randomInferenceConfigUpdate() {
        return randomFrom(
            RegressionConfigUpdateTests.randomRegressionConfigUpdate(),
            ClassificationConfigUpdateTests.randomClassificationConfigUpdate(),
            ResultsFieldUpdateTests.randomUpdate(),
            TextClassificationConfigUpdateTests.randomUpdate(),
            TextEmbeddingConfigUpdateTests.randomUpdate(),
            NerConfigUpdateTests.randomUpdate(),
            FillMaskConfigUpdateTests.randomUpdate(),
            ZeroShotClassificationConfigUpdateTests.randomUpdate(),
            PassThroughConfigUpdateTests.randomUpdate(),
            QuestionAnsweringConfigUpdateTests.randomUpdate(),
            EmptyConfigUpdateTests.testInstance()
        );
    }

    private static Map<String, Object> randomMap() {
        return Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomInt(10))
            .collect(Collectors.toMap(Function.identity(), (v) -> randomAlphaOfLength(10)));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, Version version) {
        InferenceConfigUpdate adjustedUpdate;
        InferenceConfigUpdate currentUpdate = instance.getUpdate();
        if (currentUpdate instanceof NlpConfigUpdate nlpConfigUpdate) {
            if (nlpConfigUpdate instanceof TextClassificationConfigUpdate update) {
                adjustedUpdate = TextClassificationConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof TextEmbeddingConfigUpdate update) {
                adjustedUpdate = TextEmbeddingConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof NerConfigUpdate update) {
                adjustedUpdate = NerConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof FillMaskConfigUpdate update) {
                adjustedUpdate = FillMaskConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof ZeroShotClassificationConfigUpdate update) {
                adjustedUpdate = ZeroShotClassificationConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof PassThroughConfigUpdate update) {
                adjustedUpdate = PassThroughConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof QuestionAnsweringConfigUpdate update) {
                adjustedUpdate = QuestionAnsweringConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof SlimConfigUpdate update) {
                adjustedUpdate = SlimConfigUpdateTests.mutateForVersion(update, version);
            } else {
                throw new IllegalArgumentException("Unknown update [" + currentUpdate.getName() + "]");
            }
        } else {
            adjustedUpdate = currentUpdate;
        }

        if (version.before(Version.V_8_3_0)) {
            return new Request(
                instance.getModelId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                null,
                TimeValue.MAX_VALUE,
                instance.isPreviouslyLicensed()
            );
        } else if (version.before(Version.V_8_7_0)) {
            return new Request(
                instance.getModelId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                null,
                instance.getInferenceTimeout(),
                instance.isPreviouslyLicensed()
            );
        }

        return instance;
    }
}
