/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Request;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdateTests;
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
        var request = randomBoolean()
            ? Request.forIngestDocs(
                randomAlphaOfLength(10),
                Stream.generate(InferModelActionRequestTests::randomMap).limit(randomInt(10)).collect(Collectors.toList()),
                randomInferenceConfigUpdate(),
                randomBoolean(),
                TimeValue.timeValueMillis(randomLongBetween(1, 2048))
            )
            : Request.forTextInput(
                randomAlphaOfLength(10),
                randomInferenceConfigUpdate(),
                Arrays.asList(generateRandomStringArray(3, 5, false)),
                randomBoolean(),
                TimeValue.timeValueMillis(randomLongBetween(1, 2048))
            );

        request.setHighPriority(randomBoolean());
        if (randomBoolean()) {
            request.setPrefixType(randomFrom(TrainedModelPrefixStrings.PrefixType.values()));
        }
        request.setChunked(randomBoolean());
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {

        var modelId = instance.getId();
        var objectsToInfer = instance.getObjectsToInfer();
        var highPriority = instance.isHighPriority();
        var textInput = instance.getTextInput();
        var update = instance.getUpdate();
        var previouslyLicensed = instance.isPreviouslyLicensed();
        var timeout = instance.getInferenceTimeout();
        var prefixType = instance.getPrefixType();
        var chunked = instance.isChunked();

        int change = randomIntBetween(0, 8);
        switch (change) {
            case 0:
                modelId = modelId + "foo";
                break;
            case 1:
                var newDocs = new ArrayList<>(objectsToInfer);
                newDocs.add(randomMap());
                objectsToInfer = newDocs;
                break;
            case 2:
                highPriority = highPriority == false;
                break;
            case 3:
                var newInput = new ArrayList<>(textInput == null ? List.of() : textInput);
                newInput.add((randomAlphaOfLength(4)));
                textInput = newInput;
                break;
            case 4:
                var newUpdate = randomInferenceConfigUpdate();
                while (newUpdate.getName().equals(update.getName())) {
                    newUpdate = randomInferenceConfigUpdate();
                }
                update = newUpdate;
                break;
            case 5:
                previouslyLicensed = previouslyLicensed == false;
                break;
            case 6:
                timeout = TimeValue.timeValueSeconds(timeout.getSeconds() - 1);
                break;
            case 7:
                prefixType = TrainedModelPrefixStrings.PrefixType.values()[(prefixType.ordinal() + 1) % TrainedModelPrefixStrings.PrefixType
                    .values().length];
                break;
            case 8:
                chunked = chunked == false;
                break;
            default:
                throw new IllegalStateException();
        }

        var r = new Request(modelId, update, objectsToInfer, textInput, timeout, previouslyLicensed);
        r.setHighPriority(highPriority);
        r.setPrefixType(prefixType);
        r.setChunked(chunked);
        return r;
    }

    public static InferenceConfigUpdate randomInferenceConfigUpdate() {
        return randomFrom(
            ClassificationConfigUpdateTests.randomClassificationConfigUpdate(),
            EmptyConfigUpdateTests.testInstance(),
            FillMaskConfigUpdateTests.randomUpdate(),
            NerConfigUpdateTests.randomUpdate(),
            PassThroughConfigUpdateTests.randomUpdate(),
            QuestionAnsweringConfigUpdateTests.randomUpdate(),
            RegressionConfigUpdateTests.randomRegressionConfigUpdate(),
            ResultsFieldUpdateTests.randomUpdate(),
            TextClassificationConfigUpdateTests.randomUpdate(),
            TextEmbeddingConfigUpdateTests.randomUpdate(),
            TextExpansionConfigUpdateTests.randomUpdate(),
            TextSimilarityConfigUpdateTests.randomUpdate(),
            TokenizationConfigUpdateTests.randomUpdate(),
            ZeroShotClassificationConfigUpdateTests.randomUpdate()
        );
    }

    public static InferenceConfigUpdate mutateInferenceConfigUpdate(InferenceConfigUpdate currentUpdate, TransportVersion version) {
        InferenceConfigUpdate adjustedUpdate;
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
            } else if (nlpConfigUpdate instanceof TextExpansionConfigUpdate update) {
                adjustedUpdate = TextExpansionConfigUpdateTests.mutateForVersion(update, version);
            } else if (nlpConfigUpdate instanceof TextSimilarityConfigUpdate update) {
                adjustedUpdate = TextSimilarityConfigUpdateTests.mutateForVersion(update, version);
            } else {
                throw new IllegalArgumentException("Unknown update [" + currentUpdate.getName() + "]");
            }
        } else {
            adjustedUpdate = currentUpdate;
        }

        return adjustedUpdate;
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
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        InferenceConfigUpdate adjustedUpdate = mutateInferenceConfigUpdate(instance.getUpdate(), version);

        if (version.before(TransportVersions.V_8_3_0)) {
            return new Request(
                instance.getId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                null,
                TimeValue.MAX_VALUE,
                instance.isPreviouslyLicensed()
            );
        } else if (version.before(TransportVersions.V_8_7_0)) {
            return new Request(
                instance.getId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                null,
                instance.getInferenceTimeout(),
                instance.isPreviouslyLicensed()
            );
        } else if (version.before(TransportVersions.V_8_8_0)) {
            var r = new Request(
                instance.getId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                instance.getTextInput(),
                instance.getInferenceTimeout(),
                instance.isPreviouslyLicensed()
            );
            r.setHighPriority(false);
            return r;
        } else if (version.before(TransportVersions.V_8_12_0)) {
            var r = new Request(
                instance.getId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                instance.getTextInput(),
                instance.getInferenceTimeout(),
                instance.isPreviouslyLicensed()
            );
            r.setHighPriority(instance.isHighPriority());
            r.setPrefixType(TrainedModelPrefixStrings.PrefixType.NONE);
            return r;
        } else if (version.before(TransportVersions.V_8_15_0)) {
            var r = new Request(
                instance.getId(),
                adjustedUpdate,
                instance.getObjectsToInfer(),
                instance.getTextInput(),
                instance.getInferenceTimeout(),
                instance.isPreviouslyLicensed()
            );
            r.setHighPriority(instance.isHighPriority());
            r.setPrefixType(instance.getPrefixType());
            r.setChunked(false);  // r.setChunked(instance.isChunked()); for the next version
            return r;
        }

        return instance;
    }
}
