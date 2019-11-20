/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.trainedmodels;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;


public class LangIdentNeuralNetworkInferenceTests extends ESTestCase {

    public void testLangInference() throws Exception {
        byte[] fileBytes = Files.readAllBytes(PathUtils.get(getClass()
            .getResource("/org/elasticsearch/xpack/ml/inference/lang_ident_model_1.json").toURI()));
        TrainedModelConfig config;
        try (XContentParser parser =
                 XContentHelper.createParser(xContentRegistry(),
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                     new BytesArray(fileBytes),
                     XContentType.JSON)) {
            config = TrainedModelConfig.fromXContent(parser, true).build();
            config.ensureParsedDefinition(xContentRegistry());
        } catch (Exception ex) {
            fail(ex.getMessage());
            return;
        }

        TrainedModelDefinition trainedModelDefinition = config.getModelDefinition();

        for (int i = 0; i < LanguageExamples.goldLangText.length; ++i) {
            String text = LanguageExamples.goldLangText[i][1];

            String cld3Expected = LanguageExamples.goldLangResults[i][0];
            String cld3Actual = LanguageExamples.goldLangResults[i][1];
            String cld3ProbabilityStr = LanguageExamples.goldLangResults[i][2];

            float cld3Probability = Float.parseFloat(cld3ProbabilityStr);

            Map<String, Object> inferenceFields = new HashMap<>();
            inferenceFields.put("text", text);
            ClassificationInferenceResults singleValueInferenceResults =
                (ClassificationInferenceResults) trainedModelDefinition.infer(inferenceFields, new ClassificationConfig(5));

            assertEquals(text + ":" + singleValueInferenceResults.valueAsString(),
                cld3Actual,
                singleValueInferenceResults.valueAsString());
            assertEquals(cld3Expected,
                cld3Probability,
                singleValueInferenceResults.getTopClasses().get(0).getProbability(), 0.01);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
