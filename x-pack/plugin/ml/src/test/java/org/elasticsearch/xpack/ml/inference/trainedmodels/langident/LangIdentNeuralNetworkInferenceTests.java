/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.trainedmodels.langident;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;


public class LangIdentNeuralNetworkInferenceTests extends ESTestCase {

    public void testLangInference() throws Exception {
        TrainedModelConfig config = getLangIdentModel();

        TrainedModelDefinition trainedModelDefinition = config.getModelDefinition();
        List<LanguageExamples.LanguageExampleEntry> examples = new LanguageExamples().getLanguageExamples();
        ClassificationConfig classificationConfig = new ClassificationConfig(1);

        for (LanguageExamples.LanguageExampleEntry entry : examples) {
            String text = entry.getText();
            String cld3Actual = entry.getPredictedLanguage();
            double cld3Probability = entry.getProbability();

            Map<String, Object> inferenceFields = new HashMap<>();
            inferenceFields.put("text", text);
            ClassificationInferenceResults singleValueInferenceResults =
                (ClassificationInferenceResults) trainedModelDefinition.infer(inferenceFields, classificationConfig);

            assertThat(singleValueInferenceResults.valueAsString(), equalTo(cld3Actual));
            double eps = entry.getLanguage().equals("hr") ? 0.001 : 0.00001;
            assertThat("mismatch probability for language " + cld3Actual,
                singleValueInferenceResults.getTopClasses().get(0).getProbability(), closeTo(cld3Probability, eps));
        }
    }

    private TrainedModelConfig getLangIdentModel() throws IOException {
        String path = "/org/elasticsearch/xpack/ml/inference/persistence/lang_ident_model_1.json";
        try(XContentParser parser =
                XContentType.JSON.xContent().createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    Files.newInputStream(getDataPath(path)))) {
            TrainedModelConfig config = TrainedModelConfig.fromXContent(parser, true).build();
            config.ensureParsedDefinition(xContentRegistry());
            return config;
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
