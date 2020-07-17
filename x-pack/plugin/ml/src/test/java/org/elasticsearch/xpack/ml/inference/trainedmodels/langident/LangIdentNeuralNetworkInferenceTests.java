/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.trainedmodels.langident;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Mockito.mock;

public class LangIdentNeuralNetworkInferenceTests extends ESTestCase {

    public void testLangInference() throws Exception {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<TrainedModelConfig> future = new PlainActionFuture<>();
        // Should be OK as we don't make any client calls
        trainedModelProvider.getTrainedModel("lang_ident_model_1", true, future);
        TrainedModelConfig config = future.actionGet();

        config.ensureParsedDefinition(xContentRegistry());
        TrainedModelDefinition trainedModelDefinition = config.getModelDefinition();
        InferenceDefinition inferenceDefinition = new InferenceDefinition(
            (LangIdentNeuralNetwork)trainedModelDefinition.getTrainedModel(),
            trainedModelDefinition.getPreProcessors()
        );
        List<LanguageExamples.LanguageExampleEntry> examples = new LanguageExamples().getLanguageExamples();
        ClassificationConfig classificationConfig = new ClassificationConfig(1);

        for (LanguageExamples.LanguageExampleEntry entry : examples) {
            String text = entry.getText();
            String cld3Actual = entry.getPredictedLanguage();
            double cld3Probability = entry.getProbability();

            Map<String, Object> inferenceFields = new HashMap<>();
            inferenceFields.put("text", text);
            ClassificationInferenceResults singleValueInferenceResults =
                (ClassificationInferenceResults) inferenceDefinition.infer(inferenceFields, classificationConfig);

            assertThat(singleValueInferenceResults.valueAsString(), equalTo(cld3Actual));
            double eps = entry.getLanguage().equals("hr") ? 0.001 : 0.00001;
            assertThat("mismatch probability for language " + cld3Actual,
                singleValueInferenceResults.getTopClasses().get(0).getProbability(), closeTo(cld3Probability, eps));
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
