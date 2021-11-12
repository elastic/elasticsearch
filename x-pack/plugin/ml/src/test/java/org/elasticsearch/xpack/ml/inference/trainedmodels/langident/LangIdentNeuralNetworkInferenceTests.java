/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.trainedmodels.langident;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class LangIdentNeuralNetworkInferenceTests extends ESTestCase {

    public void testAdverseScenarios() throws Exception {
        InferenceDefinition inferenceDefinition = grabModel();
        ClassificationConfig classificationConfig = new ClassificationConfig(5);

        ClassificationInferenceResults singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj(""),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ja"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("     "),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ja"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("!@#$%^&*()"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ja"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("1234567890"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ja"));
        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("-----=-=--=-=+__+_+__==-=-!@#$%^&*()"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ja"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(inferenceObj("A"), classificationConfig);
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("lb"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("„ÍÎÏ◊˝ÏÎ´„€‹›ﬁﬂ‡°·‚∏ØÒÚÒ˘ÚÆ’ÆÚ”∏Ø\uF8FFÔÓ˝ÏÎ´„‹›ﬁˇﬂÁ¨ˆØ"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("vi"));

        // Should not throw
        inferenceDefinition.infer(inferenceObj("행 A A"), classificationConfig);
        inferenceDefinition.infer(inferenceObj("행 A성 xx"), classificationConfig);
        inferenceDefinition.infer(inferenceObj("행 A성 성x"), classificationConfig);
        inferenceDefinition.infer(inferenceObj("행A A성 x성"), classificationConfig);
        inferenceDefinition.infer(inferenceObj("행A 성 x"), classificationConfig);
    }

    public void testMixedLangInference() throws Exception {
        InferenceDefinition inferenceDefinition = grabModel();
        ClassificationConfig classificationConfig = new ClassificationConfig(5);

        ClassificationInferenceResults singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("행 레이블 this is english text obviously and 생성 tom said to test it "),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("en"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("행 레이블 Dashboard ISSUE Qual. Plan Qual. Report Qual. 현황 Risk Task생성 개발과제지정 개발모델 개발목표 개발비 개발팀별 현황 과제이슈"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ko"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(inferenceObj("이Q현"), classificationConfig);
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ko"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj(
                "@#$%^&*(행 레이블 Dashboard ISSUE Qual. Plan Qual. !@#$%^&*() Report Qual."
                    + " 현황 Risk Task생성 개발과제지정 개발모델 개발목표 개발비 개발팀별 현황 과제이슈"
            ),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ko"));

    }

    public void testLangInference() throws Exception {

        InferenceDefinition inferenceDefinition = grabModel();
        List<LanguageExamples.LanguageExampleEntry> examples = new LanguageExamples().getLanguageExamples();
        ClassificationConfig classificationConfig = new ClassificationConfig(1);

        for (LanguageExamples.LanguageExampleEntry entry : examples) {
            String text = entry.getText();
            String cld3Actual = entry.getPredictedLanguage();
            double cld3Probability = entry.getProbability();

            ClassificationInferenceResults singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
                inferenceObj(text),
                classificationConfig
            );

            assertThat(singleValueInferenceResults.valueAsString(), equalTo(cld3Actual));
            Matcher<Double> matcher = entry.getLanguage().equals("hr") ? greaterThan(cld3Probability) : closeTo(cld3Probability, .00001);
            assertThat(
                "mismatch probability for language " + cld3Actual,
                singleValueInferenceResults.getTopClasses().get(0).getProbability(),
                matcher
            );
        }
    }

    InferenceDefinition grabModel() throws IOException {
        TrainedModelProvider trainedModelProvider = new TrainedModelProvider(mock(Client.class), xContentRegistry());
        PlainActionFuture<TrainedModelConfig> future = new PlainActionFuture<>();
        // Should be OK as we don't make any client calls
        trainedModelProvider.getTrainedModel("lang_ident_model_1", GetTrainedModelsAction.Includes.forModelDefinition(), future);
        TrainedModelConfig config = future.actionGet();

        config.ensureParsedDefinition(xContentRegistry());
        TrainedModelDefinition trainedModelDefinition = config.getModelDefinition();
        return new InferenceDefinition(
            (LangIdentNeuralNetwork) trainedModelDefinition.getTrainedModel(),
            trainedModelDefinition.getPreProcessors()
        );
    }

    private static Map<String, Object> inferenceObj(String text) {
        Map<String, Object> inferenceFields = new HashMap<>();
        inferenceFields.put("text", text);
        return inferenceFields;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
