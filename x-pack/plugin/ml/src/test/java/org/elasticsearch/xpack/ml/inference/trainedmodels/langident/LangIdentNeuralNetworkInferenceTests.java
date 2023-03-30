/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.trainedmodels.langident;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
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
import static org.mockito.Mockito.mock;

public class LangIdentNeuralNetworkInferenceTests extends ESTestCase {

    public void testAdverseScenarios() throws Exception {
        InferenceDefinition inferenceDefinition = grabModel();
        ClassificationConfig classificationConfig = new ClassificationConfig(5);

        ClassificationInferenceResults singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj(""),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("zxx"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("     "),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("zxx"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("!@#$%^&*()"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("zxx"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("1234567890"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("zxx"));
        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj("-----=-=--=-=+__+_+__==-=-!@#$%^&*()"),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("zxx"));

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
            inferenceObj("매트 스미스는 BBC äôs Doctor Who를 그만둔다."),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ko"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj(
                "@#$%^&*(행 레이블 Dashboard ISSUE Qual. Plan Qual. !@#$%^&*() Report Qual."
                    + " 현황 Risk Task생성 개발과제지정 개발모델 개발목표 개발비 개발팀별 현황 과제이슈"
            ),
            classificationConfig
        );
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ko"));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj(
                "김걸도혁(金乞都革) 김공소(金公疎) 김교합(金咬哈) 김다롱합(金多弄哈) 김마상개(金麻尙介) 김우리개(金于里介) 김상미(金尙美) 김아도을치(金阿都乙赤) "
                    + "김아라(金阿喇) 김아랑합(金阿郞哈) 김아을가(金阿乙加) 김역류(金易留) 김우두(金于豆) 김우허내(金右虛乃) 김유리가(金留里加) 김윤적(金允績) "
                    + "김이랑합(金伊郞哈) 김인을개(金引乙介) 김입성(金入成) 김주창개(金主昌介) 김지하리(金之下里) 김차독(金箚禿) 김지칭가(金只稱哥) 김자라노(金者羅老)."
            ),
            classificationConfig
        );
        // Half the string is ko the other half is zh
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("ko"));
        assertThat(singleValueInferenceResults.getPredictionScore(), closeTo(0.5, 0.1));
        assertThat(singleValueInferenceResults.getTopClasses().get(1).getClassification(), equalTo("zh"));
        assertThat(singleValueInferenceResults.getTopClasses().get(1).getScore(), closeTo(0.5, 0.1));

        singleValueInferenceResults = (ClassificationInferenceResults) inferenceDefinition.infer(
            inferenceObj(
                "[ Republic of Korea ],\n"
                    + "วันนี้ - ตัวอย่างนี้เป็นภาษาไทย\n"
                    + "วันนี้ - ตัวอย่างนี้เป็นภาษาไทย\n"
                    + "        !대한민국(, 영어: Republic of Korea, KOR)은 동아시아의 한반도 남부에 자리한 민주공화국이다. 서쪽으로 중화인민공화국과 황해를 사이에 두고"
            ),
            classificationConfig
        );
        // Majority of the text is obviously Thai, but a close second is Korean
        assertThat(singleValueInferenceResults.valueAsString(), equalTo("th"));
        assertThat(singleValueInferenceResults.getPredictionScore(), closeTo(0.6, 0.1));
        assertThat(singleValueInferenceResults.getTopClasses().get(1).getClassification(), equalTo("ko"));
        assertThat(singleValueInferenceResults.getTopClasses().get(1).getScore(), closeTo(0.4, 0.1));
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
            // The stored language example is a mixture of `ja` and other languages, it should not be predicted with 1.0 accuracy as the
            // cld3 probability indicates.
            Matcher<Double> matcher = entry.getLanguage().equals("ja") ? closeTo(cld3Probability, 0.11) : closeTo(cld3Probability, .01);
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
        trainedModelProvider.getTrainedModel("lang_ident_model_1", GetTrainedModelsAction.Includes.forModelDefinition(), null, future);
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
