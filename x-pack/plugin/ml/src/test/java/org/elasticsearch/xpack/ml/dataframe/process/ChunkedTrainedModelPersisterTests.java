/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.license.License;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.FeatureImportanceBaselineTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.HyperparametersTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TotalFeatureImportanceTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.ml.dataframe.process.results.ModelMetadata;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfoTests;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ChunkedTrainedModelPersisterTests extends ESTestCase {

    private static final String JOB_ID = "analytics-result-processor-tests";
    private static final String JOB_DESCRIPTION = "This describes the job of these tests";

    private TrainedModelProvider trainedModelProvider;
    private DataFrameAnalyticsAuditor auditor;

    @Before
    public void setUpMocks() {
        trainedModelProvider = mock(TrainedModelProvider.class);
        auditor = mock(DataFrameAnalyticsAuditor.class);
    }

    @SuppressWarnings("unchecked")
    public void testPersistAllDocs() {
        DataFrameAnalyticsConfig analyticsConfig = new DataFrameAnalyticsConfig.Builder().setId(JOB_ID)
            .setDescription(JOB_DESCRIPTION)
            .setSource(new DataFrameAnalyticsSource(new String[] { "my_source" }, null, null, null))
            .setDest(new DataFrameAnalyticsDest("my_dest", null))
            .setAnalysis(randomBoolean() ? new Regression("foo") : new Classification("foo"))
            .build();
        List<ExtractedField> extractedFieldList = Collections.singletonList(new DocValueField("foo", Collections.emptySet()));

        doAnswer(invocationOnMock -> {
            ActionListener<Boolean> storeListener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
            storeListener.onResponse(true);
            return null;
        }).when(trainedModelProvider).storeTrainedModelConfig(any(TrainedModelConfig.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener<Void> storeListener = (ActionListener<Void>) invocationOnMock.getArguments()[1];
            storeListener.onResponse(null);
            return null;
        }).when(trainedModelProvider).storeTrainedModelDefinitionDoc(any(TrainedModelDefinitionDoc.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener<Void> storeListener = (ActionListener<Void>) invocationOnMock.getArguments()[1];
            storeListener.onResponse(null);
            return null;
        }).when(trainedModelProvider).storeTrainedModelMetadata(any(TrainedModelMetadata.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener<RefreshResponse> storeListener = (ActionListener<RefreshResponse>) invocationOnMock.getArguments()[0];
            storeListener.onResponse(null);
            return null;
        }).when(trainedModelProvider).refreshInferenceIndex(any(ActionListener.class));

        ChunkedTrainedModelPersister resultProcessor = createChunkedTrainedModelPersister(extractedFieldList, analyticsConfig);
        ModelSizeInfo modelSizeInfo = ModelSizeInfoTests.createRandom();
        TrainedModelDefinitionChunk chunk1 = new TrainedModelDefinitionChunk(randomAlphaOfLength(10), 0, false);
        TrainedModelDefinitionChunk chunk2 = new TrainedModelDefinitionChunk(randomAlphaOfLength(10), 1, true);
        ModelMetadata modelMetadata = new ModelMetadata(
            Stream.generate(TotalFeatureImportanceTests::randomInstance).limit(randomIntBetween(1, 10)).collect(Collectors.toList()),
            FeatureImportanceBaselineTests.randomInstance(),
            Stream.generate(HyperparametersTests::randomInstance).limit(randomIntBetween(1, 10)).collect(Collectors.toList())
        );

        resultProcessor.createAndIndexInferenceModelConfig(modelSizeInfo, TrainedModelType.TREE_ENSEMBLE);
        resultProcessor.createAndIndexInferenceModelDoc(chunk1);
        resultProcessor.createAndIndexInferenceModelDoc(chunk2);
        resultProcessor.createAndIndexInferenceModelMetadata(modelMetadata);

        ArgumentCaptor<TrainedModelConfig> storedModelCaptor = ArgumentCaptor.forClass(TrainedModelConfig.class);
        verify(trainedModelProvider).storeTrainedModelConfig(storedModelCaptor.capture(), any(ActionListener.class));

        ArgumentCaptor<TrainedModelDefinitionDoc> storedDocCapture = ArgumentCaptor.forClass(TrainedModelDefinitionDoc.class);
        verify(trainedModelProvider, times(2)).storeTrainedModelDefinitionDoc(storedDocCapture.capture(), any(ActionListener.class));

        ArgumentCaptor<TrainedModelMetadata> storedMetadataCaptor = ArgumentCaptor.forClass(TrainedModelMetadata.class);
        verify(trainedModelProvider, times(1)).storeTrainedModelMetadata(storedMetadataCaptor.capture(), any(ActionListener.class));

        TrainedModelConfig storedModel = storedModelCaptor.getValue();
        assertThat(storedModel.getLicenseLevel(), equalTo(License.OperationMode.PLATINUM));
        assertThat(storedModel.getModelId(), containsString(JOB_ID));
        assertThat(storedModel.getVersion(), equalTo(Version.CURRENT));
        assertThat(storedModel.getCreatedBy(), equalTo(InternalUsers.XPACK_USER.principal()));
        assertThat(storedModel.getTags(), contains(JOB_ID));
        assertThat(storedModel.getDescription(), equalTo(JOB_DESCRIPTION));
        assertThat(storedModel.getModelDefinition(), is(nullValue()));
        assertThat(storedModel.getModelSize(), equalTo(modelSizeInfo.ramBytesUsed()));
        assertThat(storedModel.getEstimatedOperations(), equalTo((long) modelSizeInfo.numOperations()));
        if (analyticsConfig.getAnalysis() instanceof Classification) {
            assertThat(storedModel.getInferenceConfig().getName(), equalTo("classification"));
        } else {
            assertThat(storedModel.getInferenceConfig().getName(), equalTo("regression"));
        }
        Map<String, Object> metadata = storedModel.getMetadata();
        assertThat(metadata.size(), equalTo(1));
        assertThat(metadata, hasKey("analytics_config"));
        Map<String, Object> analyticsConfigAsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, analyticsConfig.toString(), true);
        assertThat(analyticsConfigAsMap, equalTo(metadata.get("analytics_config")));

        TrainedModelDefinitionDoc storedDoc1 = storedDocCapture.getAllValues().get(0);
        assertThat(storedDoc1.getDocNum(), equalTo(0));
        TrainedModelDefinitionDoc storedDoc2 = storedDocCapture.getAllValues().get(1);
        assertThat(storedDoc2.getDocNum(), equalTo(1));

        assertThat(storedModel.getModelId(), equalTo(storedDoc1.getModelId()));
        assertThat(storedModel.getModelId(), equalTo(storedDoc2.getModelId()));

        TrainedModelMetadata storedMetadata = storedMetadataCaptor.getValue();
        assertThat(storedMetadata.getModelId(), equalTo(storedModel.getModelId()));

        ArgumentCaptor<String> auditCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor).info(eq(JOB_ID), auditCaptor.capture());
        assertThat(auditCaptor.getValue(), containsString("Stored trained model with id [" + JOB_ID));
        Mockito.verifyNoMoreInteractions(auditor);
    }

    private ChunkedTrainedModelPersister createChunkedTrainedModelPersister(
        List<ExtractedField> fieldNames,
        DataFrameAnalyticsConfig analyticsConfig
    ) {
        return new ChunkedTrainedModelPersister(
            trainedModelProvider,
            analyticsConfig,
            auditor,
            (unused) -> {},
            new ExtractedFields(fieldNames, Collections.emptyList(), Collections.emptyMap())
        );
    }

}
