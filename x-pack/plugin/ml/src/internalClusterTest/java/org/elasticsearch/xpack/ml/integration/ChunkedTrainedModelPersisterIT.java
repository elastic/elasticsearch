/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.FeatureImportanceBaselineTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.HyperparametersTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TotalFeatureImportanceTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.dataframe.process.ChunkedTrainedModelPersister;
import org.elasticsearch.xpack.ml.dataframe.process.results.ModelMetadata;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfoTests;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.startsWith;

public class ChunkedTrainedModelPersisterIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testStoreModelViaChunkedPersister() throws IOException {
        String modelId = "stored-chunked-model";
        DataFrameAnalyticsConfig analyticsConfig = new DataFrameAnalyticsConfig.Builder().setId(modelId)
            .setSource(new DataFrameAnalyticsSource(new String[] { "my_source" }, null, null, null))
            .setDest(new DataFrameAnalyticsDest("my_dest", null))
            .setAnalysis(new Regression("foo"))
            .build();
        List<ExtractedField> extractedFieldList = Collections.singletonList(new DocValueField("foo", Collections.emptySet()));
        TrainedModelConfig.Builder configBuilder = buildTrainedModelConfigBuilder(modelId);
        BytesReference compressedDefinition = configBuilder.build().getCompressedDefinition();
        List<String> base64Chunks = chunkBinaryDefinition(compressedDefinition, compressedDefinition.length() / 3);

        ChunkedTrainedModelPersister persister = new ChunkedTrainedModelPersister(
            trainedModelProvider,
            analyticsConfig,
            new DataFrameAnalyticsAuditor(client(), getInstanceFromNode(ClusterService.class)),
            (ex) -> { throw new ElasticsearchException(ex); },
            new ExtractedFields(extractedFieldList, Collections.emptyList(), Collections.emptyMap())
        );

        // Accuracy for size is not tested here
        ModelSizeInfo modelSizeInfo = ModelSizeInfoTests.createRandom();
        persister.createAndIndexInferenceModelConfig(modelSizeInfo, configBuilder.getModelType());
        for (int i = 0; i < base64Chunks.size(); i++) {
            persister.createAndIndexInferenceModelDoc(
                new TrainedModelDefinitionChunk(base64Chunks.get(i), i, i == (base64Chunks.size() - 1))
            );
        }
        ModelMetadata modelMetadata = new ModelMetadata(
            Stream.generate(TotalFeatureImportanceTests::randomInstance).limit(randomIntBetween(1, 10)).collect(Collectors.toList()),
            FeatureImportanceBaselineTests.randomInstance(),
            Stream.generate(HyperparametersTests::randomInstance).limit(randomIntBetween(1, 10)).collect(Collectors.toList())
        );
        persister.createAndIndexInferenceModelMetadata(modelMetadata);

        PlainActionFuture<Tuple<Long, Map<String, Set<String>>>> getIdsFuture = new PlainActionFuture<>();
        trainedModelProvider.expandIds(
            modelId + "*",
            false,
            PageParams.defaultParams(),
            Collections.emptySet(),
            ModelAliasMetadata.EMPTY,
            getIdsFuture
        );
        Tuple<Long, Map<String, Set<String>>> ids = getIdsFuture.actionGet();
        assertThat(ids.v1(), equalTo(1L));
        String inferenceModelId = ids.v2().keySet().iterator().next();

        PlainActionFuture<TrainedModelConfig> getTrainedModelFuture = new PlainActionFuture<>();
        trainedModelProvider.getTrainedModel(inferenceModelId, GetTrainedModelsAction.Includes.all(), getTrainedModelFuture);

        TrainedModelConfig storedConfig = getTrainedModelFuture.actionGet();
        assertThat(storedConfig.getCompressedDefinition(), equalTo(compressedDefinition));
        assertThat(storedConfig.getEstimatedOperations(), equalTo((long) modelSizeInfo.numOperations()));
        assertThat(storedConfig.getModelSize(), equalTo(modelSizeInfo.ramBytesUsed()));
        assertThat(storedConfig.getMetadata(), hasKey("total_feature_importance"));
        assertThat(storedConfig.getMetadata(), hasKey("feature_importance_baseline"));
        assertThat(storedConfig.getMetadata(), hasKey("hyperparameters"));

        PlainActionFuture<Map<String, TrainedModelMetadata>> getTrainedMetadataFuture = new PlainActionFuture<>();
        trainedModelProvider.getTrainedModelMetadata(Collections.singletonList(inferenceModelId), getTrainedMetadataFuture);

        TrainedModelMetadata storedMetadata = getTrainedMetadataFuture.actionGet().get(inferenceModelId);
        assertThat(storedMetadata.getModelId(), startsWith(modelId));
        assertThat(storedMetadata.getTotalFeatureImportances(), equalTo(modelMetadata.getFeatureImportances()));
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        TrainedModelDefinition.Builder definitionBuilder = TrainedModelDefinitionTests.createRandomBuilder();
        long bytesUsed = definitionBuilder.build().ramBytesUsed();
        long operations = definitionBuilder.build().getTrainedModel().estimatedNumOperations();
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION))
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType(TrainedModelType.TREE_ENSEMBLE)
            .setVersion(Version.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setModelSize(bytesUsed)
            .setEstimatedOperations(operations)
            .setInput(TrainedModelInputTests.createRandomInput());
    }

    public static List<String> chunkBinaryDefinition(BytesReference bytes, int chunkSize) {
        List<String> subStrings = new ArrayList<>((bytes.length() + chunkSize - 1) / chunkSize);
        for (int i = 0; i < bytes.length(); i += chunkSize) {
            subStrings.add(
                Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes.array(), i, Math.min(i + chunkSize, bytes.length())))
            );
        }
        return subStrings;
    }
}
