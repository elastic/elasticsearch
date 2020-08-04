/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.dataframe.process.ChunkedTrainedModelPersister;
import org.elasticsearch.xpack.ml.dataframe.process.results.TrainedModelDefinitionChunk;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.inference.modelsize.MlModelSizeNamedXContentProvider;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfo;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfoTests;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ChunkedTrainedModelPersisterIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testStoreModelViaChunkedPersister() throws IOException {
        String modelId = "stored-chunked-model";
        DataFrameAnalyticsConfig analyticsConfig = new DataFrameAnalyticsConfig.Builder()
            .setId(modelId)
            .setSource(new DataFrameAnalyticsSource(new String[] {"my_source"}, null, null))
            .setDest(new DataFrameAnalyticsDest("my_dest", null))
            .setAnalysis(new Regression("foo"))
            .build();
        List<ExtractedField> extractedFieldList = Collections.singletonList(new DocValueField("foo", Collections.emptySet()));
        TrainedModelConfig.Builder configBuilder = buildTrainedModelConfigBuilder(modelId);
        String compressedDefinition = configBuilder.build().getCompressedDefinition();
        int totalSize = compressedDefinition.length();
        List<String> chunks = chunkStringWithSize(compressedDefinition, totalSize/3);

        ChunkedTrainedModelPersister persister = new ChunkedTrainedModelPersister(trainedModelProvider,
            analyticsConfig,
            new DataFrameAnalyticsAuditor(client(), "test-node"),
            (ex) -> { throw new ElasticsearchException(ex); },
            new ExtractedFields(extractedFieldList, Collections.emptyMap())
        );

        //Accuracy for size is not tested here
        ModelSizeInfo modelSizeInfo = ModelSizeInfoTests.createRandom();
        persister.createAndIndexInferenceModelMetadata(modelSizeInfo);
        for (int i = 0; i < chunks.size(); i++) {
            persister.createAndIndexInferenceModelDoc(new TrainedModelDefinitionChunk(chunks.get(i), i, i == (chunks.size() - 1)));
        }

        PlainActionFuture<Tuple<Long, Set<String>>> getIdsFuture = new PlainActionFuture<>();
        trainedModelProvider.expandIds(modelId + "*", false, PageParams.defaultParams(), Collections.emptySet(), getIdsFuture);
        Tuple<Long, Set<String>> ids = getIdsFuture.actionGet();
        assertThat(ids.v1(), equalTo(1L));

        PlainActionFuture<TrainedModelConfig> getTrainedModelFuture = new PlainActionFuture<>();
        trainedModelProvider.getTrainedModel(ids.v2().iterator().next(), true, getTrainedModelFuture);

        TrainedModelConfig storedConfig = getTrainedModelFuture.actionGet();
        assertThat(storedConfig.getCompressedDefinition(), equalTo(compressedDefinition));
        assertThat(storedConfig.getEstimatedOperations(), equalTo((long)modelSizeInfo.numOperations()));
        assertThat(storedConfig.getEstimatedHeapMemory(), equalTo(modelSizeInfo.ramBytesUsed()));
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
            .setVersion(Version.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setEstimatedHeapMemory(bytesUsed)
            .setEstimatedOperations(operations)
            .setInput(TrainedModelInputTests.createRandomInput());
    }

    public static List<String> chunkStringWithSize(String str, int chunkSize) {
        List<String> subStrings = new ArrayList<>((str.length() + chunkSize - 1) / chunkSize);
        for (int i = 0; i < str.length(); i += chunkSize) {
            subStrings.add(str.substring(i, Math.min(i + chunkSize, str.length())));
        }
        return subStrings;
    }

    @Override
    public NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlModelSizeNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }

}
