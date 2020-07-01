/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.license.License;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;
import static org.elasticsearch.xpack.ml.integration.ChunkedTrainedModelPersisterIT.chunkStringWithSize;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TrainedModelProviderIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testPutTrainedModelConfig() throws Exception {
        String modelId = "test-put-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testPutTrainedModelConfigThatAlreadyExists() throws Exception {
        String modelId = "test-put-trained-model-config-exists";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, modelId)));
    }

    public void testGetTrainedModelConfig() throws Exception {
        String modelId = "test-get-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        getConfigHolder.get().ensureParsedDefinition(xContentRegistry());
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(config));
        assertThat(getConfigHolder.get().getModelDefinition(), is(not(nullValue())));
    }

    public void testGetTrainedModelConfigWithoutDefinition() throws Exception {
        String modelId = "test-get-trained-model-config-no-definition";
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).build();
        TrainedModelConfig copyWithoutDefinition = TrainedModelConfig.builder()
            .setCreatedBy(config.getCreatedBy())
            .setCreateTime(config.getCreateTime())
            .setDescription(config.getDescription())
            .setEstimatedHeapMemory(config.getEstimatedHeapMemory())
            .setEstimatedOperations(config.getEstimatedOperations())
            .setInput(config.getInput())
            .setModelId(config.getModelId())
            .setTags(config.getTags())
            .setVersion(config.getVersion())
            .setMetadata(config.getMetadata())
            .build();

        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, false, listener), getConfigHolder, exceptionHolder);
        getConfigHolder.get().ensureParsedDefinition(xContentRegistry());
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(copyWithoutDefinition));
        assertThat(getConfigHolder.get().getModelDefinition(), is(nullValue()));
    }

    public void testGetMissingTrainingModelConfig() throws Exception {
        String modelId = "test-get-missing-trained-model-config";
        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
    }

    public void testGetMissingTrainingModelConfigDefinition() throws Exception {
        String modelId = "test-get-missing-trained-model-config-definition";
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        client().delete(new DeleteRequest(InferenceIndexConstants.LATEST_INDEX_NAME)
            .id(TrainedModelDefinitionDoc.docId(config.getModelId(), 0))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
            .actionGet();

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(),
            equalTo(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
    }

    public void testGetTruncatedModelDeprecatedDefinition() throws Exception {
        String modelId = "test-get-truncated-legacy-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        TrainedModelDefinitionDoc truncatedDoc = new TrainedModelDefinitionDoc.Builder()
            .setDocNum(0)
            .setCompressedString(config.getCompressedDefinition().substring(0, config.getCompressedDefinition().length() - 10))
            .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
            .setDefinitionLength(config.getCompressedDefinition().length())
            .setTotalDefinitionLength(config.getCompressedDefinition().length())
            .setModelId(modelId)
            .build();

        try(XContentBuilder xContentBuilder = truncatedDoc.toXContent(XContentFactory.jsonBuilder(),
            new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true")))) {
            AtomicReference<IndexResponse> putDocHolder = new AtomicReference<>();
            blockingCall(listener -> client().prepareIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource(xContentBuilder)
                .setId(TrainedModelDefinitionDoc.docId(modelId, 0))
                .execute(listener),
                putDocHolder,
                exceptionHolder);
            assertThat(exceptionHolder.get(), is(nullValue()));
        }

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(getConfigHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(), equalTo(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId)));
    }

    public void testGetTruncatedModelDefinition() throws Exception {
        String modelId = "test-get-truncated-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        List<String> chunks = chunkStringWithSize(config.getCompressedDefinition(), config.getCompressedDefinition().length()/3);

        List<TrainedModelDefinitionDoc.Builder> docBuilders = IntStream.range(0, chunks.size())
            .mapToObj(i -> new TrainedModelDefinitionDoc.Builder()
                .setDocNum(i)
                .setCompressedString(chunks.get(i))
                .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                .setDefinitionLength(chunks.get(i).length())
                .setEos(i == chunks.size() - 1)
                .setModelId(modelId))
            .collect(Collectors.toList());
        boolean missingEos = randomBoolean();
        docBuilders.get(docBuilders.size() - 1).setEos(missingEos == false);
        for (int i = missingEos ? 0 : 1 ; i < docBuilders.size(); ++i) {
            TrainedModelDefinitionDoc doc = docBuilders.get(i).build();
            try(XContentBuilder xContentBuilder = doc.toXContent(XContentFactory.jsonBuilder(),
                new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true")))) {
                AtomicReference<IndexResponse> putDocHolder = new AtomicReference<>();
                blockingCall(listener -> client().prepareIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .setSource(xContentBuilder)
                        .setId(TrainedModelDefinitionDoc.docId(modelId, 0))
                        .execute(listener),
                    putDocHolder,
                    exceptionHolder);
                assertThat(exceptionHolder.get(), is(nullValue()));
            }
        }
        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(listener -> trainedModelProvider.getTrainedModel(modelId, true, listener), getConfigHolder, exceptionHolder);
        assertThat(getConfigHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(), equalTo(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId)));
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setVersion(Version.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setEstimatedHeapMemory(0)
            .setEstimatedOperations(0)
            .setInput(TrainedModelInputTests.createRandomInput());
    }

    private static TrainedModelConfig buildTrainedModelConfig(String modelId) {
        return buildTrainedModelConfigBuilder(modelId).build();
    }

    @Override
    public NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);

    }

}
