/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.license.License;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.FeatureImportanceBaseline;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
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
        assertThat(exceptionHolder.get().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, modelId)));
    }

    public void testGetTrainedModelConfig() throws Exception {
        String modelId = "test-get-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        AtomicReference<Void> putMetadataHolder = new AtomicReference<>();
        TrainedModelMetadata modelMetadata = new TrainedModelMetadata(
            modelId,
            Collections.emptyList(),
            new FeatureImportanceBaseline(1.0, Collections.emptyList()),
            Collections.emptyList()
        );
        blockingCall(
            listener -> trainedModelProvider.storeTrainedModelMetadata(modelMetadata, listener),
            putMetadataHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(nullValue()));

        AtomicReference<RefreshResponse> refreshResponseAtomicReference = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.refreshInferenceIndex(listener),
            refreshResponseAtomicReference,
            new AtomicReference<>()
        );

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        getConfigHolder.get().ensureParsedDefinition(xContentRegistry());
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(config));
        assertThat(getConfigHolder.get().getModelDefinition(), is(not(nullValue())));
        assertThat(getConfigHolder.get().getMetadata(), is(nullValue()));
        assertThat(getConfigHolder.get().getMetadata(), is(nullValue()));
        assertThat(getConfigHolder.get().getMetadata(), is(nullValue()));

        getConfigHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.all(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(nullValue()));
        getConfigHolder.get().ensureParsedDefinition(xContentRegistry());
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get().getModelDefinition(), is(not(nullValue())));
        assertThat(getConfigHolder.get().getMetadata(), is(not(nullValue())));
        assertThat(getConfigHolder.get().getMetadata(), hasKey("total_feature_importance"));
        assertThat(getConfigHolder.get().getMetadata(), hasKey("feature_importance_baseline"));
        assertThat(getConfigHolder.get().getMetadata(), hasKey("hyperparameters"));
    }

    public void testGetTrainedModelConfigWithMultiDocDefinition() throws Exception {
        String modelId = "test-get-trained-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);

        AtomicReference<Void> dummy = new AtomicReference<>();
        AtomicReference<Boolean> booleanDummy = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        BytesReference definition = config.getCompressedDefinition();

        blockingCall(
            listener -> trainedModelProvider.storeTrainedModelDefinitionDoc(
                new TrainedModelDefinitionDoc(
                    new BytesArray(definition.array(), 0, definition.length() - 5),
                    modelId,
                    0,
                    (long) definition.length(),
                    definition.length() - 5,
                    1,
                    false
                ),
                listener
            ),
            dummy::set,
            e -> fail(e.getMessage())
        );
        blockingCall(
            listener -> trainedModelProvider.storeTrainedModelDefinitionDoc(
                new TrainedModelDefinitionDoc(
                    new BytesArray(definition.array(), definition.length() - 5, 5),
                    modelId,
                    1,
                    (long) definition.length(),
                    5,
                    1,
                    true
                ),
                listener
            ),
            dummy::set,
            e -> fail(e.getMessage())
        );
        blockingCall(
            listener -> trainedModelProvider.storeTrainedModelConfig(
                new TrainedModelConfig.Builder(config).clearDefinition().build(),
                listener
            ),
            booleanDummy::set,
            e -> fail(e.getMessage())
        );
        blockingCall(
            listener -> trainedModelProvider.refreshInferenceIndex(listener),
            new AtomicReference<RefreshResponse>(),
            new AtomicReference<>()
        );

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }
        getConfigHolder.get().ensureParsedDefinition(xContentRegistry());
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(config));
        assertThat(getConfigHolder.get().getModelDefinition(), is(not(nullValue())));

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            // Should not throw
            getConfigHolder.get().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }

    public void testGetTrainedModelConfigWithoutDefinition() throws Exception {
        String modelId = "test-get-trained-model-config-no-definition";
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).build();
        TrainedModelConfig copyWithoutDefinition = TrainedModelConfig.builder()
            .setCreatedBy(config.getCreatedBy())
            .setCreateTime(config.getCreateTime())
            .setDescription(config.getDescription())
            .setModelSize(config.getModelSize())
            .setEstimatedOperations(config.getEstimatedOperations())
            .setInput(config.getInput())
            .setModelId(config.getModelId())
            .setModelType(TrainedModelType.TREE_ENSEMBLE)
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
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.empty(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        getConfigHolder.get().ensureParsedDefinition(xContentRegistry());
        assertThat(getConfigHolder.get(), is(not(nullValue())));
        assertThat(getConfigHolder.get(), equalTo(copyWithoutDefinition));
        assertThat(getConfigHolder.get().getModelDefinition(), is(nullValue()));
    }

    public void testGetMissingTrainingModelConfig() throws Exception {
        String modelId = "test-get-missing-trained-model-config";
        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
    }

    public void testGetMissingTrainingModelConfigDefinition() throws Exception {
        String modelId = "test-get-missing-trained-model-config-definition";
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        client().delete(
            new DeleteRequest(InferenceIndexConstants.LATEST_INDEX_NAME).id(TrainedModelDefinitionDoc.docId(config.getModelId(), 0))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(), equalTo(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
    }

    public void testGetTruncatedModelDeprecatedDefinition() throws Exception {
        String modelId = "test-get-truncated-legacy-model-config";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        TrainedModelDefinitionDoc truncatedDoc = new TrainedModelDefinitionDoc.Builder().setDocNum(0)
            .setBinaryData(config.getCompressedDefinition().slice(0, config.getCompressedDefinition().length() - 10))
            .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
            .setDefinitionLength(config.getCompressedDefinition().length())
            .setTotalDefinitionLength(config.getCompressedDefinition().length())
            .setModelId(modelId)
            .build();

        try (
            XContentBuilder xContentBuilder = truncatedDoc.toXContent(
                XContentFactory.jsonBuilder(),
                new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true"))
            )
        ) {
            AtomicReference<IndexResponse> putDocHolder = new AtomicReference<>();
            blockingCall(
                listener -> client().prepareIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .setSource(xContentBuilder)
                    .setId(TrainedModelDefinitionDoc.docId(modelId, 0))
                    .execute(listener),
                putDocHolder,
                exceptionHolder
            );
            assertThat(exceptionHolder.get(), is(nullValue()));
        }

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
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

        // The model definition has been put with the config above but it
        // is not large enough to be split into chunk. Chunks are required
        // for this test so overwrite the definition with multiple chunks
        List<TrainedModelDefinitionDoc.Builder> docBuilders = createModelDefinitionDocs(config.getCompressedDefinition(), modelId);

        boolean missingEos = randomBoolean();
        if (missingEos) {
            // Set the wrong end of stream value
            docBuilders.get(docBuilders.size() - 1).setEos(false);
        } else {
            // else write fewer than the expected number of docs
            docBuilders.remove(docBuilders.size() - 1);
        }
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < docBuilders.size(); ++i) {
            TrainedModelDefinitionDoc doc = docBuilders.get(i).build();
            try (XContentBuilder xContentBuilder = doc.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {

                IndexRequestBuilder indexRequestBuilder = client().prepareIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
                    .setSource(xContentBuilder)
                    .setId(TrainedModelDefinitionDoc.docId(modelId, i));

                bulkRequestBuilder.add(indexRequestBuilder);
            }
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        AtomicReference<BulkResponse> putDocsHolder = new AtomicReference<>();
        blockingCall(bulkRequestBuilder::execute, putDocsHolder, exceptionHolder);
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertFalse(putDocsHolder.get().hasFailures());

        AtomicReference<TrainedModelConfig> getConfigHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModel(modelId, GetTrainedModelsAction.Includes.forModelDefinition(), null, listener),
            getConfigHolder,
            exceptionHolder
        );
        assertThat(getConfigHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertThat(exceptionHolder.get().getMessage(), equalTo(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId)));
    }

    public void testGetTrainedModelForInference() throws InterruptedException, IOException {
        String modelId = "test-model-for-inference";
        TrainedModelConfig config = buildTrainedModelConfig(modelId);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        List<TrainedModelDefinitionDoc.Builder> docBuilders = createModelDefinitionDocs(config.getCompressedDefinition(), modelId);

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < docBuilders.size(); i++) {
            TrainedModelDefinitionDoc doc = docBuilders.get(i).build();
            try (XContentBuilder xContentBuilder = doc.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
                IndexRequestBuilder indexRequestBuilder = client().prepareIndex(InferenceIndexConstants.LATEST_INDEX_NAME)
                    .setSource(xContentBuilder)
                    .setId(TrainedModelDefinitionDoc.docId(modelId, i));

                bulkRequestBuilder.add(indexRequestBuilder);
            }
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        AtomicReference<BulkResponse> putDocsHolder = new AtomicReference<>();
        blockingCall(bulkRequestBuilder::execute, putDocsHolder, exceptionHolder);

        assertThat(exceptionHolder.get(), is(nullValue()));
        assertFalse(putDocsHolder.get().hasFailures());

        AtomicReference<InferenceDefinition> definitionHolder = new AtomicReference<>();
        blockingCall(
            listener -> trainedModelProvider.getTrainedModelForInference(modelId, false, listener),
            definitionHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertThat(definitionHolder.get(), is(not(nullValue())));
    }

    private List<TrainedModelDefinitionDoc.Builder> createModelDefinitionDocs(BytesReference compressedDefinition, String modelId) {
        List<BytesReference> chunks = TrainedModelProvider.chunkDefinitionWithSize(compressedDefinition, compressedDefinition.length() / 3);

        return IntStream.range(0, chunks.size())
            .mapToObj(
                i -> new TrainedModelDefinitionDoc.Builder().setDocNum(i)
                    .setBinaryData(chunks.get(i))
                    .setCompressionVersion(TrainedModelConfig.CURRENT_DEFINITION_COMPRESSION_VERSION)
                    .setDefinitionLength(chunks.get(i).length())
                    .setTotalDefinitionLength(compressedDefinition.length())
                    .setEos(i == chunks.size() - 1)
                    .setModelId(modelId)
            )
            .collect(Collectors.toList());
    }

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType(TrainedModelType.TREE_ENSEMBLE)
            .setVersion(MlConfigVersion.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setModelSize(0)
            .setEstimatedOperations(0)
            .setInput(TrainedModelInputTests.createRandomInput());
    }

    private static TrainedModelConfig buildTrainedModelConfig(String modelId) {
        return buildTrainedModelConfigBuilder(modelId).build();
    }

}
