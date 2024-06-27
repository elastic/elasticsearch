package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionCreator;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.TransportVersions.ML_INFERENCE_AMAZON_BEDROCK_ADDED;
import static org.elasticsearch.xpack.core.inference.results.ResultUtils.createInvalidChunkedResultException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class AmazonBedrockService extends SenderService {
    public static final String NAME = "amazon_bedrock_service";

    private final AmazonBedrockRequestSender.Factory amazonBedrockSenderFactory;

    public AmazonBedrockService(
        HttpRequestSender.Factory httpSenderFactory,
        AmazonBedrockRequestSender.Factory amazonBedrockFactory,
        ServiceComponents serviceComponents
    ) {
        super(httpSenderFactory, serviceComponents);
        this.amazonBedrockSenderFactory = amazonBedrockFactory;
    }

    @Override
    protected void doInfer(
        Model model,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var actionCreator = new AmazonBedrockActionCreator(amazonBedrockSenderFactory.createSender(), this.getServiceComponents());
        if (model instanceof AmazonBedrockModel baseAmazonBedrockModel) {
            var action = baseAmazonBedrockModel.accept(actionCreator, taskSettings);
            action.execute(new DocumentsOnlyInput(input), timeout, listener);
        } else {
            listener.onFailure(createInvalidModelException(model));
        }
    }

    @Override
    protected void doInfer(
        Model model,
        String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        throw new UnsupportedOperationException("Amazon Bedrock service does not support inference with query input");
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        String query,
        List<String> input,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        ActionListener<InferenceServiceResults> inferListener = listener.delegateFailureAndWrap(
            (delegate, response) -> delegate.onResponse(translateToChunkedResults(input, response))
        );

        doInfer(model, input, taskSettings, inputType, timeout, inferListener);
    }

    private static List<ChunkedInferenceServiceResults> translateToChunkedResults(
        List<String> inputs,
        InferenceServiceResults inferenceResults
    ) {
        if (inferenceResults instanceof InferenceTextEmbeddingFloatResults textEmbeddingResults) {
            return InferenceChunkedTextEmbeddingFloatResults.listOf(inputs, textEmbeddingResults);
        } else if (inferenceResults instanceof ErrorInferenceResults error) {
            return List.of(new ErrorChunkedInferenceResults(error.getException()));
        } else {
            throw createInvalidChunkedResultException(InferenceTextEmbeddingFloatResults.NAME, inferenceResults.getWriteableName());
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void parseRequestConfig(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Set<String> platformArchitectures,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            AmazonBedrockModel model = createModel(
                modelId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                serviceSettingsMap,
                TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME),
                ConfigurationParseContext.REQUEST
            );

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    @Override
    public Model parsePersistedConfigWithSecrets(
        String modelId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrDefaultEmpty(secrets, ModelSecrets.SECRET_SETTINGS);

        return createModel(
            modelId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(modelId, NAME),
            ConfigurationParseContext.PERSISTENT
        );
    }

    @Override
    public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
        return null;
    }

    private static AmazonBedrockModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        switch (taskType) {
            case TEXT_EMBEDDING -> {
                return new AmazonBedrockEmbeddingsModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    secretSettings,
                    context
                );
            }
            case COMPLETION -> {
                return new AmazonBedrockChatCompletionModel(
                    inferenceEntityId,
                    taskType,
                    NAME,
                    serviceSettings,
                    taskSettings,
                    secretSettings,
                    context
                );
            }
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_AMAZON_BEDROCK_ADDED;
    }

    /**
     * For text embedding models get the embedding size and
     * update the service settings.
     *
     * @param model The new model
     * @param listener The listener
     */
    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof AmazonBedrockEmbeddingsModel embeddingsModel) {
            ServiceUtils.getEmbeddingSize(
                model,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateModelWithEmbeddingDetails(embeddingsModel, size)))
            );
        } else {
            listener.onResponse(model);
        }
    }

    private AmazonBedrockEmbeddingsModel updateModelWithEmbeddingDetails(AmazonBedrockEmbeddingsModel model, int embeddingSize) {
        AmazonBedrockEmbeddingsServiceSettings serviceSettings = (AmazonBedrockEmbeddingsServiceSettings) model.getServiceSettings();
        if (serviceSettings.dimensionsSetByUser()
            && serviceSettings.dimensions() != null
            && serviceSettings.dimensions() != embeddingSize) {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "The retrieved embeddings size [%s] does not match the size specified in the settings [%s]. "
                        + "Please recreate the [%s] configuration with the correct dimensions",
                    embeddingSize,
                    serviceSettings.dimensions(),
                    model.getConfigurations().getInferenceEntityId()
                ),
                RestStatus.BAD_REQUEST
            );
        }

        var similarityFromModel = serviceSettings.similarity();
        var similarityToUse = similarityFromModel == null ? SimilarityMeasure.DOT_PRODUCT : similarityFromModel;

        AmazonBedrockEmbeddingsServiceSettings settingsToUse = new AmazonBedrockEmbeddingsServiceSettings(
            serviceSettings.region(),
            serviceSettings.model(),
            serviceSettings.provider(),
            embeddingSize,
            serviceSettings.dimensionsSetByUser(),
            serviceSettings.maxInputTokens(),
            similarityToUse,
            serviceSettings.rateLimitSettings()
        );

        return new AmazonBedrockEmbeddingsModel(model, settingsToUse);
    }
}
