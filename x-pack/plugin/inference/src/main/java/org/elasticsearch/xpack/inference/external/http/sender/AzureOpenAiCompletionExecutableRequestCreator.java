package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.azureopenai.AzureOpenAiResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.azureopenai.AzureOpenAiCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AzureOpenAiCompletionExecutableRequestCreator implements ExecutableRequestCreator {

    private static final Logger logger = LogManager.getLogger(AzureOpenAiCompletionExecutableRequestCreator.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private final AzureOpenAiCompletionModel model;

    private static ResponseHandler createCompletionHandler() {
        return new AzureOpenAiResponseHandler("azure openai completion", AzureOpenAiCompletionResponseEntity::fromResponse);
    }

    public AzureOpenAiCompletionExecutableRequestCreator(AzureOpenAiCompletionModel model) {
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public Runnable create(
        @Nullable String query,
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        AzureOpenAiCompletionRequest request = new AzureOpenAiCompletionRequest(input, model);
        return new ExecutableInferenceRequest(requestSender, logger, request, context, HANDLER, hasRequestCompletedFunction, listener);
    }

}
