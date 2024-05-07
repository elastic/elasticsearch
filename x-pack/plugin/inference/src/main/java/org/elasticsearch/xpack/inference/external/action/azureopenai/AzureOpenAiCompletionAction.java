package org.elasticsearch.xpack.inference.external.action.azureopenai;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.AzureOpenAiCompletionExecutableRequestCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.createInternalServerError;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.wrapFailuresInElasticsearchException;

//TODO: test
public class AzureOpenAiCompletionAction implements ExecutableAction {

    private final String errorMessage;
    private final AzureOpenAiCompletionExecutableRequestCreator requestCreator;
    private final Sender sender;

    public AzureOpenAiCompletionAction(Sender sender, AzureOpenAiCompletionModel model, ServiceComponents serviceComponents) {
        Objects.requireNonNull(serviceComponents);
        Objects.requireNonNull(model);
        this.sender = Objects.requireNonNull(sender);
        this.requestCreator = new AzureOpenAiCompletionExecutableRequestCreator(model);
        this.errorMessage = constructFailedToSendRequestMessage(model.getUri(), "Azure OpenAI completion");
    }

    @Override
    public void execute(InferenceInputs inferenceInputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        if (inferenceInputs instanceof DocumentsOnlyInput docsOnlyInput) {
            if (docsOnlyInput.getInputs().size() > 1) {
                listener.onFailure(
                    new ElasticsearchStatusException("Azure OpenAI completion only accepts 1 input", RestStatus.BAD_REQUEST)
                );
                return;
            }
        } else {
            listener.onFailure(new ElasticsearchStatusException("Invalid inference input type", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }

        try {
            ActionListener<InferenceServiceResults> wrappedListener = wrapFailuresInElasticsearchException(errorMessage, listener);

            sender.send(requestCreator, inferenceInputs, timeout, wrappedListener);
        } catch (ElasticsearchException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            listener.onFailure(createInternalServerError(e, errorMessage));
        }
    }
}
