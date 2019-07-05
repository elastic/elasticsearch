/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class ModelMetadataManager {

    private static final Map<String, String> TO_XCONTENT_PARAMS = Map.of(ToXContentParams.INCLUDE_TYPE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public ModelMetadataManager(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Persist model meta to the ml config index.
     * It is an error if a document with the same Id already exists -
     * the config will not be overwritten.
     *
     * @param modelMeta The meta data
     * @param listener Index response listener
     */
    public void putModelMeta(ModelMeta modelMeta, ActionListener<IndexResponse> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = modelMeta.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            String documentId = ModelMeta.documentId(modelMeta.getModelId());
            IndexRequest indexRequest =  new IndexRequest(AnomalyDetectorsIndex.configIndexName())
                    .id(documentId)
                    .source(source)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                    listener::onResponse,
                    e -> {
                        if (e instanceof VersionConflictEngineException) {
                            listener.onFailure(new ResourceAlreadyExistsException(
                                    "The model cannot be created with the Id [{}]. The Id is already used.", documentId));
                        } else {
                            listener.onFailure(e);
                        }
                    }));

        } catch (IOException e) {
            listener.onFailure(
                    new ElasticsearchParseException("Failed to serialise model meta with id [" + modelMeta.getModelId() + "]", e));
        }
    }

    /**
     * Get the model meta specified by {@code modelId}.
     * If the document is missing a {@code ResourceNotFoundException}
     * is returned via the listener.
     *
     * If the .ml-config index does not exist it is treated as a
     * resource not found error.
     *
     * @param modelId The model ID
     * @param listener The model meta listener
     */
    public void getModelMeta(String modelId, ActionListener<ModelMeta> listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorsIndex.configIndexName(), ModelMeta.documentId(modelId));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    listener.onFailure(missingDocumentExecption(modelId));
                    return;
                }
                BytesReference source = getResponse.getSourceAsBytesRef();
                parseLenientlyFromSource(source, listener);
            }
            @Override
            public void onFailure(Exception e) {
                if (e.getClass() == IndexNotFoundException.class) {
                    listener.onFailure(missingDocumentExecption(modelId));
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    /**
     * Delete the model meta document
     *
     * @param modelId The model id
     * @param actionListener Deleted listener
     */
    public void deleteModelMeta(String modelId,  ActionListener<DeleteResponse> actionListener) {
        DeleteRequest request = new DeleteRequest(AnomalyDetectorsIndex.configIndexName(), ModelMeta.documentId(modelId));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    actionListener.onFailure(missingDocumentExecption(modelId));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                actionListener.onResponse(deleteResponse);
            }
            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    private ResourceNotFoundException missingDocumentExecption(String modelId) {
        return new ResourceNotFoundException("No model with id [{}] found", modelId);
    }

    private void parseLenientlyFromSource(BytesReference source, ActionListener<ModelMeta> listener)  {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            listener.onResponse(ModelMeta.LENIENT_PARSER.apply(parser, null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
