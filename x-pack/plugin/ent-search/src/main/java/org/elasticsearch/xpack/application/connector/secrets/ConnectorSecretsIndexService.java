/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.secrets.action.DeleteConnectorSecretResponse;
import org.elasticsearch.xpack.application.connector.secrets.action.GetConnectorSecretResponse;
import org.elasticsearch.xpack.application.connector.secrets.action.PostConnectorSecretRequest;
import org.elasticsearch.xpack.application.connector.secrets.action.PostConnectorSecretResponse;
import org.elasticsearch.xpack.application.connector.secrets.action.PutConnectorSecretRequest;
import org.elasticsearch.xpack.application.connector.secrets.action.PutConnectorSecretResponse;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;

/**
 * A service that manages persistent Connector Secrets.
 */
public class ConnectorSecretsIndexService {

    private final Client clientWithOrigin;

    public static final String CONNECTOR_SECRETS_INDEX_NAME = ".connector-secrets";
    private static final int CURRENT_INDEX_VERSION = 1;
    private static final String MAPPING_VERSION_VARIABLE = "connector-secrets.version";
    private static final String MAPPING_MANAGED_VERSION_VARIABLE = "connector-secrets.managed.index.version";

    public ConnectorSecretsIndexService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONNECTORS_ORIGIN);
    }

    /**
     * Returns the {@link SystemIndexDescriptor} for the Connector Secrets system index.
     *
     * @return The {@link SystemIndexDescriptor} for the Connector Secrets system index.
     */
    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();

        String templateSource = TemplateUtils.loadTemplate(
            "/connector-secrets.json",
            Version.CURRENT.toString(),
            MAPPING_VERSION_VARIABLE,
            Map.of(MAPPING_MANAGED_VERSION_VARIABLE, Integer.toString(CURRENT_INDEX_VERSION))
        );
        request.source(templateSource, XContentType.JSON);

        return SystemIndexDescriptor.builder()
            .setIndexPattern(CONNECTOR_SECRETS_INDEX_NAME + "*")
            .setPrimaryIndex(CONNECTOR_SECRETS_INDEX_NAME + "-" + CURRENT_INDEX_VERSION)
            .setDescription("Secret values managed by Connectors")
            .setMappings(request.mappings())
            .setSettings(request.settings())
            .setAliasName(CONNECTOR_SECRETS_INDEX_NAME)
            .setOrigin(CONNECTORS_ORIGIN)
            .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
            .build();
    }

    /**
     * Gets the secret from the underlying index with the specified id.
     *
     * @param id        The id of the secret.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void getSecret(String id, ActionListener<GetConnectorSecretResponse> listener) {
        clientWithOrigin.prepareGet(CONNECTOR_SECRETS_INDEX_NAME, id).execute(listener.delegateFailureAndWrap((delegate, getResponse) -> {
            if (getResponse.isSourceEmpty()) {
                delegate.onFailure(new ResourceNotFoundException("No secret with id [" + id + "]"));
                return;
            }
            delegate.onResponse(new GetConnectorSecretResponse(getResponse.getId(), getResponse.getSource().get("value").toString()));
        }));
    }

    /**
     * Creates a secret in the underlying index with an auto-generated doc ID.
     *
     * @param request   Request for creating the secret.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void createSecret(PostConnectorSecretRequest request, ActionListener<PostConnectorSecretResponse> listener) {
        try {
            clientWithOrigin.prepareIndex(CONNECTOR_SECRETS_INDEX_NAME)
                .setSource(request.toXContent(jsonBuilder()))
                .execute(
                    listener.delegateFailureAndWrap(
                        (l, indexResponse) -> l.onResponse(new PostConnectorSecretResponse(indexResponse.getId()))
                    )
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Creates a secret in the underlying index with a specified doc ID.
     *
     * @param request   Request for creating the secret.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void createSecretWithDocId(PutConnectorSecretRequest request, ActionListener<PutConnectorSecretResponse> listener) {

        String connectorSecretId = request.id();

        try {
            clientWithOrigin.prepareIndex(CONNECTOR_SECRETS_INDEX_NAME)
                .setId(connectorSecretId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .execute(
                    listener.delegateFailureAndWrap(
                        (l, indexResponse) -> l.onResponse(new PutConnectorSecretResponse(indexResponse.getResult()))
                    )
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Deletes the secret in the underlying index with the specified doc ID.
     *
     * @param id        The id of the secret to delete.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void deleteSecret(String id, ActionListener<DeleteConnectorSecretResponse> listener) {
        try {
            clientWithOrigin.prepareDelete(CONNECTOR_SECRETS_INDEX_NAME, id)
                .execute(listener.delegateFailureAndWrap((delegate, deleteResponse) -> {
                    if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        delegate.onFailure(new ResourceNotFoundException("No secret with id [" + id + "]"));
                        return;
                    }
                    delegate.onResponse(new DeleteConnectorSecretResponse(deleteResponse.getResult() == DocWriteResponse.Result.DELETED));
                }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
