/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.InvalidIndexNameException;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.MANAGED_CONNECTOR_INDEX_PREFIX;

/**
 * Abstract base class for action requests targeting the connectors index.
 */
public abstract class ConnectorActionRequest extends ActionRequest {

    public ConnectorActionRequest() {
        super();
    }

    public ConnectorActionRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Validates the given index name and updates the validation exception if the name is invalid.
     *
     * @param indexName The index name to validate. If null, no validation is performed.
     * @param validationException The exception to accumulate validation errors.
     * @return The updated or original {@code validationException} with any new validation errors added, if the index name is invalid.
     */
    public ActionRequestValidationException validateIndexName(String indexName, ActionRequestValidationException validationException) {
        if (indexName != null) {
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(indexName, InvalidIndexNameException::new);
            } catch (InvalidIndexNameException e) {
                return addValidationError(e.toString(), validationException);
            }
        }
        return validationException;
    }

    /**
     * Validates that the given index name starts with the required prefix for Elastic-managed connectors.
     * If the index name does not start with the required prefix, the validation exception is updated with an error message.
     *
     * @param indexName The index name to validate. If null, no validation is performed.
     * @param validationException The exception to accumulate validation errors.
     * @return The updated or original {@code validationException} with any new validation errors added,
     *         if the index name does not start with the required prefix.
     */
    public ActionRequestValidationException validateManagedConnectorIndexPrefix(
        String indexName,
        ActionRequestValidationException validationException
    ) {
        if (indexName != null && indexName.startsWith(MANAGED_CONNECTOR_INDEX_PREFIX) == false) {
            return addValidationError(
                "Index ["
                    + indexName
                    + "] is invalid. Index attached to an Elastic-managed connector must start with the prefix: ["
                    + MANAGED_CONNECTOR_INDEX_PREFIX
                    + "]",
                validationException
            );
        }
        return validationException;
    }
}
