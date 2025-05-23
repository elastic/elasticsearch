/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Abstract base class for action requests targeting the connectors index. Implements {@link org.elasticsearch.action.IndicesRequest}
 * to ensure index-level privilege support. This class defines the connectors index as the target for all derived action requests.
 */
public abstract class ConnectorActionRequest extends LegacyActionRequest implements IndicesRequest {

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

    @Override
    public String[] indices() {
        return new String[] { ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandHidden();
    }
}
