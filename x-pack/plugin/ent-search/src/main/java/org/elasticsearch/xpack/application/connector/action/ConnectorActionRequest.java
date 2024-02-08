/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry;

import java.io.IOException;

/**
 * Abstract base class for action requests targeting the connectors index. Implements {@link org.elasticsearch.action.IndicesRequest}
 * to ensure index-level privilege support. This class defines the connectors index as the target for all derived action requests.
 */
public abstract class ConnectorActionRequest extends ActionRequest implements IndicesRequest {

    public ConnectorActionRequest() {
        super();
    }

    public ConnectorActionRequest(StreamInput in) throws IOException {
        super(in);
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
