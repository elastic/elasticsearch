/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry;

import java.io.IOException;

/**
 * Abstract base class for action requests targeting the connector sync job index.
 * Implements {@link org.elasticsearch.action.IndicesRequest} to ensure index-level privilege support.
 * This class defines the connectors sync job index as the target for all derived action requests.
 */
public abstract class ConnectorSyncJobActionRequest extends LegacyActionRequest implements IndicesRequest {

    public ConnectorSyncJobActionRequest() {
        super();
    }

    public ConnectorSyncJobActionRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String[] indices() {
        return new String[] { ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandHidden();
    }
}
