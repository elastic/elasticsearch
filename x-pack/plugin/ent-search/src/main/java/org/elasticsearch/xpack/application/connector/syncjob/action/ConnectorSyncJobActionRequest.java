/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequest;

/**
 * Abstract base class for action requests targeting the connector sync job index.
 */
public abstract class ConnectorSyncJobActionRequest extends ActionRequest {

    public ConnectorSyncJobActionRequest() {
        super();
    }
}
