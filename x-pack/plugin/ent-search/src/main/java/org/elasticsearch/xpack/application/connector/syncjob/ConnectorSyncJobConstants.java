/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import static org.elasticsearch.xpack.application.connector.syncjob.action.DeleteConnectorSyncJobAction.Request.CONNECTOR_SYNC_JOB_ID_FIELD;

public class ConnectorSyncJobConstants {

    public static final String EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE =
        "[connector_sync_job_id] of the connector sync job cannot be null or empty.";
    public static final String EMPTY_WORKER_HOSTNAME_ERROR_MESSAGE = "[worker_hostname] of the connector sync job cannot be null.";
    public static final String CONNECTOR_SYNC_JOB_ID_PARAM = CONNECTOR_SYNC_JOB_ID_FIELD.getPreferredName();

}
