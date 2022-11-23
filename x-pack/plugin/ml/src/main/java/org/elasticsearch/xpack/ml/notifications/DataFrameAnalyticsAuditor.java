/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.ml.notifications.DataFrameAnalyticsAuditMessage;

public class DataFrameAnalyticsAuditor extends AbstractMlAuditor<DataFrameAnalyticsAuditMessage> {

    public DataFrameAnalyticsAuditor(Client client, ClusterService clusterService) {
        super(client, DataFrameAnalyticsAuditMessage::new, clusterService);
    }
}
