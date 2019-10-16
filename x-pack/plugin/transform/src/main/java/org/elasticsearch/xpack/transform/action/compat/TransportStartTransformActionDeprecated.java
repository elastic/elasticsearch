/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action.compat;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.compat.StartTransformActionDeprecated;
import org.elasticsearch.xpack.transform.action.TransportStartTransformAction;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

public class TransportStartTransformActionDeprecated extends TransportStartTransformAction {

    @Inject
    public TransportStartTransformActionDeprecated(TransportService transportService, ActionFilters actionFilters,
                                                   ClusterService clusterService, XPackLicenseState licenseState,
                                                   ThreadPool threadPool, IndexNameExpressionResolver indexNameExpressionResolver,
                                                   TransformConfigManager transformConfigManager,
                                                   PersistentTasksService persistentTasksService, Client client,
                                                   TransformAuditor auditor) {
        super(StartTransformActionDeprecated.NAME, transportService, actionFilters, clusterService, licenseState, threadPool,
              indexNameExpressionResolver, transformConfigManager, persistentTasksService, client, auditor);
    }
}
