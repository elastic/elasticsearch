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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.compat.PreviewTransformActionDeprecated;
import org.elasticsearch.xpack.transform.action.TransportPreviewTransformAction;

public class TransportPreviewTransformActionDeprecated extends TransportPreviewTransformAction {

    @Inject
    public TransportPreviewTransformActionDeprecated(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Settings settings
    ) {
        super(
            PreviewTransformActionDeprecated.NAME,
            transportService,
            actionFilters,
            client,
            threadPool,
            licenseState,
            indexNameExpressionResolver,
            clusterService,
            settings
        );
    }

}
