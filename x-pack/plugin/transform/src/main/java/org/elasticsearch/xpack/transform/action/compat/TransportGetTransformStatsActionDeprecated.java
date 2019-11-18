/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action.compat;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.compat.GetTransformStatsActionDeprecated;
import org.elasticsearch.xpack.transform.action.TransportGetTransformStatsAction;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

public class TransportGetTransformStatsActionDeprecated extends TransportGetTransformStatsAction {

    @Inject
    public TransportGetTransformStatsActionDeprecated(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TransformConfigManager transformsConfigManager,
        TransformCheckpointService transformsCheckpointService
    ) {
        super(
            GetTransformStatsActionDeprecated.NAME,
            transportService,
            actionFilters,
            clusterService,
            transformsConfigManager,
            transformsCheckpointService
        );
    }
}
