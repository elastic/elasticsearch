/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;

public class DesiredNodesService {
    private final Logger logger = LogManager.getLogger(DesiredNodesService.class);

    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;

    public DesiredNodesService(ClusterService clusterService, ClusterSettings clusterSettings) {
        this.clusterService = clusterService;
        this.clusterSettings = clusterSettings;
    }

    public void updateDesiredNodes(UpdateDesiredNodesRequest request, ActionListener<AcknowledgedResponse> listener) {}

}
