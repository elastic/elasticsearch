/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.backups;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.health.GetHealthAction;

import java.util.Collections;
import java.util.List;

public class Backups extends GetHealthAction.Component {

    @Override
    public String getName() {
        return "backups";
    }

    @Override
    public ClusterHealthStatus getStatus() {
        return ClusterHealthStatus.GREEN;
    }

    @Override
    public List<GetHealthAction.Indicator> getIndicators() {
        return Collections.emptyList();
    }
}
