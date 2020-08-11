/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics.action;

import org.elasticsearch.action.ActionType;

public final class ClearRepositoriesMetricsArchiveAction extends ActionType<RepositoriesMetricsResponse> {
    public static final ClearRepositoriesMetricsArchiveAction INSTANCE = new ClearRepositoriesMetricsArchiveAction();

    static final String NAME = "cluster:monitor/xpack/repositories_metrics/clear_repositories_metrics_archive";

    ClearRepositoriesMetricsArchiveAction() {
        super(NAME, RepositoriesMetricsResponse::new);
    }
}
