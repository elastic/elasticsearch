/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics.action;

import org.elasticsearch.action.ActionType;

public final class RepositoriesMetricsAction extends ActionType<RepositoriesMetricsResponse> {
    public static final RepositoriesMetricsAction INSTANCE = new RepositoriesMetricsAction();

    static final String NAME = "cluster:monitor/xpack/repositories_metrics/get_metrics";

    RepositoriesMetricsAction() {
        super(NAME, RepositoriesMetricsResponse::new);
    }
}
