/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.ActionType;

public final class RepositoriesStatsAction extends ActionType<RepositoriesStatsResponse> {
    public static final RepositoriesStatsAction INSTANCE = new RepositoriesStatsAction();

    static final String NAME = "cluster:monitor/xpack/repositories_stats/get_stats";

    RepositoriesStatsAction() {
        super(NAME, RepositoriesStatsResponse::new);
    }
}
