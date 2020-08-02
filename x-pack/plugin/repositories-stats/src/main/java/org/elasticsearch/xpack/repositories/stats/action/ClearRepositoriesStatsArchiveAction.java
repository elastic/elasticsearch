/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.ActionType;

public final class ClearRepositoriesStatsArchiveAction extends ActionType<ClearRepositoriesStatsArchiveResponse> {
    public static final ClearRepositoriesStatsArchiveAction INSTANCE = new ClearRepositoriesStatsArchiveAction();

    static final String NAME = "cluster:monitor/xpack/repositories_stats/clear_repositories_stats_archive";

    ClearRepositoriesStatsArchiveAction() {
        super(NAME, ClearRepositoriesStatsArchiveResponse::new);
    }
}
