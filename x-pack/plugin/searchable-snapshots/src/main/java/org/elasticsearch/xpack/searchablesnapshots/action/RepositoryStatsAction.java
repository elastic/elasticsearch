/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionType;

public class RepositoryStatsAction extends ActionType<RepositoryStatsResponse> {

    public static final RepositoryStatsAction INSTANCE = new RepositoryStatsAction();
    public static final String NAME = "cluster:admin/repository/stats";

    private RepositoryStatsAction() {
        super(NAME, RepositoryStatsResponse::new);
    }
}
