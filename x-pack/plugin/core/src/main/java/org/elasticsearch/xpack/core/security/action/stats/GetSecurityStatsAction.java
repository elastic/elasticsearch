/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.stats;

import org.elasticsearch.action.ActionType;

public class GetSecurityStatsAction extends ActionType<GetSecurityStatsNodesResponse> {

    public static final GetSecurityStatsAction INSTANCE = new GetSecurityStatsAction();
    public static final String NAME = "cluster:monitor/xpack/security/stats";

    private GetSecurityStatsAction() {
        super(NAME);
    }
}
