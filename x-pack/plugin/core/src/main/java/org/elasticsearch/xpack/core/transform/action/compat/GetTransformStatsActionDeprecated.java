/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;

public class GetTransformStatsActionDeprecated extends ActionType<GetTransformStatsAction.Response> {

    public static final GetTransformStatsActionDeprecated INSTANCE = new GetTransformStatsActionDeprecated();
    public static final String NAME = "cluster:monitor/data_frame/stats/get";

    private GetTransformStatsActionDeprecated() {
        super(NAME, GetTransformStatsAction.Response::new);
    }

}
