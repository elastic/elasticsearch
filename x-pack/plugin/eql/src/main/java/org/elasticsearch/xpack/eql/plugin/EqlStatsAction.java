/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.ActionType;

public class EqlStatsAction extends ActionType<EqlStatsResponse> {

    public static final EqlStatsAction INSTANCE = new EqlStatsAction();
    public static final String NAME = "cluster:monitor/xpack/eql/stats/dist";

    private EqlStatsAction() {
        super(NAME, EqlStatsResponse::new);
    }
}
