/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ActionType;

public class FieldUsageStatsAction extends ActionType<FieldUsageStatsResponse> {

    public static final FieldUsageStatsAction INSTANCE = new FieldUsageStatsAction();
    public static final String NAME = "indices:monitor/field_usage_stats";

    private FieldUsageStatsAction() {
        super(NAME, FieldUsageStatsResponse::new);
    }
}
