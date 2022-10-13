/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.ActionType;

public class AnalyzeIndexDiskUsageAction extends ActionType<AnalyzeIndexDiskUsageResponse> {
    public static final AnalyzeIndexDiskUsageAction INSTANCE = new AnalyzeIndexDiskUsageAction();
    public static final String NAME = "indices:admin/analyze_disk_usage";

    public AnalyzeIndexDiskUsageAction() {
        super(NAME, AnalyzeIndexDiskUsageResponse::new);
    }
}
