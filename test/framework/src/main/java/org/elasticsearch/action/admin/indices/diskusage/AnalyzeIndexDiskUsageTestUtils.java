/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.core.Nullable;

public class AnalyzeIndexDiskUsageTestUtils {

    private AnalyzeIndexDiskUsageTestUtils() {}

    @Nullable
    public static IndexDiskUsageStats getIndexStats(final AnalyzeIndexDiskUsageResponse diskUsageResponse, final String indexName) {
        var stats = diskUsageResponse.getStats();
        if (stats != null) {
            return stats.get(indexName);
        }
        return null;
    }

    @Nullable
    public static IndexDiskUsageStats.PerFieldDiskUsage getPerFieldDiskUsage(
        final IndexDiskUsageStats indexDiskUsageStats,
        final String fieldName
    ) {
        if (indexDiskUsageStats != null) {
            var fields = indexDiskUsageStats.getFields();
            if (fields != null) {
                return fields.get(fieldName);
            }
        }
        return null;
    }
}
