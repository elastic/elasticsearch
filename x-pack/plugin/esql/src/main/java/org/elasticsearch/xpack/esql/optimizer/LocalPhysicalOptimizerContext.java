/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.AvoidFieldExtractionAfterTopN;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public record LocalPhysicalOptimizerContext(
    EsqlFlags flags,
    Configuration configuration,
    FoldContext foldCtx,
    SearchStats searchStats,
    ProjectAfterTopN removeProjectAfterTopN
) {
    /**
     * Controls whether to run the {@link AvoidFieldExtractionAfterTopN}. Runs on
     * {@code REMOVE}, skipped on {@code KEEP}.
     */
    // FIXME(gal, NOCOMMIT) rename
    public enum ProjectAfterTopN {
        REMOVE,
        KEEP
    }
}
