/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.AutoFollowStatsAction;

import static org.elasticsearch.xpack.ccr.action.AutoFollowStatsTests.randomReadExceptions;

public class AutoFollowStatsResponseTests extends AbstractStreamableTestCase<AutoFollowStatsAction.Response> {

    @Override
    protected AutoFollowStatsAction.Response createBlankInstance() {
        return new AutoFollowStatsAction.Response();
    }

    @Override
    protected AutoFollowStatsAction.Response createTestInstance() {
        AutoFollowStats autoFollowStats = new AutoFollowStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomReadExceptions()
        );
        return new AutoFollowStatsAction.Response(autoFollowStats);
    }
}
