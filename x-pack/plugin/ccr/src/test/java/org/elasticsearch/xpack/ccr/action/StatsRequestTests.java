/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;

public class StatsRequestTests extends AbstractStreamableTestCase<CcrStatsAction.StatsRequest> {

    @Override
    protected CcrStatsAction.StatsRequest createBlankInstance() {
        return new CcrStatsAction.StatsRequest();
    }

    @Override
    protected CcrStatsAction.StatsRequest createTestInstance() {
        CcrStatsAction.StatsRequest statsRequest = new CcrStatsAction.StatsRequest();
        if (randomBoolean()) {
            statsRequest.setIndices(generateRandomStringArray(8, 4, false));
        }
        return statsRequest;
    }
}
