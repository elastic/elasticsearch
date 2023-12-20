/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

public class StatsRequestTests extends AbstractWireSerializingTestCase<FollowStatsAction.StatsRequest> {

    @Override
    protected Writeable.Reader<FollowStatsAction.StatsRequest> instanceReader() {
        return FollowStatsAction.StatsRequest::new;
    }

    @Override
    protected FollowStatsAction.StatsRequest createTestInstance() {
        FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
        if (randomBoolean()) {
            statsRequest.setIndices(generateRandomStringArray(8, 4, false));
        }
        return statsRequest;
    }

    @Override
    protected FollowStatsAction.StatsRequest mutateInstance(FollowStatsAction.StatsRequest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
