/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;

public class GetDeploymentStatsActionResponseTests extends AbstractWireSerializingTestCase<GetDeploymentStatsAction.Response> {
    @Override
    protected Writeable.Reader<GetDeploymentStatsAction.Response> instanceReader() {
        return GetDeploymentStatsAction.Response::new;
    }

    @Override
    protected GetDeploymentStatsAction.Response createTestInstance() {
        int numStats = randomIntBetween(0, 2);
        var stats = new ArrayList<GetDeploymentStatsAction.Response.DeploymentStats>(numStats);
        for (var i=0; i<numStats; i++) {
            stats.add(randomDeploymentStats());
        }
        return new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), stats, stats.size());
    }

    private GetDeploymentStatsAction.Response.DeploymentStats randomDeploymentStats() {
        return new GetDeploymentStatsAction.Response.DeploymentStats(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomNonNegativeLong(),
            randomDoubleBetween(0.0, 100.0, true),
            Instant.now(),
            ByteSizeValue.ofBytes(randomNonNegativeLong()));
    }
}
