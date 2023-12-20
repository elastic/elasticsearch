/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStatsTests.randomDeploymentStats;

public class GetDeploymentStatsActionResponseTests extends AbstractWireSerializingTestCase<GetDeploymentStatsAction.Response> {
    @Override
    protected Writeable.Reader<GetDeploymentStatsAction.Response> instanceReader() {
        return GetDeploymentStatsAction.Response::new;
    }

    @Override
    protected GetDeploymentStatsAction.Response createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetDeploymentStatsAction.Response mutateInstance(GetDeploymentStatsAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static GetDeploymentStatsAction.Response createRandom() {
        int numStats = randomIntBetween(0, 2);
        var stats = new ArrayList<AssignmentStats>(numStats);
        for (var i = 0; i < numStats; i++) {
            stats.add(randomDeploymentStats());
        }
        stats.sort(Comparator.comparing(AssignmentStats::getDeploymentId));
        return new GetDeploymentStatsAction.Response(Collections.emptyList(), Collections.emptyList(), stats, stats.size());
    }

}
