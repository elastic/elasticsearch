/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformStatsTests;

import java.util.ArrayList;
import java.util.List;

public class GetTransformStatsActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {
    @Override
    protected Response createTestInstance() {
        List<TransformStats> stats = new ArrayList<>();
        int totalStats = randomInt(10);
        for (int i = 0; i < totalStats; ++i) {
            stats.add(TransformStatsTests.randomDataFrameTransformStats());
        }
        int totalErrors = randomInt(10);
        List<TaskOperationFailure> taskFailures = new ArrayList<>(totalErrors);
        List<ElasticsearchException> nodeFailures = new ArrayList<>(totalErrors);
        for (int i = 0; i < totalErrors; i++) {
            taskFailures.add(new TaskOperationFailure("node1", randomLongBetween(1, 10), new Exception("error")));
            nodeFailures.add(new FailedNodeException("node1", "message", new Exception("error")));
        }
        return new Response(stats, randomLongBetween(stats.size(), 10_000_000L), taskFailures, nodeFailures);
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }
}
