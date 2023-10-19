/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetJobModelSnapshotsUpgradeStatsAction.Response;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class GetJobModelSnapshotsUpgradeStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {

        int listSize = randomInt(10);
        List<Response.JobModelSnapshotUpgradeStats> statsList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            statsList.add(createRandomizedStat());
        }

        return new Response(new QueryPage<>(statsList, statsList.size(), GetJobModelSnapshotsUpgradeStatsAction.RESULTS_FIELD));
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    public static Response.JobModelSnapshotUpgradeStats createRandomizedStat() {
        Response.JobModelSnapshotUpgradeStats.Builder builder = Response.JobModelSnapshotUpgradeStats.builder(
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 20)
        ).setUpgradeState(randomFrom(SnapshotUpgradeState.values()));
        if (randomBoolean()) {
            builder.setNode(DiscoveryNodeUtils.create("_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)));
        } else {
            builder.setAssignmentExplanation(randomAlphaOfLengthBetween(20, 50));
        }
        return builder.build();
    }
}
