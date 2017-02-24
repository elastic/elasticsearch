/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;
import org.joda.time.DateTime;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class GetJobStatsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Response.JobStats> jobStatsList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String jobId = randomAsciiOfLength(10);

            DataCounts dataCounts = new DataCounts(randomAsciiOfLength(10), randomIntBetween(1, 1_000_000),
                    randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                    randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                    new DateTime(randomDateTimeZone()).toDate(), new DateTime(randomDateTimeZone()).toDate());

            ModelSizeStats sizeStats = null;
            if (randomBoolean()) {
                sizeStats = new ModelSizeStats.Builder("foo").build();
            }
            JobState jobState = randomFrom(EnumSet.allOf(JobState.class));

            DiscoveryNode node = null;
            if (randomBoolean()) {
                node = new DiscoveryNode("_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
            }
            String explanation = null;
            if (randomBoolean()) {
                explanation = randomAsciiOfLength(3);
            }
            Response.JobStats jobStats = new Response.JobStats(jobId, dataCounts, sizeStats, jobState, node, explanation);
            jobStatsList.add(jobStats);
        }

        result = new Response(new QueryPage<>(jobStatsList, jobStatsList.size(), Job.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
