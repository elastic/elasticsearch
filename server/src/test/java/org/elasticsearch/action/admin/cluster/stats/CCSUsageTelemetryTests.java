/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CCSUsageTelemetryTests extends ESTestCase {

    public void testSuccessfulSearchResults() {
        CCSUsageTelemetry ccsUsageHolder = new CCSUsageTelemetry();

        long expectedAsyncCount = 0L;
        long expectedMinRTCount = 0L;
        long expectedSearchesWithSkippedRemotes = 0L;
        long took1 = 0L;
        long took1Remote1 = 0L;

        // first search
        {
            boolean minimizeRoundTrips = randomBoolean();
            boolean async = randomBoolean();
            took1 = randomLongBetween(5, 10000);
            int numSkippedRemotes = randomIntBetween(0, 3);
            expectedSearchesWithSkippedRemotes = numSkippedRemotes > 0 ? 1 : 0;
            expectedAsyncCount = async ? 1 : 0;
            expectedMinRTCount = minimizeRoundTrips ? 1 : 0;

            // per cluster telemetry
            long tookLocal = randomLongBetween(2, 8000);
            took1Remote1 = randomLongBetween(2, 8000);
            Map<String, CCSUsage.PerClusterUsage> perClusterUsage = Map.of(
                "(local)",
                new CCSUsage.PerClusterUsage(new TimeValue(tookLocal)),
                "remote1",
                new CCSUsage.PerClusterUsage(new TimeValue(took1Remote1))
            );

            CCSUsage ccsUsage = new CCSUsage.Builder().took(took1)
                .async(async)
                .minimizeRoundTrips(minimizeRoundTrips)
                .numSkippedRemotes(numSkippedRemotes)
                .perClusterUsage(perClusterUsage)
                .build();

            ccsUsageHolder.updateUsage(ccsUsage);

            assertThat(ccsUsageHolder.getTotalCCSCount(), equalTo(1L));
            CCSUsageTelemetry.SuccessfulCCSTelemetry successfulCCSTelemetry = ccsUsageHolder.getSuccessfulSearchTelemetry();
            assertThat(successfulCCSTelemetry.getCount(), equalTo(1L));
            assertThat(successfulCCSTelemetry.getCountAsync(), equalTo(expectedAsyncCount));
            assertThat(successfulCCSTelemetry.getCountMinimizeRoundtrips(), equalTo(expectedMinRTCount));
            assertThat(successfulCCSTelemetry.getCountSearchesWithSkippedRemotes(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(successfulCCSTelemetry.getMeanLatency(), greaterThan(0.0d));
            assertThat(successfulCCSTelemetry.getMeanLatency(), lessThanOrEqualTo((double) took1));

            // per cluster telemetry asserts
            Map<String, CCSUsageTelemetry.PerClusterCCSTelemetry> telemetryByCluster = ccsUsageHolder.getTelemetryByCluster();
            assertThat(telemetryByCluster.size(), equalTo(2));
            var localClusterTelemetry = telemetryByCluster.get("(local)");
            assertNotNull(localClusterTelemetry);
            assertThat(localClusterTelemetry.getCount(), equalTo(1L));
            assertThat(localClusterTelemetry.getMeanLatency(), greaterThan(0.0d));
            assertThat(localClusterTelemetry.getMeanLatency(), lessThanOrEqualTo((double) tookLocal));

            var remote1ClusterTelemetry = telemetryByCluster.get("remote1");
            assertNotNull(remote1ClusterTelemetry);
            assertThat(remote1ClusterTelemetry.getCount(), equalTo(1L));
            assertThat(remote1ClusterTelemetry.getMeanLatency(), greaterThan(0.0d));
            assertThat(remote1ClusterTelemetry.getMeanLatency(), lessThanOrEqualTo((double) took1Remote1));
        }

        // second search
        {
            boolean minimizeRoundTrips = randomBoolean();
            boolean async = randomBoolean();
            expectedAsyncCount += async ? 1 : 0;
            expectedMinRTCount += minimizeRoundTrips ? 1 : 0;
            long took2 = randomLongBetween(5, 10000);
            int numSkippedRemotes = randomIntBetween(0, 3);
            expectedSearchesWithSkippedRemotes += numSkippedRemotes > 0 ? 1 : 0;

            long took2Remote1 = randomLongBetween(2, 8000);
            Map<String, CCSUsage.PerClusterUsage> perClusterUsage = Map.of(
                "remote1",
                new CCSUsage.PerClusterUsage(new TimeValue(took1Remote1))
            );

            CCSUsage ccsUsage = new CCSUsage.Builder().took(took2)
                .async(async)
                .minimizeRoundTrips(minimizeRoundTrips)
                .numSkippedRemotes(numSkippedRemotes)
                .perClusterUsage(perClusterUsage)
                .build();

            ccsUsageHolder.updateUsage(ccsUsage);

            assertThat(ccsUsageHolder.getTotalCCSCount(), equalTo(2L));
            CCSUsageTelemetry.SuccessfulCCSTelemetry successfulCCSTelemetry = ccsUsageHolder.getSuccessfulSearchTelemetry();
            assertThat(successfulCCSTelemetry.getCount(), equalTo(2L));
            assertThat(successfulCCSTelemetry.getCountAsync(), equalTo(expectedAsyncCount));
            assertThat(successfulCCSTelemetry.getCountMinimizeRoundtrips(), equalTo(expectedMinRTCount));
            assertThat(successfulCCSTelemetry.getCountSearchesWithSkippedRemotes(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(successfulCCSTelemetry.getMeanLatency(), greaterThan(0.0d));
            assertThat(successfulCCSTelemetry.getMeanLatency(), lessThanOrEqualTo((double) Math.max(took1, took2)));

            // per cluster telemetry asserts
            Map<String, CCSUsageTelemetry.PerClusterCCSTelemetry> telemetryByCluster = ccsUsageHolder.getTelemetryByCluster();
            assertThat(telemetryByCluster.size(), equalTo(2));
            var localClusterTelemetry = telemetryByCluster.get("(local)");
            assertNotNull(localClusterTelemetry);
            assertThat(localClusterTelemetry.getCount(), equalTo(1L));  // not part of second search

            var remote1ClusterTelemetry = telemetryByCluster.get("remote1");
            assertNotNull(remote1ClusterTelemetry);
            assertThat(remote1ClusterTelemetry.getCount(), equalTo(2L));
            assertThat(remote1ClusterTelemetry.getMeanLatency(), greaterThan(0.0d));
            assertThat(remote1ClusterTelemetry.getMeanLatency(), lessThanOrEqualTo((double) Math.max(took1Remote1, took2Remote1)));
        }
    }
}
