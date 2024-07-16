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

import static org.elasticsearch.action.admin.cluster.stats.ApproximateMatcher.closeTo;
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
            boolean skippedRemote = randomBoolean();
            expectedSearchesWithSkippedRemotes = skippedRemote ? 1 : 0;
            expectedAsyncCount = async ? 1 : 0;
            expectedMinRTCount = minimizeRoundTrips ? 1 : 0;

            // per cluster telemetry
            long tookLocal = randomLongBetween(2, 8000);
            took1Remote1 = randomLongBetween(2, 8000);

            CCSUsage.Builder builder = new CCSUsage.Builder();
            builder.took(took1).setRemotesCount(1);
            if (async) {
                builder.setFeature(CCSUsageTelemetry.ASYNC_FEATURE);
            }
            if (minimizeRoundTrips) {
                builder.setFeature(CCSUsageTelemetry.MRT_FEATURE);
            }
            if (skippedRemote) {
                builder.skipRemote("remote1");
            }
            builder.perClusterUsage("(local)", new TimeValue(tookLocal));
            builder.perClusterUsage("remote1", new TimeValue(took1Remote1));

            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);

            CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();

            assertThat(snapshot.getTotalCount(), equalTo(1L));
            assertThat(snapshot.getSuccessCount(), equalTo(1L));
            assertThat(snapshot.getFeatureCounts().getOrDefault(CCSUsageTelemetry.ASYNC_FEATURE, 0L), equalTo(expectedAsyncCount));
            assertThat(snapshot.getFeatureCounts().getOrDefault(CCSUsageTelemetry.MRT_FEATURE, 0L), equalTo(expectedMinRTCount));
            assertThat(snapshot.getSkippedRemotes(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(snapshot.getTook().avg(), greaterThan(0L));
            // Expect it to be within 1% of the actual value
            assertThat(snapshot.getTook().avg(), closeTo(took1));
            assertThat(snapshot.getTook().max(), lessThanOrEqualTo(took1));
            if (minimizeRoundTrips) {
                assertThat(snapshot.getTookMrtTrue().avg(), greaterThan(0L));
                assertThat(snapshot.getTookMrtFalse().max(), equalTo(0L));
            } else {
                assertThat(snapshot.getTookMrtFalse().avg(), greaterThan(0L));
                assertThat(snapshot.getTookMrtTrue().max(), equalTo(0L));
            }
            assertThat(snapshot.getClientCounts().get("unknown"), equalTo(1L));

            // per cluster telemetry asserts

            var telemetryByCluster = snapshot.getByRemoteCluster();
            assertThat(telemetryByCluster.size(), equalTo(2));
            var localClusterTelemetry = telemetryByCluster.get("(local)");
            assertNotNull(localClusterTelemetry);
            assertThat(localClusterTelemetry.getCount(), equalTo(1L));
            assertThat(localClusterTelemetry.getSkippedCount(), equalTo(0L));
            assertThat(localClusterTelemetry.getTook().count(), equalTo(1L));
            assertThat(localClusterTelemetry.getTook().avg(), greaterThan(0L));
            assertThat(localClusterTelemetry.getTook().avg(), closeTo(tookLocal));
            // assertThat(localClusterTelemetry.getTook().max(), greaterThanOrEqualTo(tookLocal));

            var remote1ClusterTelemetry = telemetryByCluster.get("remote1");
            assertNotNull(remote1ClusterTelemetry);
            assertThat(remote1ClusterTelemetry.getCount(), equalTo(1L));
            assertThat(remote1ClusterTelemetry.getSkippedCount(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(remote1ClusterTelemetry.getTook().avg(), greaterThan(0L));
            assertThat(remote1ClusterTelemetry.getTook().count(), equalTo(1L));
            assertThat(remote1ClusterTelemetry.getTook().avg(), greaterThan(0L));
            assertThat(remote1ClusterTelemetry.getTook().avg(), closeTo(took1Remote1));
            // assertThat(remote1ClusterTelemetry.getTook().max(), greaterThanOrEqualTo(took1Remote1));
        }

        // second search
        {
            boolean minimizeRoundTrips = randomBoolean();
            boolean async = randomBoolean();
            expectedAsyncCount += async ? 1 : 0;
            expectedMinRTCount += minimizeRoundTrips ? 1 : 0;
            long took2 = randomLongBetween(5, 10000);
            boolean skippedRemote = randomBoolean();
            expectedSearchesWithSkippedRemotes += skippedRemote ? 1 : 0;
            long took2Remote1 = randomLongBetween(2, 8000);

            CCSUsage.Builder builder = new CCSUsage.Builder();
            builder.took(took2).setRemotesCount(1).setClient("kibana");
            if (async) {
                builder.setFeature(CCSUsageTelemetry.ASYNC_FEATURE);
            }
            if (minimizeRoundTrips) {
                builder.setFeature(CCSUsageTelemetry.MRT_FEATURE);
            }
            if (skippedRemote) {
                builder.skipRemote("remote1");
            }
            builder.perClusterUsage("remote1", new TimeValue(took2Remote1));

            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);

            CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();

            assertThat(snapshot.getTotalCount(), equalTo(2L));
            assertThat(snapshot.getSuccessCount(), equalTo(2L));
            assertThat(snapshot.getFeatureCounts().getOrDefault(CCSUsageTelemetry.ASYNC_FEATURE, 0L), equalTo(expectedAsyncCount));
            assertThat(snapshot.getFeatureCounts().getOrDefault(CCSUsageTelemetry.MRT_FEATURE, 0L), equalTo(expectedMinRTCount));
            assertThat(snapshot.getSkippedRemotes(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(snapshot.getTook().avg(), greaterThan(0L));
            assertThat(snapshot.getTook().avg(), closeTo((took1 + took2) / 2));
            // assertThat(snapshot.getTook().max(), greaterThanOrEqualTo(Math.max(took1, took2)));
            assertThat(snapshot.getClientCounts().size(), equalTo(2));
            assertThat(snapshot.getClientCounts().get("kibana"), equalTo(1L));
            assertThat(snapshot.getClientCounts().get("unknown"), equalTo(1L));

            // per cluster telemetry asserts

            var telemetryByCluster = snapshot.getByRemoteCluster();
            assertThat(telemetryByCluster.size(), equalTo(2));
            var localClusterTelemetry = telemetryByCluster.get("(local)");
            assertNotNull(localClusterTelemetry);
            assertThat(localClusterTelemetry.getCount(), equalTo(1L));
            assertThat(localClusterTelemetry.getSkippedCount(), equalTo(0L));
            assertThat(localClusterTelemetry.getTook().count(), equalTo(1L));

            var remote1ClusterTelemetry = telemetryByCluster.get("remote1");
            assertNotNull(remote1ClusterTelemetry);
            assertThat(remote1ClusterTelemetry.getCount(), equalTo(2L));
            assertThat(remote1ClusterTelemetry.getSkippedCount(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(remote1ClusterTelemetry.getTook().avg(), greaterThan(0L));
            assertThat(remote1ClusterTelemetry.getTook().count(), equalTo(2L));
            assertThat(remote1ClusterTelemetry.getTook().avg(), greaterThan(0L));
            assertThat(remote1ClusterTelemetry.getTook().avg(), closeTo((took1Remote1 + took2Remote1) / 2));
            // assertThat(remote1ClusterTelemetry.getTook().max(), greaterThanOrEqualTo(Math.max(took1Remote1, took2Remote1)));
        }
    }
}
