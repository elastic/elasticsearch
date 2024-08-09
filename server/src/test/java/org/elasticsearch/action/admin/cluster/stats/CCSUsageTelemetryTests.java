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
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.ASYNC_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.KNOWN_CLIENTS;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.MRT_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.Result.CANCELED;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.WILDCARD_FEATURE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
                builder.setFeature(ASYNC_FEATURE);
            }
            if (minimizeRoundTrips) {
                builder.setFeature(MRT_FEATURE);
            }
            if (skippedRemote) {
                builder.skippedRemote("remote1");
            }
            builder.perClusterUsage("(local)", new TimeValue(tookLocal));
            builder.perClusterUsage("remote1", new TimeValue(took1Remote1));

            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);

            CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();

            assertThat(snapshot.getTotalCount(), equalTo(1L));
            assertThat(snapshot.getSuccessCount(), equalTo(1L));
            assertThat(snapshot.getFeatureCounts().getOrDefault(ASYNC_FEATURE, 0L), equalTo(expectedAsyncCount));
            assertThat(snapshot.getFeatureCounts().getOrDefault(MRT_FEATURE, 0L), equalTo(expectedMinRTCount));
            assertThat(snapshot.getSearchCountWithSkippedRemotes(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(snapshot.getTook().avg(), greaterThan(0L));
            // Expect it to be within 1% of the actual value
            assertThat(snapshot.getTook().avg(), closeTo(took1));
            assertThat(snapshot.getTook().max(), closeTo(took1));
            if (minimizeRoundTrips) {
                assertThat(snapshot.getTookMrtTrue().count(), equalTo(1L));
                assertThat(snapshot.getTookMrtTrue().avg(), greaterThan(0L));
                assertThat(snapshot.getTookMrtTrue().avg(), closeTo(took1));
                assertThat(snapshot.getTookMrtFalse().count(), equalTo(0L));
                assertThat(snapshot.getTookMrtFalse().max(), equalTo(0L));
            } else {
                assertThat(snapshot.getTookMrtFalse().count(), equalTo(1L));
                assertThat(snapshot.getTookMrtFalse().avg(), greaterThan(0L));
                assertThat(snapshot.getTookMrtFalse().avg(), closeTo(took1));
                assertThat(snapshot.getTookMrtTrue().count(), equalTo(0L));
                assertThat(snapshot.getTookMrtTrue().max(), equalTo(0L));
            }
            // We currently don't count unknown clients
            assertThat(snapshot.getClientCounts().size(), equalTo(0));

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
                builder.setFeature(ASYNC_FEATURE);
            }
            if (minimizeRoundTrips) {
                builder.setFeature(MRT_FEATURE);
            }
            if (skippedRemote) {
                builder.skippedRemote("remote1");
            }
            builder.perClusterUsage("remote1", new TimeValue(took2Remote1));

            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);

            CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();

            assertThat(snapshot.getTotalCount(), equalTo(2L));
            assertThat(snapshot.getSuccessCount(), equalTo(2L));
            assertThat(snapshot.getFeatureCounts().getOrDefault(ASYNC_FEATURE, 0L), equalTo(expectedAsyncCount));
            assertThat(snapshot.getFeatureCounts().getOrDefault(MRT_FEATURE, 0L), equalTo(expectedMinRTCount));
            assertThat(snapshot.getSearchCountWithSkippedRemotes(), equalTo(expectedSearchesWithSkippedRemotes));
            assertThat(snapshot.getTook().avg(), greaterThan(0L));
            assertThat(snapshot.getTook().avg(), closeTo((took1 + took2) / 2));
            // assertThat(snapshot.getTook().max(), greaterThanOrEqualTo(Math.max(took1, took2)));

            // Counting only known clients
            assertThat(snapshot.getClientCounts().get("kibana"), equalTo(1L));
            assertThat(snapshot.getClientCounts().size(), equalTo(1));

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

    public void testClientsLimit() {
        CCSUsageTelemetry ccsUsageHolder = new CCSUsageTelemetry();
        // Add known clients
        for (String knownClient : KNOWN_CLIENTS) {
            CCSUsage.Builder builder = new CCSUsage.Builder();
            builder.took(randomLongBetween(5, 10000)).setRemotesCount(1).setClient(knownClient);
            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);
        }
        var counts = ccsUsageHolder.getCCSTelemetrySnapshot().getClientCounts();
        for (String knownClient : KNOWN_CLIENTS) {
            assertThat(counts.get(knownClient), equalTo(1L));
        }
        // Check that knowns are counted
        for (String knownClient : KNOWN_CLIENTS) {
            CCSUsage.Builder builder = new CCSUsage.Builder();
            builder.took(randomLongBetween(5, 10000)).setRemotesCount(1).setClient(knownClient);
            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);
        }
        counts = ccsUsageHolder.getCCSTelemetrySnapshot().getClientCounts();
        for (String knownClient : KNOWN_CLIENTS) {
            assertThat(counts.get(knownClient), equalTo(2L));
        }
        // Check that new clients are not counted
        CCSUsage.Builder builder = new CCSUsage.Builder();
        String randomClient = randomAlphaOfLength(10);
        builder.took(randomLongBetween(5, 10000)).setRemotesCount(1).setClient(randomClient);
        CCSUsage ccsUsage = builder.build();
        ccsUsageHolder.updateUsage(ccsUsage);
        counts = ccsUsageHolder.getCCSTelemetrySnapshot().getClientCounts();
        assertThat(counts.get(randomClient), equalTo(null));
    }

    public void testFailures() {
        CCSUsageTelemetry ccsUsageHolder = new CCSUsageTelemetry();

        // first search
        {
            boolean skippedRemote = randomBoolean();
            boolean minimizeRoundTrips = randomBoolean();
            boolean async = randomBoolean();

            CCSUsage.Builder builder = new CCSUsage.Builder();
            builder.setRemotesCount(1).took(10L);
            if (skippedRemote) {
                builder.skippedRemote("remote1");
            }
            builder.perClusterUsage("(local)", new TimeValue(1));
            builder.perClusterUsage("remote1", new TimeValue(2));
            builder.setFailure(CANCELED);
            if (async) {
                builder.setFeature(ASYNC_FEATURE);
            }
            if (minimizeRoundTrips) {
                builder.setFeature(MRT_FEATURE);
            }

            CCSUsage ccsUsage = builder.build();
            ccsUsageHolder.updateUsage(ccsUsage);

            CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();

            assertThat(snapshot.getTotalCount(), equalTo(1L));
            assertThat(snapshot.getSuccessCount(), equalTo(0L));
            assertThat(snapshot.getSearchCountWithSkippedRemotes(), equalTo(skippedRemote ? 1L : 0L));
            assertThat(snapshot.getTook().count(), equalTo(0L));
            assertThat(snapshot.getFailureReasons().size(), equalTo(1));
            assertThat(snapshot.getFailureReasons().get(CANCELED.getName()), equalTo(1L));
            // still counting features on failure
            assertThat(snapshot.getFeatureCounts().getOrDefault(ASYNC_FEATURE, 0L), equalTo(async ? 1L : 0L));
            assertThat(snapshot.getFeatureCounts().getOrDefault(MRT_FEATURE, 0L), equalTo(minimizeRoundTrips ? 1L : 0L));
        }

        // second search
        {
            CCSUsage.Builder builder = new CCSUsage.Builder();
            boolean skippedRemote = randomBoolean();
            builder.setRemotesCount(1).took(10L).setClient("kibana");
            if (skippedRemote) {
                builder.skippedRemote("remote1");
            }
            builder.setFailure(CANCELED);
            CCSUsage ccsUsage = builder.build();

            ccsUsageHolder.updateUsage(ccsUsage);

            CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();

            assertThat(snapshot.getTotalCount(), equalTo(2L));
            assertThat(snapshot.getSuccessCount(), equalTo(0L));
            assertThat(snapshot.getTook().count(), equalTo(0L));
            assertThat(snapshot.getFailureReasons().size(), equalTo(1));
            assertThat(snapshot.getFailureReasons().get(CANCELED.getName()), equalTo(2L));
            assertThat(snapshot.getClientCounts().get("kibana"), equalTo(1L));
        }
    }

    public void testConcurrentUpdates() throws InterruptedException {
        CCSUsageTelemetry ccsUsageHolder = new CCSUsageTelemetry();
        CCSUsageTelemetry expectedHolder = new CCSUsageTelemetry();
        int numSearches = randomIntBetween(1000, 5000);
        int numThreads = randomIntBetween(10, 20);
        Thread[] threads = new Thread[numThreads];
        CCSUsage[] ccsUsages = new CCSUsage[numSearches];

        // Make random usage objects
        for (int i = 0; i < numSearches; i++) {
            CCSUsage.Builder builder = new CCSUsage.Builder();
            builder.took(randomLongBetween(5, 10000)).setRemotesCount(randomIntBetween(1, 10));
            if (randomBoolean()) {
                builder.setFeature(ASYNC_FEATURE);
            }
            if (randomBoolean()) {
                builder.setFeature(WILDCARD_FEATURE);
            }
            if (randomBoolean()) {
                builder.setFeature(MRT_FEATURE);
            }
            if (randomBoolean()) {
                builder.setClient("kibana");
            }
            if (randomInt(20) == 7) {
                // 5% of requests will fail
                builder.setFailure(randomFrom(CCSUsageTelemetry.Result.values()));
                ccsUsages[i] = builder.build();
                continue;
            }
            builder.perClusterUsage("", new TimeValue(randomLongBetween(1, 10000)));
            if (randomBoolean()) {
                builder.skippedRemote("remote1");
            } else {
                builder.perClusterUsage("remote1", new TimeValue(randomLongBetween(1, 10000)));
            }
            builder.perClusterUsage(randomFrom("remote2", "remote3", "remote4"), new TimeValue(randomLongBetween(1, 10000)));
            ccsUsages[i] = builder.build();
        }

        // Add each of the search objects to the telemetry holder in a different thread
        for (int i = 0; i < numThreads; i++) {
            final int threadNo = i;
            threads[i] = new Thread(() -> {
                for (int j = threadNo; j < numSearches; j += numThreads) {
                    ccsUsageHolder.updateUsage(ccsUsages[j]);
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        // Add the same search objects to the expected holder in a single thread
        for (int i = 0; i < numSearches; i++) {
            expectedHolder.updateUsage(ccsUsages[i]);
        }

        CCSTelemetrySnapshot snapshot = ccsUsageHolder.getCCSTelemetrySnapshot();
        CCSTelemetrySnapshot expectedSnapshot = ccsUsageHolder.getCCSTelemetrySnapshot();
        assertThat(snapshot, equalTo(expectedSnapshot));
    }
}
