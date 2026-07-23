/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse.Cluster;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CrossClusterSearchDirectoryMetricsIT extends AbstractCrossClusterSearchTestCase {

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testDirectoryMetricsLocalAndRemoteMinimizeRoundtripsTrue() throws Exception {
        assertStoreBytesRead(true, false);
    }

    public void testDirectoryMetricsLocalAndRemoteMinimizeRoundtripsFalse() throws Exception {
        assertStoreBytesRead(false, false);
    }

    public void testDirectoryMetricsRemoteOnlyMinimizeRoundtripsTrue() throws Exception {
        assertStoreBytesRead(true, true);
    }

    public void testDirectoryMetricsRemoteOnlyMinimizeRoundtripsFalse() throws Exception {
        assertStoreBytesRead(false, true);
    }

    private void assertStoreBytesRead(boolean minimizeRoundtrips, boolean remoteOnly) throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = remoteOnly
            ? new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex)
            : new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        int expectedSuccessfulClusters = remoteOnly ? 1 : 2;
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertThat(response.getClusters().getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(expectedSuccessfulClusters));
            assertThat(storeBytesRead(response.getDirectoryMetrics()), greaterThan(0L));
        });
    }

    private static long storeBytesRead(DirectoryMetrics metrics) {
        String value = metrics.entries().get(StoreMetrics.BYTES_READ_METRIC_KEY);
        return value == null ? 0L : Long.parseLong(value);
    }
}
