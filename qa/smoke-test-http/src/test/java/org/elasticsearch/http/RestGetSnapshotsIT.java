/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.assertSorted;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class RestGetSnapshotsIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
                .build();
    }

    public void testSortOrder() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        final List<String> snapshotNamesWithoutIndex =
            AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(3, 20));

        createIndexWithContent("test-index");

        final List<String> snapshotNamesWithIndex =
            AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(3, 20));

        final Collection<String> allSnapshotNames = new HashSet<>(snapshotNamesWithIndex);
        allSnapshotNames.addAll(snapshotNamesWithoutIndex);

        final List<SnapshotInfo> defaultSorting = clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots(repoName);
        assertSorted(defaultSorting, null);
        assertSorted(allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.NAME), GetSnapshotsAction.SortBy.NAME);
        assertSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.DURATION), GetSnapshotsAction.SortBy.DURATION
        );
        assertSorted(allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.INDICES), GetSnapshotsAction.SortBy.INDICES);
        assertSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsAction.SortBy.START_TIME), GetSnapshotsAction.SortBy.START_TIME
        );
    }

    public void testResponseSizeLimit() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        final List<String> names = AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(6, 20));
        for (GetSnapshotsAction.SortBy sort : GetSnapshotsAction.SortBy.values()) {
            logger.info("--> testing pagination for [{}]", sort);
            doTestResponseSizeLimit(sort, repoName, names);
        }
    }

    private void doTestResponseSizeLimit(GetSnapshotsAction.SortBy sort, String repoName, List<String> snapshotNames) throws IOException {
        final List<SnapshotInfo> allSnapshotsSorted = allSnapshotsSorted(snapshotNames, repoName, sort);
        final List<SnapshotInfo> batch1 = sortedWithSize(repoName, sort, 2);
        assertEquals(batch1, allSnapshotsSorted.subList(0, 2));
        final List<SnapshotInfo> batch2 = sortedWithSize(repoName, sort, batch1.get(1), 2);
        assertEquals(batch2, allSnapshotsSorted.subList(2, 4));
        final int lastBatch = snapshotNames.size() - batch1.size() - batch2.size();
        final List<SnapshotInfo> batch3 = sortedWithSize(repoName, sort, batch2.get(1), lastBatch);
        assertEquals(batch3, allSnapshotsSorted.subList(batch1.size() + batch2.size(), snapshotNames.size()));
    }

    private List<SnapshotInfo> allSnapshotsSorted(Collection<String> allSnapshotNames,
                                                  String repoName,
                                                  GetSnapshotsAction.SortBy sortBy) throws IOException {
        final Response response =
                getRestClient().performRequest(new Request(HttpGet.METHOD_NAME, "/_snapshot/" + repoName + "/*?sort=" + sortBy));
        final List<SnapshotInfo> snapshotInfos;
        try (InputStream input = response.getEntity().getContent();
             XContentParser parser = JsonXContent.jsonXContent.createParser(
                     NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)) {
            snapshotInfos = GetSnapshotsResponse.fromXContent(parser).getSnapshots(repoName);
        }
        assertEquals(snapshotInfos.size(), allSnapshotNames.size());
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            assertThat(snapshotInfo.snapshotId().getName(), is(in(allSnapshotNames)));
        }
        return snapshotInfos;
    }

    private List<SnapshotInfo> sortedWithSize(String repoName, GetSnapshotsAction.SortBy sortBy, int size) throws IOException {
        final Response response = getRestClient().performRequest(new Request(HttpGet.METHOD_NAME, "/_snapshot/" + repoName
                + "/*?sort=" + sortBy + "&size=" + size));
        final List<SnapshotInfo> snapshotInfos;
        try (InputStream input = response.getEntity().getContent();
             XContentParser parser = JsonXContent.jsonXContent.createParser(
                     NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)) {
            snapshotInfos = GetSnapshotsResponse.fromXContent(parser).getSnapshots(repoName);
        }
        assertThat(snapshotInfos, hasSize(size));
        return snapshotInfos;
    }

    private List<SnapshotInfo> sortedWithSize(String repoName, GetSnapshotsAction.SortBy sortBy, SnapshotInfo after,
                                              int size) throws IOException {
        final Response response = getRestClient().performRequest(
            new Request(
                    HttpGet.METHOD_NAME,
                    "/_snapshot/" + repoName
                        + "/*?sort=" + sortBy
                        + "&size=" + size
                        + "&after=" + GetSnapshotsRequest.After.from(after, sortBy).value() + "," + after.snapshotId().getName()
            )
        );
        final List<SnapshotInfo> snapshotInfos;
        try (InputStream input = response.getEntity().getContent();
             XContentParser parser = JsonXContent.jsonXContent.createParser(
                     NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)) {
            snapshotInfos = GetSnapshotsResponse.fromXContent(parser).getSnapshots(repoName);
        }
        assertThat(snapshotInfos, hasSize(size));
        return snapshotInfos;
    }

    protected void createIndexWithContent(String indexName) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, AbstractSnapshotIntegTestCase.SINGLE_SHARD_NO_REPLICA);
        ensureGreen(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");
    }
}
