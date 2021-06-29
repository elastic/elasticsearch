/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.snapshots;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.assertSnapshotListSorted;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

// TODO: dry up duplication across this suite and org.elasticsearch.snapshots.GetSnapshotsIT more
public class RestGetSnapshotsIT extends AbstractSnapshotRestTestCase {

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
        doTestSortOrder(repoName, allSnapshotNames, SortOrder.ASC);
        doTestSortOrder(repoName, allSnapshotNames, SortOrder.DESC);
    }

    private void doTestSortOrder(String repoName, Collection<String> allSnapshotNames, SortOrder order) throws IOException {
        final List<SnapshotInfo> defaultSorting = clusterAdmin().prepareGetSnapshots(repoName).setOrder(order).get().getSnapshots();
        assertSnapshotListSorted(defaultSorting, null, order);
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.NAME, order),
                GetSnapshotsRequest.SortBy.NAME,
                order
        );
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.DURATION, order),
                GetSnapshotsRequest.SortBy.DURATION,
                order
        );
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.INDICES, order),
                GetSnapshotsRequest.SortBy.INDICES,
                order
        );
        assertSnapshotListSorted(
                allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.START_TIME, order),
                GetSnapshotsRequest.SortBy.START_TIME,
                order
        );
    }

    public void testResponseSizeLimit() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        final List<String> names = AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(6, 20));
        for (GetSnapshotsRequest.SortBy sort : GetSnapshotsRequest.SortBy.values()) {
            for (SortOrder order : SortOrder.values()) {
                logger.info("--> testing pagination for [{}] [{}]", sort, order);
                doTestPagination(repoName, names, sort, order);
            }
        }
    }

    private void doTestPagination(String repoName,
                                  List<String> names,
                                  GetSnapshotsRequest.SortBy sort,
                                  SortOrder order) throws IOException {
        final List<SnapshotInfo> allSnapshotsSorted = allSnapshotsSorted(names, repoName, sort, order);
        final Tuple<String, List<SnapshotInfo>> batch1 = sortedWithLimit(repoName, sort, null, 2, order);
        assertEquals(allSnapshotsSorted.subList(0, 2), batch1.v2());
        final Tuple<String, List<SnapshotInfo>> batch2 = sortedWithLimit(repoName, sort, batch1.v1(), 2, order);
        assertEquals(allSnapshotsSorted.subList(2, 4), batch2.v2());
        final int lastBatch = names.size() - batch1.v2().size() - batch2.v2().size();
        final Tuple<String, List<SnapshotInfo>> batch3 = sortedWithLimit(repoName, sort, batch2.v1(), lastBatch, order);
        assertEquals(batch3.v2(), allSnapshotsSorted.subList(batch1.v2().size() + batch2.v2().size(), names.size()));
        final Tuple<String, List<SnapshotInfo>> batch3NoLimit = sortedWithLimit(
                repoName,
                sort,
                batch2.v1(),
                GetSnapshotsRequest.NO_LIMIT,
                order
        );
        assertNull(batch3NoLimit.v1());
        assertEquals(batch3.v2(), batch3NoLimit.v2());
        final Tuple<String, List<SnapshotInfo>> batch3LargeLimit = sortedWithLimit(
                repoName,
                sort,
                batch2.v1(),
                lastBatch + randomIntBetween(1, 100),
                order
        );
        assertEquals(batch3.v2(), batch3LargeLimit.v2());
        assertNull(batch3LargeLimit.v1());
    }

    public void testSortAndPaginateWithInProgress() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "mock");
        final Collection<String> allSnapshotNames =
                new HashSet<>(AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(3, 20)));
        createIndexWithContent("test-index-1");
        allSnapshotNames.addAll(AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(3, 20)));
        createIndexWithContent("test-index-2");

        final int inProgressCount = randomIntBetween(6, 20);
        final List<ActionFuture<CreateSnapshotResponse>> inProgressSnapshots = new ArrayList<>(inProgressCount);
        AbstractSnapshotIntegTestCase.blockAllDataNodes(repoName);
        for (int i = 0; i < inProgressCount; i++) {
            final String snapshotName = "snap-" + i;
            allSnapshotNames.add(snapshotName);
            inProgressSnapshots.add(AbstractSnapshotIntegTestCase.startFullSnapshot(logger, repoName, snapshotName, false));
        }
        AbstractSnapshotIntegTestCase.awaitNumberOfSnapshotsInProgress(logger, inProgressCount);

        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsRequest.SortBy.START_TIME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsRequest.SortBy.NAME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsRequest.SortBy.INDICES);

        AbstractSnapshotIntegTestCase.unblockAllDataNodes(repoName);
        for (ActionFuture<CreateSnapshotResponse> inProgressSnapshot : inProgressSnapshots) {
            AbstractSnapshotIntegTestCase.assertSuccessful(logger, inProgressSnapshot);
        }

        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsRequest.SortBy.START_TIME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsRequest.SortBy.NAME);
        assertStablePagination(repoName, allSnapshotNames, GetSnapshotsRequest.SortBy.INDICES);
    }

    private void createIndexWithContent(String indexName) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, AbstractSnapshotIntegTestCase.SINGLE_SHARD_NO_REPLICA);
        ensureGreen(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");
    }

    private static void assertStablePagination(String repoName,
                                               Collection<String> allSnapshotNames,
                                               GetSnapshotsRequest.SortBy sort) throws IOException {
        final SortOrder order = randomFrom(SortOrder.values());
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(allSnapshotNames, repoName, sort, order);

        for (int i = 1; i <= allSnapshotNames.size(); i++) {
            final List<SnapshotInfo> subsetSorted = sortedWithLimit(repoName, sort, null, i, order).v2();
            assertEquals(subsetSorted, allSorted.subList(0, i));
        }

        for (int j = 0; j < allSnapshotNames.size(); j++) {
            final SnapshotInfo after = allSorted.get(j);
            for (int i = 1; i < allSnapshotNames.size() - j; i++) {
                final List<SnapshotInfo> subsetSorted = sortedWithLimit(
                        repoName, sort, GetSnapshotsRequest.After.from(after, sort).asQueryParam(), i, order).v2();
                assertEquals(subsetSorted, allSorted.subList(j + 1, j + i + 1));
            }
        }
    }

    private static List<SnapshotInfo> allSnapshotsSorted(Collection<String> allSnapshotNames,
                                                         String repoName,
                                                         GetSnapshotsRequest.SortBy sortBy,
                                                         SortOrder order) throws IOException {
        final Request request = baseGetSnapshotsRequest(repoName);
        request.addParameter("sort", sortBy.toString());
        if (order == SortOrder.DESC || randomBoolean()) {
            request.addParameter("order", order.toString());
        }
        final Response response = getRestClient().performRequest(request);
        final List<SnapshotInfo> snapshotInfos = readSnapshotInfos(response).v2();
        assertEquals(snapshotInfos.size(), allSnapshotNames.size());
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            assertThat(snapshotInfo.snapshotId().getName(), is(in(allSnapshotNames)));
        }
        return snapshotInfos;
    }

    private static Request baseGetSnapshotsRequest(String repoName) {
        return new Request(HttpGet.METHOD_NAME, "/_snapshot/" + repoName + "/*");
    }

    private static Tuple<String, List<SnapshotInfo>> readSnapshotInfos(Response response) throws IOException {
        try (InputStream input = response.getEntity().getContent();
             XContentParser parser = JsonXContent.jsonXContent.createParser(
                     NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)) {
            final GetSnapshotsResponse getSnapshotsResponse = GetSnapshotsResponse.fromXContent(parser);
            return Tuple.tuple(getSnapshotsResponse.next(), getSnapshotsResponse.getSnapshots());
        }
    }

    private static Tuple<String, List<SnapshotInfo>> sortedWithLimit(String repoName,
                                                                     GetSnapshotsRequest.SortBy sortBy,
                                                                     String after,
                                                                     int size,
                                                                     SortOrder order) throws IOException {
        final Request request = baseGetSnapshotsRequest(repoName);
        request.addParameter("sort", sortBy.toString());
        if (size != GetSnapshotsRequest.NO_LIMIT || randomBoolean()) {
            request.addParameter("size", String.valueOf(size));
        }
        if (after != null) {
            request.addParameter("after", after);
        }
        if (order == SortOrder.DESC || randomBoolean()) {
            request.addParameter("order", order.toString());
        }
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response);
    }
}
