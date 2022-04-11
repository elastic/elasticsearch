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
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.assertSnapshotListSorted;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.matchAllPattern;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

// TODO: dry up duplication across this suite and org.elasticsearch.snapshots.GetSnapshotsIT more
public class RestGetSnapshotsIT extends AbstractSnapshotRestTestCase {

    /**
     * Large snapshot pool settings to set up nodes for tests involving multiple repositories that need to have enough
     * threads so that blocking some threads on one repository doesn't block other repositories from doing work
     */
    private static final Settings LARGE_SNAPSHOT_POOL_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot.core", 3)
        .put("thread_pool.snapshot.max", 3)
        .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LARGE_SNAPSHOT_POOL_SETTINGS)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .build();
    }

    public void testSortOrder() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        final List<String> snapshotNamesWithoutIndex = AbstractSnapshotIntegTestCase.createNSnapshots(
            logger,
            repoName,
            randomIntBetween(3, 20)
        );

        createIndexWithContent("test-index");

        final List<String> snapshotNamesWithIndex = AbstractSnapshotIntegTestCase.createNSnapshots(
            logger,
            repoName,
            randomIntBetween(3, 20)
        );

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
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.SHARDS, order),
            GetSnapshotsRequest.SortBy.SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.FAILED_SHARDS, order),
            GetSnapshotsRequest.SortBy.FAILED_SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, GetSnapshotsRequest.SortBy.REPOSITORY, order),
            GetSnapshotsRequest.SortBy.REPOSITORY,
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

    private void doTestPagination(String repoName, List<String> names, GetSnapshotsRequest.SortBy sort, SortOrder order)
        throws IOException {
        final List<SnapshotInfo> allSnapshotsSorted = allSnapshotsSorted(names, repoName, sort, order);
        final GetSnapshotsResponse batch1 = sortedWithLimit(repoName, sort, null, 2, order);
        assertEquals(allSnapshotsSorted.subList(0, 2), batch1.getSnapshots());
        final GetSnapshotsResponse batch2 = sortedWithLimit(repoName, sort, batch1.next(), 2, order);
        assertEquals(allSnapshotsSorted.subList(2, 4), batch2.getSnapshots());
        final int lastBatch = names.size() - batch1.getSnapshots().size() - batch2.getSnapshots().size();
        final GetSnapshotsResponse batch3 = sortedWithLimit(repoName, sort, batch2.next(), lastBatch, order);
        assertEquals(
            batch3.getSnapshots(),
            allSnapshotsSorted.subList(batch1.getSnapshots().size() + batch2.getSnapshots().size(), names.size())
        );
        final GetSnapshotsResponse batch3NoLimit = sortedWithLimit(repoName, sort, batch2.next(), GetSnapshotsRequest.NO_LIMIT, order);
        assertNull(batch3NoLimit.next());
        assertEquals(batch3.getSnapshots(), batch3NoLimit.getSnapshots());
        final GetSnapshotsResponse batch3LargeLimit = sortedWithLimit(
            repoName,
            sort,
            batch2.next(),
            lastBatch + randomIntBetween(1, 100),
            order
        );
        assertEquals(batch3.getSnapshots(), batch3LargeLimit.getSnapshots());
        assertNull(batch3LargeLimit.next());
    }

    public void testSortAndPaginateWithInProgress() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "mock");
        final Collection<String> allSnapshotNames = new HashSet<>(
            AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(3, 20))
        );
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
        AbstractSnapshotIntegTestCase.awaitClusterState(logger, state -> {
            boolean firstIndexSuccessfullySnapshot = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .asStream()
                .flatMap(s -> s.shards().entrySet().stream())
                .allMatch(
                    e -> e.getKey().getIndexName().equals("test-index-1") == false
                        || e.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS
                );
            boolean secondIndexIsBlocked = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .asStream()
                .flatMap(s -> s.shards().entrySet().stream())
                .filter(e -> e.getKey().getIndexName().equals("test-index-2"))
                .map(e -> e.getValue().state())
                .collect(Collectors.groupingBy(e -> e, Collectors.counting()))
                .equals(Map.of(SnapshotsInProgress.ShardState.INIT, 1L, SnapshotsInProgress.ShardState.QUEUED, (long) inProgressCount - 1));
            return firstIndexSuccessfullySnapshot && secondIndexIsBlocked;
        });
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

    public void testFilterBySLMPolicy() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(1, 5));
        final List<SnapshotInfo> snapshotsWithoutPolicy = clusterAdmin().prepareGetSnapshots("*")
            .setSnapshots("*")
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .get()
            .getSnapshots();
        final String snapshotWithPolicy = "snapshot-with-policy";
        final String policyName = "some-policy";
        final SnapshotInfo withPolicy = AbstractSnapshotIntegTestCase.assertSuccessful(
            logger,
            clusterAdmin().prepareCreateSnapshot(repoName, snapshotWithPolicy)
                .setUserMetadata(Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, policyName))
                .setWaitForCompletion(true)
                .execute()
        );

        assertThat(getAllSnapshotsForPolicies(policyName), is(List.of(withPolicy)));
        assertThat(getAllSnapshotsForPolicies("some-*"), is(List.of(withPolicy)));
        assertThat(getAllSnapshotsForPolicies("*", "-" + policyName), empty());
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN), is(snapshotsWithoutPolicy));
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN, "-" + policyName), is(snapshotsWithoutPolicy));
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN), is(snapshotsWithoutPolicy));
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN, "-*"), is(snapshotsWithoutPolicy));
        assertThat(getAllSnapshotsForPolicies("no-such-policy"), empty());
        assertThat(getAllSnapshotsForPolicies("no-such-policy*"), empty());

        final String snapshotWithOtherPolicy = "snapshot-with-other-policy";
        final String otherPolicyName = "other-policy";
        final SnapshotInfo withOtherPolicy = AbstractSnapshotIntegTestCase.assertSuccessful(
            logger,
            clusterAdmin().prepareCreateSnapshot(repoName, snapshotWithOtherPolicy)
                .setUserMetadata(Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, otherPolicyName))
                .setWaitForCompletion(true)
                .execute()
        );
        assertThat(getAllSnapshotsForPolicies("*"), is(List.of(withOtherPolicy, withPolicy)));
        assertThat(getAllSnapshotsForPolicies(policyName, otherPolicyName), is(List.of(withOtherPolicy, withPolicy)));
        assertThat(getAllSnapshotsForPolicies(policyName, otherPolicyName, "no-such-policy*"), is(List.of(withOtherPolicy, withPolicy)));
        final List<SnapshotInfo> allSnapshots = clusterAdmin().prepareGetSnapshots("*")
            .setSnapshots("*")
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .get()
            .getSnapshots();
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN, policyName, otherPolicyName), is(allSnapshots));
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN, "*"), is(allSnapshots));
    }

    public void testSortAfterStartTime() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        final HashSet<Long> startTimes = new HashSet<>();
        final SnapshotInfo snapshot1 = createFullSnapshotWithUniqueStartTime(repoName, "snapshot-1", startTimes);
        final SnapshotInfo snapshot2 = createFullSnapshotWithUniqueStartTime(repoName, "snapshot-2", startTimes);
        final SnapshotInfo snapshot3 = createFullSnapshotWithUniqueStartTime(repoName, "snapshot-3", startTimes);

        final List<SnapshotInfo> allSnapshotInfo = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.START_TIME)
            .get()
            .getSnapshots();
        assertThat(allSnapshotInfo, is(List.of(snapshot1, snapshot2, snapshot3)));

        final long startTime1 = snapshot1.startTime();
        final long startTime2 = snapshot2.startTime();
        final long startTime3 = snapshot3.startTime();

        assertThat(allAfterStartTimeAscending(startTime1 - 1), is(allSnapshotInfo));
        assertThat(allAfterStartTimeAscending(startTime1), is(allSnapshotInfo));
        assertThat(allAfterStartTimeAscending(startTime2), is(List.of(snapshot2, snapshot3)));
        assertThat(allAfterStartTimeAscending(startTime3), is(List.of(snapshot3)));
        assertThat(allAfterStartTimeAscending(startTime3 + 1), empty());

        final List<SnapshotInfo> allSnapshotInfoDesc = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.START_TIME)
            .setOrder(SortOrder.DESC)
            .get()
            .getSnapshots();
        assertThat(allSnapshotInfoDesc, is(List.of(snapshot3, snapshot2, snapshot1)));

        assertThat(allBeforeStartTimeDescending(startTime3 + 1), is(allSnapshotInfoDesc));
        assertThat(allBeforeStartTimeDescending(startTime3), is(allSnapshotInfoDesc));
        assertThat(allBeforeStartTimeDescending(startTime2), is(List.of(snapshot2, snapshot1)));
        assertThat(allBeforeStartTimeDescending(startTime1), is(List.of(snapshot1)));
        assertThat(allBeforeStartTimeDescending(startTime1 - 1), empty());
    }

    // create a snapshot that is guaranteed to have a unique start time
    private SnapshotInfo createFullSnapshotWithUniqueStartTime(String repoName, String snapshotName, Set<Long> forbiddenStartTimes) {
        while (true) {
            final SnapshotInfo snapshotInfo = AbstractSnapshotIntegTestCase.createFullSnapshot(logger, repoName, snapshotName);
            if (forbiddenStartTimes.contains(snapshotInfo.startTime())) {
                logger.info("--> snapshot start time collided");
                assertAcked(clusterAdmin().prepareDeleteSnapshot(repoName, snapshotName).get());
            } else {
                assertTrue(forbiddenStartTimes.add(snapshotInfo.startTime()));
                return snapshotInfo;
            }
        }
    }

    private List<SnapshotInfo> allAfterStartTimeAscending(long timestamp) throws IOException {
        final Request request = baseGetSnapshotsRequest("*");
        request.addParameter("sort", GetSnapshotsRequest.SortBy.START_TIME.toString());
        request.addParameter("from_sort_value", String.valueOf(timestamp));
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response).getSnapshots();
    }

    private List<SnapshotInfo> allBeforeStartTimeDescending(long timestamp) throws IOException {
        final Request request = baseGetSnapshotsRequest("*");
        request.addParameter("sort", GetSnapshotsRequest.SortBy.START_TIME.toString());
        request.addParameter("from_sort_value", String.valueOf(timestamp));
        request.addParameter("order", SortOrder.DESC.toString());
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response).getSnapshots();
    }

    private static List<SnapshotInfo> getAllSnapshotsForPolicies(String... policies) throws IOException {
        final Request requestWithPolicy = new Request(HttpGet.METHOD_NAME, "/_snapshot/*/*");
        requestWithPolicy.addParameter("slm_policy_filter", Strings.arrayToCommaDelimitedString(policies));
        requestWithPolicy.addParameter("sort", GetSnapshotsRequest.SortBy.NAME.toString());
        return readSnapshotInfos(getRestClient().performRequest(requestWithPolicy)).getSnapshots();
    }

    private void createIndexWithContent(String indexName) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, AbstractSnapshotIntegTestCase.SINGLE_SHARD_NO_REPLICA);
        ensureGreen(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");
    }

    private static void assertStablePagination(String repoName, Collection<String> allSnapshotNames, GetSnapshotsRequest.SortBy sort)
        throws IOException {
        final SortOrder order = randomFrom(SortOrder.values());
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(allSnapshotNames, repoName, sort, order);

        for (int i = 1; i <= allSnapshotNames.size(); i++) {
            final List<SnapshotInfo> subsetSorted = sortedWithLimit(repoName, sort, null, i, order).getSnapshots();
            assertEquals(subsetSorted, allSorted.subList(0, i));
        }

        for (int j = 0; j < allSnapshotNames.size(); j++) {
            final SnapshotInfo after = allSorted.get(j);
            for (int i = 1; i < allSnapshotNames.size() - j; i++) {
                final GetSnapshotsResponse getSnapshotsResponse = sortedWithLimit(
                    repoName,
                    sort,
                    GetSnapshotsRequest.After.from(after, sort).asQueryParam(),
                    i,
                    order
                );
                final GetSnapshotsResponse getSnapshotsResponseNumeric = sortedWithLimit(repoName, sort, j + 1, i, order);
                final List<SnapshotInfo> subsetSorted = getSnapshotsResponse.getSnapshots();
                assertEquals(subsetSorted, getSnapshotsResponseNumeric.getSnapshots());
                assertEquals(subsetSorted, allSorted.subList(j + 1, j + i + 1));
                assertEquals(allSnapshotNames.size(), getSnapshotsResponse.totalCount());
                assertEquals(allSnapshotNames.size() - (j + i + 1), getSnapshotsResponse.remaining());
                assertEquals(getSnapshotsResponseNumeric.totalCount(), getSnapshotsResponse.totalCount());
                assertEquals(getSnapshotsResponseNumeric.remaining(), getSnapshotsResponse.remaining());
            }
        }
    }

    private static List<SnapshotInfo> allSnapshotsSorted(
        Collection<String> allSnapshotNames,
        String repoName,
        GetSnapshotsRequest.SortBy sortBy,
        SortOrder order
    ) throws IOException {
        final Request request = baseGetSnapshotsRequest(repoName);
        request.addParameter("sort", sortBy.toString());
        if (order == SortOrder.DESC || randomBoolean()) {
            request.addParameter("order", order.toString());
        }
        final GetSnapshotsResponse getSnapshotsResponse = readSnapshotInfos(getRestClient().performRequest(request));
        final List<SnapshotInfo> snapshotInfos = getSnapshotsResponse.getSnapshots();
        assertEquals(snapshotInfos.size(), allSnapshotNames.size());
        assertEquals(getSnapshotsResponse.totalCount(), allSnapshotNames.size());
        assertEquals(0, getSnapshotsResponse.remaining());
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            assertThat(snapshotInfo.snapshotId().getName(), is(in(allSnapshotNames)));
        }
        return snapshotInfos;
    }

    private static Request baseGetSnapshotsRequest(String repoName) {
        return new Request(HttpGet.METHOD_NAME, "/_snapshot/" + repoName + "/*");
    }

    private static GetSnapshotsResponse readSnapshotInfos(Response response) throws IOException {
        try (
            InputStream input = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                input
            )
        ) {
            return GetSnapshotsResponse.fromXContent(parser);
        }
    }

    private static GetSnapshotsResponse sortedWithLimit(
        String repoName,
        GetSnapshotsRequest.SortBy sortBy,
        String after,
        int size,
        SortOrder order
    ) throws IOException {
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

    private static GetSnapshotsResponse sortedWithLimit(
        String repoName,
        GetSnapshotsRequest.SortBy sortBy,
        int offset,
        int size,
        SortOrder order
    ) throws IOException {
        final Request request = baseGetSnapshotsRequest(repoName);
        request.addParameter("sort", sortBy.toString());
        if (size != GetSnapshotsRequest.NO_LIMIT || randomBoolean()) {
            request.addParameter("size", String.valueOf(size));
        }
        request.addParameter("offset", String.valueOf(offset));
        if (order == SortOrder.DESC || randomBoolean()) {
            request.addParameter("order", order.toString());
        }
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response);
    }
}
