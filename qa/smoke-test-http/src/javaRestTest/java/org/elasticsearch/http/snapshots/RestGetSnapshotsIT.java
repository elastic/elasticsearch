/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.snapshots;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.SnapshotSortKey;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoUtils;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
        final boolean includeIndexNames = randomBoolean();
        final List<SnapshotInfo> defaultSorting = clusterAdmin().prepareGetSnapshots(repoName)
            .setOrder(order)
            .setIncludeIndexNames(includeIndexNames)
            .get()
            .getSnapshots();
        assertSnapshotListSorted(defaultSorting, null, order);
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.NAME, order, includeIndexNames),
            SnapshotSortKey.NAME,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.DURATION, order, includeIndexNames),
            SnapshotSortKey.DURATION,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.INDICES, order, includeIndexNames),
            SnapshotSortKey.INDICES,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.START_TIME, order, includeIndexNames),
            SnapshotSortKey.START_TIME,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.SHARDS, order, includeIndexNames),
            SnapshotSortKey.SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.FAILED_SHARDS, order, includeIndexNames),
            SnapshotSortKey.FAILED_SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repoName, SnapshotSortKey.REPOSITORY, order, includeIndexNames),
            SnapshotSortKey.REPOSITORY,
            order
        );
    }

    public void testResponseSizeLimit() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        final List<String> names = AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(6, 20));
        for (SnapshotSortKey sort : SnapshotSortKey.values()) {
            for (SortOrder order : SortOrder.values()) {
                logger.info("--> testing pagination for [{}] [{}]", sort, order);
                doTestPagination(repoName, names, sort, order);
            }
        }
    }

    private void doTestPagination(String repoName, List<String> names, SnapshotSortKey sort, SortOrder order) throws IOException {
        final boolean includeIndexNames = randomBoolean();
        final List<SnapshotInfo> allSnapshotsSorted = allSnapshotsSorted(names, repoName, sort, order, includeIndexNames);
        final GetSnapshotsResponse batch1 = sortedWithLimit(repoName, sort, null, 2, order, includeIndexNames);
        assertEquals(allSnapshotsSorted.subList(0, 2), batch1.getSnapshots());
        final GetSnapshotsResponse batch2 = sortedWithLimit(repoName, sort, batch1.next(), 2, order, includeIndexNames);
        assertEquals(allSnapshotsSorted.subList(2, 4), batch2.getSnapshots());
        final int lastBatch = names.size() - batch1.getSnapshots().size() - batch2.getSnapshots().size();
        final GetSnapshotsResponse batch3 = sortedWithLimit(repoName, sort, batch2.next(), lastBatch, order, includeIndexNames);
        assertEquals(
            batch3.getSnapshots(),
            allSnapshotsSorted.subList(batch1.getSnapshots().size() + batch2.getSnapshots().size(), names.size())
        );
        final GetSnapshotsResponse batch3NoLimit = sortedWithLimit(
            repoName,
            sort,
            batch2.next(),
            GetSnapshotsRequest.NO_LIMIT,
            order,
            includeIndexNames
        );
        assertNull(batch3NoLimit.next());
        assertEquals(batch3.getSnapshots(), batch3NoLimit.getSnapshots());
        final GetSnapshotsResponse batch3LargeLimit = sortedWithLimit(
            repoName,
            sort,
            batch2.next(),
            lastBatch + randomIntBetween(1, 100),
            order,
            includeIndexNames
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
            final var snapshotsInProgress = SnapshotsInProgress.get(state);
            boolean firstIndexSuccessfullySnapshot = snapshotsInProgress.asStream()
                .flatMap(s -> s.shards().entrySet().stream())
                .allMatch(
                    e -> e.getKey().getIndexName().equals("test-index-1") == false
                        || e.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS
                );
            boolean secondIndexIsBlocked = snapshotsInProgress.asStream()
                .flatMap(s -> s.shards().entrySet().stream())
                .filter(e -> e.getKey().getIndexName().equals("test-index-2"))
                .map(e -> e.getValue().state())
                .collect(Collectors.groupingBy(e -> e, Collectors.counting()))
                .equals(Map.of(SnapshotsInProgress.ShardState.INIT, 1L, SnapshotsInProgress.ShardState.QUEUED, (long) inProgressCount - 1));
            return firstIndexSuccessfullySnapshot && secondIndexIsBlocked;
        });
        assertStablePagination(repoName, allSnapshotNames, SnapshotSortKey.START_TIME);
        assertStablePagination(repoName, allSnapshotNames, SnapshotSortKey.NAME);
        assertStablePagination(repoName, allSnapshotNames, SnapshotSortKey.INDICES);

        AbstractSnapshotIntegTestCase.unblockAllDataNodes(repoName);
        for (ActionFuture<CreateSnapshotResponse> inProgressSnapshot : inProgressSnapshots) {
            AbstractSnapshotIntegTestCase.assertSuccessful(logger, inProgressSnapshot);
        }

        assertStablePagination(repoName, allSnapshotNames, SnapshotSortKey.START_TIME);
        assertStablePagination(repoName, allSnapshotNames, SnapshotSortKey.NAME);
        assertStablePagination(repoName, allSnapshotNames, SnapshotSortKey.INDICES);
    }

    public void testFilterBySLMPolicy() throws Exception {
        final String repoName = "test-repo";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName, "fs");
        AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName, randomIntBetween(1, 5));
        final List<SnapshotInfo> snapshotsWithoutPolicy = clusterAdmin().prepareGetSnapshots("*")
            .setSnapshots("*")
            .setSort(SnapshotSortKey.NAME)
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
            .setSort(SnapshotSortKey.NAME)
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
            .setSort(SnapshotSortKey.START_TIME)
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
            .setSort(SnapshotSortKey.START_TIME)
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
        request.addParameter("sort", SnapshotSortKey.START_TIME.toString());
        request.addParameter("from_sort_value", String.valueOf(timestamp));
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response).getSnapshots();
    }

    private List<SnapshotInfo> allBeforeStartTimeDescending(long timestamp) throws IOException {
        final Request request = baseGetSnapshotsRequest("*");
        request.addParameter("sort", SnapshotSortKey.START_TIME.toString());
        request.addParameter("from_sort_value", String.valueOf(timestamp));
        request.addParameter("order", SortOrder.DESC.toString());
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response).getSnapshots();
    }

    private static List<SnapshotInfo> getAllSnapshotsForPolicies(String... policies) throws IOException {
        final Request requestWithPolicy = new Request(HttpGet.METHOD_NAME, "/_snapshot/*/*");
        requestWithPolicy.addParameter("slm_policy_filter", Strings.arrayToCommaDelimitedString(policies));
        requestWithPolicy.addParameter("sort", SnapshotSortKey.NAME.toString());
        return readSnapshotInfos(getRestClient().performRequest(requestWithPolicy)).getSnapshots();
    }

    private void createIndexWithContent(String indexName) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");
    }

    private static void assertStablePagination(String repoName, Collection<String> allSnapshotNames, SnapshotSortKey sort)
        throws IOException {
        final SortOrder order = randomFrom(SortOrder.values());
        final boolean includeIndexNames = sort == SnapshotSortKey.INDICES || randomBoolean();
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(allSnapshotNames, repoName, sort, order, includeIndexNames);

        for (int i = 1; i <= allSnapshotNames.size(); i++) {
            final List<SnapshotInfo> subsetSorted = sortedWithLimit(repoName, sort, null, i, order, includeIndexNames).getSnapshots();
            assertEquals(subsetSorted, allSorted.subList(0, i));
        }

        for (int j = 0; j < allSnapshotNames.size(); j++) {
            final SnapshotInfo after = allSorted.get(j);
            for (int i = 1; i < allSnapshotNames.size() - j; i++) {
                final GetSnapshotsResponse getSnapshotsResponse = sortedWithLimit(
                    repoName,
                    sort,
                    sort.encodeAfterQueryParam(after),
                    i,
                    order,
                    includeIndexNames
                );
                final GetSnapshotsResponse getSnapshotsResponseNumeric = sortedWithLimit(
                    repoName,
                    sort,
                    j + 1,
                    i,
                    order,
                    includeIndexNames
                );
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
        SnapshotSortKey sortBy,
        SortOrder order,
        boolean includeIndices
    ) throws IOException {
        final Request request = baseGetSnapshotsRequest(repoName);
        request.addParameter("sort", sortBy.toString());
        if (order == SortOrder.DESC || randomBoolean()) {
            request.addParameter("order", order.toString());
        }
        addIndexNamesParameter(includeIndices, request);
        final GetSnapshotsResponse getSnapshotsResponse = readSnapshotInfos(getRestClient().performRequest(request));
        final List<SnapshotInfo> snapshotInfos = getSnapshotsResponse.getSnapshots();
        if (includeIndices == false) {
            for (SnapshotInfo snapshotInfo : snapshotInfos) {
                assertThat(snapshotInfo.indices(), empty());
            }
        }
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
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, input)
        ) {
            return GET_SNAPSHOT_PARSER.parse(parser, null);
        }
    }

    private static GetSnapshotsResponse sortedWithLimit(
        String repoName,
        SnapshotSortKey sortBy,
        String after,
        int size,
        SortOrder order,
        boolean includeIndices
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
        addIndexNamesParameter(includeIndices, request);
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response);
    }

    private static void addIndexNamesParameter(boolean includeIndices, Request request) {
        if (includeIndices == false) {
            request.addParameter(SnapshotInfo.INDEX_NAMES_XCONTENT_PARAM, "false");
        } else if (randomBoolean()) {
            request.addParameter(SnapshotInfo.INDEX_NAMES_XCONTENT_PARAM, "true");
        }
    }

    private static GetSnapshotsResponse sortedWithLimit(
        String repoName,
        SnapshotSortKey sortBy,
        int offset,
        int size,
        SortOrder order,
        boolean includeIndices
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
        addIndexNamesParameter(includeIndices, request);
        final Response response = getRestClient().performRequest(request);
        return readSnapshotInfos(response);
    }

    private static final int UNKNOWN_COUNT = -1;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetSnapshotsResponse, Void> GET_SNAPSHOT_PARSER = new ConstructingObjectParser<>(
        GetSnapshotsResponse.class.getName(),
        true,
        (args) -> new GetSnapshotsResponse(
            (List<SnapshotInfo>) args[0],
            (Map<String, ElasticsearchException>) args[1],
            (String) args[2],
            args[3] == null ? UNKNOWN_COUNT : (int) args[3],
            args[4] == null ? UNKNOWN_COUNT : (int) args[4]
        )
    );

    static {
        GET_SNAPSHOT_PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SnapshotInfoUtils.snapshotInfoFromXContent(p),
            new ParseField("snapshots")
        );
        GET_SNAPSHOT_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> p.map(HashMap::new, ElasticsearchException::fromXContent),
            new ParseField("failures")
        );
        GET_SNAPSHOT_PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField("next"));
        GET_SNAPSHOT_PARSER.declareIntOrNull(ConstructingObjectParser.optionalConstructorArg(), UNKNOWN_COUNT, new ParseField("total"));
        GET_SNAPSHOT_PARSER.declareIntOrNull(ConstructingObjectParser.optionalConstructorArg(), UNKNOWN_COUNT, new ParseField("remaining"));
    }
}
