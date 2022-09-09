/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class GetSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LARGE_SNAPSHOT_POOL_SETTINGS)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .build();
    }

    public void testSortBy() throws Exception {
        final String repoNameA = "test-repo-a";
        final Path repoPath = randomRepoPath();
        createRepository(repoNameA, "fs", repoPath);
        maybeInitWithOldSnapshotVersion(repoNameA, repoPath);
        final String repoNameB = "test-repo-b";
        createRepository(repoNameB, "fs");

        final List<String> snapshotNamesWithoutIndexA = createNSnapshots(repoNameA, randomIntBetween(3, 20));
        final List<String> snapshotNamesWithoutIndexB = createNSnapshots(repoNameB, randomIntBetween(3, 20));

        createIndexWithContent("test-index");

        final List<String> snapshotNamesWithIndexA = createNSnapshots(repoNameA, randomIntBetween(3, 20));
        final List<String> snapshotNamesWithIndexB = createNSnapshots(repoNameB, randomIntBetween(3, 20));

        final Collection<String> allSnapshotNamesA = new HashSet<>(snapshotNamesWithIndexA);
        final Collection<String> allSnapshotNamesB = new HashSet<>(snapshotNamesWithIndexB);
        allSnapshotNamesA.addAll(snapshotNamesWithoutIndexA);
        allSnapshotNamesB.addAll(snapshotNamesWithoutIndexB);

        doTestSortOrder(repoNameA, allSnapshotNamesA, SortOrder.ASC);
        doTestSortOrder(repoNameA, allSnapshotNamesA, SortOrder.DESC);

        doTestSortOrder(repoNameB, allSnapshotNamesB, SortOrder.ASC);
        doTestSortOrder(repoNameB, allSnapshotNamesB, SortOrder.DESC);

        final Collection<String> allSnapshots = new HashSet<>(allSnapshotNamesA);
        allSnapshots.addAll(allSnapshotNamesB);
        doTestSortOrder("*", allSnapshots, SortOrder.ASC);
        doTestSortOrder("*", allSnapshots, SortOrder.DESC);
    }

    private void doTestSortOrder(String repoName, Collection<String> allSnapshotNames, SortOrder order) {
        final List<SnapshotInfo> defaultSorting = clusterAdmin().prepareGetSnapshots(repoName).setOrder(order).get().getSnapshots();
        assertSnapshotListSorted(defaultSorting, null, order);
        final String[] repos = { repoName };
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.NAME, order),
            GetSnapshotsRequest.SortBy.NAME,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.DURATION, order),
            GetSnapshotsRequest.SortBy.DURATION,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.INDICES, order),
            GetSnapshotsRequest.SortBy.INDICES,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.START_TIME, order),
            GetSnapshotsRequest.SortBy.START_TIME,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.SHARDS, order),
            GetSnapshotsRequest.SortBy.SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.FAILED_SHARDS, order),
            GetSnapshotsRequest.SortBy.FAILED_SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, GetSnapshotsRequest.SortBy.REPOSITORY, order),
            GetSnapshotsRequest.SortBy.REPOSITORY,
            order
        );
    }

    public void testResponseSizeLimit() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        maybeInitWithOldSnapshotVersion(repoName, repoPath);
        final List<String> names = createNSnapshots(repoName, randomIntBetween(6, 20));
        for (GetSnapshotsRequest.SortBy sort : GetSnapshotsRequest.SortBy.values()) {
            for (SortOrder order : SortOrder.values()) {
                logger.info("--> testing pagination for [{}] [{}]", sort, order);
                doTestPagination(repoName, names, sort, order);
            }
        }
    }

    private void doTestPagination(String repoName, List<String> names, GetSnapshotsRequest.SortBy sort, SortOrder order) {
        final String[] repos = { repoName };
        final List<SnapshotInfo> allSnapshotsSorted = allSnapshotsSorted(names, repos, sort, order);
        final GetSnapshotsResponse batch1 = sortedWithLimit(repos, sort, null, 2, order);
        assertEquals(allSnapshotsSorted.subList(0, 2), batch1.getSnapshots());
        final GetSnapshotsResponse batch2 = sortedWithLimit(repos, sort, batch1.next(), 2, order);
        assertEquals(allSnapshotsSorted.subList(2, 4), batch2.getSnapshots());
        final int lastBatch = names.size() - batch1.getSnapshots().size() - batch2.getSnapshots().size();
        final GetSnapshotsResponse batch3 = sortedWithLimit(repos, sort, batch2.next(), lastBatch, order);
        assertEquals(
            batch3.getSnapshots(),
            allSnapshotsSorted.subList(batch1.getSnapshots().size() + batch2.getSnapshots().size(), names.size())
        );
        final GetSnapshotsResponse batch3NoLimit = sortedWithLimit(repos, sort, batch2.next(), GetSnapshotsRequest.NO_LIMIT, order);
        assertNull(batch3NoLimit.next());
        assertEquals(batch3.getSnapshots(), batch3NoLimit.getSnapshots());
        final GetSnapshotsResponse batch3LargeLimit = sortedWithLimit(
            repos,
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
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        maybeInitWithOldSnapshotVersion(repoName, repoPath);
        final Collection<String> allSnapshotNames = new HashSet<>(createNSnapshots(repoName, randomIntBetween(3, 20)));
        createIndexWithContent("test-index-1");
        allSnapshotNames.addAll(createNSnapshots(repoName, randomIntBetween(3, 20)));
        createIndexWithContent("test-index-2");

        final int inProgressCount = randomIntBetween(6, 20);
        final List<ActionFuture<CreateSnapshotResponse>> inProgressSnapshots = new ArrayList<>(inProgressCount);
        blockAllDataNodes(repoName);
        for (int i = 0; i < inProgressCount; i++) {
            final String snapshotName = "snap-" + i;
            allSnapshotNames.add(snapshotName);
            inProgressSnapshots.add(startFullSnapshot(repoName, snapshotName));
        }
        awaitNumberOfSnapshotsInProgress(inProgressCount);
        awaitClusterState(
            state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .asStream()
                .flatMap(s -> s.shards().entrySet().stream())
                .allMatch(
                    e -> e.getKey().getIndexName().equals("test-index-1") == false
                        || e.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS
                )
        );
        final String[] repos = { repoName };
        assertStablePagination(repos, allSnapshotNames, GetSnapshotsRequest.SortBy.START_TIME);
        assertStablePagination(repos, allSnapshotNames, GetSnapshotsRequest.SortBy.NAME);
        assertStablePagination(repos, allSnapshotNames, GetSnapshotsRequest.SortBy.INDICES);
        final List<SnapshotInfo> currentSnapshots = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT)
            .get()
            .getSnapshots();
        for (SnapshotInfo currentSnapshot : currentSnapshots) {
            assertThat(currentSnapshot.toString(), currentSnapshot.failedShards(), is(0));
        }

        assertThat(
            clusterAdmin().prepareGetSnapshots(matchAllPattern())
                .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT, "-snap*")
                .get()
                .getSnapshots(),
            empty()
        );

        unblockAllDataNodes(repoName);
        for (ActionFuture<CreateSnapshotResponse> inProgressSnapshot : inProgressSnapshots) {
            assertSuccessful(inProgressSnapshot);
        }

        assertStablePagination(repos, allSnapshotNames, GetSnapshotsRequest.SortBy.START_TIME);
        assertStablePagination(repos, allSnapshotNames, GetSnapshotsRequest.SortBy.NAME);
        assertStablePagination(repos, allSnapshotNames, GetSnapshotsRequest.SortBy.INDICES);
    }

    public void testPaginationRequiresVerboseListing() throws Exception {
        final String repoName = "tst-repo";
        createRepository(repoName, "fs");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        expectThrows(
            ActionRequestValidationException.class,
            () -> clusterAdmin().prepareGetSnapshots(repoName)
                .setVerbose(false)
                .setSort(GetSnapshotsRequest.SortBy.DURATION)
                .setSize(GetSnapshotsRequest.NO_LIMIT)
                .execute()
                .actionGet()
        );
        expectThrows(
            ActionRequestValidationException.class,
            () -> clusterAdmin().prepareGetSnapshots(repoName)
                .setVerbose(false)
                .setSort(GetSnapshotsRequest.SortBy.START_TIME)
                .setSize(randomIntBetween(1, 100))
                .execute()
                .actionGet()
        );
    }

    public void testExcludePatterns() throws Exception {
        final String repoName1 = "test-repo-1";
        final String repoName2 = "test-repo-2";
        final String otherRepo = "other-repo";
        createRepository(repoName1, "fs");
        createRepository(repoName2, "fs");
        createRepository(otherRepo, "fs");

        final List<String> namesRepo1 = createNSnapshots(repoName1, randomIntBetween(1, 5));
        final List<String> namesRepo2 = createNSnapshots(repoName2, randomIntBetween(1, 5));
        final List<String> namesOtherRepo = createNSnapshots(otherRepo, randomIntBetween(1, 5));

        final Collection<String> allSnapshotNames = new HashSet<>(namesRepo1);
        allSnapshotNames.addAll(namesRepo2);
        final Collection<String> allSnapshotNamesWithoutOther = Set.copyOf(allSnapshotNames);
        allSnapshotNames.addAll(namesOtherRepo);

        final SortOrder order = SortOrder.DESC;
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(
            allSnapshotNames,
            new String[] { "*" },
            GetSnapshotsRequest.SortBy.REPOSITORY,
            order
        );
        final List<SnapshotInfo> allSortedWithoutOther = allSnapshotsSorted(
            allSnapshotNamesWithoutOther,
            new String[] { "*", "-" + otherRepo },
            GetSnapshotsRequest.SortBy.REPOSITORY,
            order
        );
        assertThat(allSortedWithoutOther, is(allSorted.subList(0, allSnapshotNamesWithoutOther.size())));

        final List<SnapshotInfo> allInOther = allSnapshotsSorted(
            namesOtherRepo,
            new String[] { "*", "-test-repo-*" },
            GetSnapshotsRequest.SortBy.REPOSITORY,
            order
        );
        assertThat(allInOther, is(allSorted.subList(allSnapshotNamesWithoutOther.size(), allSorted.size())));

        final String otherPrefixSnapshot1 = "other-prefix-snapshot-1";
        createFullSnapshot(otherRepo, otherPrefixSnapshot1);
        final String otherPrefixSnapshot2 = "other-prefix-snapshot-2";
        createFullSnapshot(otherRepo, otherPrefixSnapshot2);

        final String[] patternOtherRepo = randomBoolean() ? new String[] { otherRepo } : new String[] { "*", "-test-repo-*" };
        final List<SnapshotInfo> allInOtherWithoutOtherPrefix = allSnapshotsSorted(
            namesOtherRepo,
            patternOtherRepo,
            GetSnapshotsRequest.SortBy.REPOSITORY,
            order,
            "-other*"
        );
        assertThat(allInOtherWithoutOtherPrefix, is(allInOther));

        final List<SnapshotInfo> allInOtherWithoutOtherExplicit = allSnapshotsSorted(
            namesOtherRepo,
            patternOtherRepo,
            GetSnapshotsRequest.SortBy.REPOSITORY,
            order,
            "-" + otherPrefixSnapshot1,
            "-" + otherPrefixSnapshot2
        );
        assertThat(allInOtherWithoutOtherExplicit, is(allInOther));

        assertThat(clusterAdmin().prepareGetSnapshots(matchAllPattern()).setSnapshots("other*", "-o*").get().getSnapshots(), empty());
        assertThat(clusterAdmin().prepareGetSnapshots("other*", "-o*").setSnapshots(matchAllPattern()).get().getSnapshots(), empty());
        assertThat(
            clusterAdmin().prepareGetSnapshots("other*", otherRepo, "-o*").setSnapshots(matchAllPattern()).get().getSnapshots(),
            empty()
        );
        assertThat(
            clusterAdmin().prepareGetSnapshots(matchAllPattern())
                .setSnapshots("non-existing*", otherPrefixSnapshot1, "-o*")
                .get()
                .getSnapshots(),
            empty()
        );
    }

    public void testNamesStartingInDash() {
        final String repoName1 = "test-repo";
        final String weirdRepo1 = "-weird-repo-1";
        final String weirdRepo2 = "-weird-repo-2";
        createRepository(repoName1, "fs");
        createRepository(weirdRepo1, "fs");
        createRepository(weirdRepo2, "fs");

        final String snapshotName = "test-snapshot";
        final String weirdSnapshot1 = "-weird-snapshot-1";
        final String weirdSnapshot2 = "-weird-snapshot-2";

        final SnapshotInfo snapshotInRepo1 = createFullSnapshot(repoName1, snapshotName);
        final SnapshotInfo weirdSnapshot1InRepo1 = createFullSnapshot(repoName1, weirdSnapshot1);
        final SnapshotInfo weirdSnapshot2InRepo1 = createFullSnapshot(repoName1, weirdSnapshot2);

        final SnapshotInfo snapshotInWeird1 = createFullSnapshot(weirdRepo1, snapshotName);
        final SnapshotInfo weirdSnapshot1InWeird1 = createFullSnapshot(weirdRepo1, weirdSnapshot1);
        final SnapshotInfo weirdSnapshot2InWeird1 = createFullSnapshot(weirdRepo1, weirdSnapshot2);

        final SnapshotInfo snapshotInWeird2 = createFullSnapshot(weirdRepo2, snapshotName);
        final SnapshotInfo weirdSnapshot1InWeird2 = createFullSnapshot(weirdRepo2, weirdSnapshot1);
        final SnapshotInfo weirdSnapshot2InWeird2 = createFullSnapshot(weirdRepo2, weirdSnapshot2);

        final List<SnapshotInfo> allSnapshots = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.REPOSITORY)
            .get()
            .getSnapshots();
        assertThat(allSnapshots, hasSize(9));

        final List<SnapshotInfo> allSnapshotsByAll = getAllByPatterns(matchAllPattern(), matchAllPattern());
        assertThat(allSnapshotsByAll, is(allSnapshots));
        assertThat(getAllByPatterns(matchAllPattern(), new String[] { snapshotName, weirdSnapshot1, weirdSnapshot2 }), is(allSnapshots));
        assertThat(getAllByPatterns(new String[] { repoName1, weirdRepo1, weirdRepo2 }, matchAllPattern()), is(allSnapshots));

        assertThat(
            getAllByPatterns(matchAllPattern(), new String[] { snapshotName }),
            is(List.of(snapshotInWeird1, snapshotInWeird2, snapshotInRepo1))
        );
        assertThat(
            getAllByPatterns(matchAllPattern(), new String[] { weirdSnapshot1 }),
            is(List.of(weirdSnapshot1InWeird1, weirdSnapshot1InWeird2, weirdSnapshot1InRepo1))
        );
        assertThat(
            getAllByPatterns(matchAllPattern(), new String[] { snapshotName, weirdSnapshot1 }),
            is(
                List.of(
                    weirdSnapshot1InWeird1,
                    snapshotInWeird1,
                    weirdSnapshot1InWeird2,
                    snapshotInWeird2,
                    weirdSnapshot1InRepo1,
                    snapshotInRepo1
                )
            )
        );
        assertThat(getAllByPatterns(matchAllPattern(), new String[] { "non-existing*", weirdSnapshot1 }), empty());
        assertThat(
            getAllByPatterns(matchAllPattern(), new String[] { "*", "--weird-snapshot-1" }),
            is(
                List.of(
                    weirdSnapshot2InWeird1,
                    snapshotInWeird1,
                    weirdSnapshot2InWeird2,
                    snapshotInWeird2,
                    weirdSnapshot2InRepo1,
                    snapshotInRepo1
                )
            )
        );
        assertThat(
            getAllByPatterns(matchAllPattern(), new String[] { "-*" }),
            is(
                List.of(
                    weirdSnapshot1InWeird1,
                    weirdSnapshot2InWeird1,
                    weirdSnapshot1InWeird2,
                    weirdSnapshot2InWeird2,
                    weirdSnapshot1InRepo1,
                    weirdSnapshot2InRepo1

                )
            )
        );
    }

    private List<SnapshotInfo> getAllByPatterns(String[] repos, String[] snapshots) {
        return clusterAdmin().prepareGetSnapshots(repos)
            .setSnapshots(snapshots)
            .setSort(GetSnapshotsRequest.SortBy.REPOSITORY)
            .get()
            .getSnapshots();
    }

    public void testFilterBySLMPolicy() throws Exception {
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        final List<SnapshotInfo> snapshotsWithoutPolicy = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .get()
            .getSnapshots();
        final String snapshotWithPolicy = "snapshot-with-policy";
        final String policyName = "some-policy";
        final SnapshotInfo withPolicy = assertSuccessful(
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
        final SnapshotInfo withOtherPolicy = assertSuccessful(
            clusterAdmin().prepareCreateSnapshot(repoName, snapshotWithOtherPolicy)
                .setUserMetadata(Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, otherPolicyName))
                .setWaitForCompletion(true)
                .execute()
        );
        assertThat(getAllSnapshotsForPolicies("*"), is(List.of(withOtherPolicy, withPolicy)));
        assertThat(getAllSnapshotsForPolicies(policyName, otherPolicyName), is(List.of(withOtherPolicy, withPolicy)));
        assertThat(getAllSnapshotsForPolicies(policyName, otherPolicyName, "no-such-policy*"), is(List.of(withOtherPolicy, withPolicy)));

        final List<SnapshotInfo> allSnapshots = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .get()
            .getSnapshots();
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN, policyName, otherPolicyName), is(allSnapshots));
        assertThat(getAllSnapshotsForPolicies(GetSnapshotsRequest.NO_POLICY_PATTERN, "*"), is(allSnapshots));
    }

    public void testSortAfter() throws Exception {
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        final Set<Long> startTimes = new HashSet<>();
        final Set<Long> durations = new HashSet<>();
        final SnapshotInfo snapshot1 = createFullSnapshotWithUniqueTimestamps(repoName, "snapshot-1", startTimes, durations);
        createIndexWithContent("index-1");
        final SnapshotInfo snapshot2 = createFullSnapshotWithUniqueTimestamps(repoName, "snapshot-2", startTimes, durations);
        createIndexWithContent("index-2");
        final SnapshotInfo snapshot3 = createFullSnapshotWithUniqueTimestamps(repoName, "snapshot-3", startTimes, durations);
        createIndexWithContent("index-3");

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

        final String name1 = snapshot1.snapshotId().getName();
        final String name2 = snapshot2.snapshotId().getName();
        final String name3 = snapshot3.snapshotId().getName();

        assertThat(allAfterNameAscending("a"), is(allSnapshotInfo));
        assertThat(allAfterNameAscending(name1), is(allSnapshotInfo));
        assertThat(allAfterNameAscending(name2), is(List.of(snapshot2, snapshot3)));
        assertThat(allAfterNameAscending(name3), is(List.of(snapshot3)));
        assertThat(allAfterNameAscending("z"), empty());

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

        assertThat(allSnapshotInfoDesc, is(List.of(snapshot3, snapshot2, snapshot1)));
        assertThat(allBeforeNameDescending("z"), is(allSnapshotInfoDesc));
        assertThat(allBeforeNameDescending(name3), is(allSnapshotInfoDesc));
        assertThat(allBeforeNameDescending(name2), is(List.of(snapshot2, snapshot1)));
        assertThat(allBeforeNameDescending(name1), is(List.of(snapshot1)));
        assertThat(allBeforeNameDescending("a"), empty());

        final List<SnapshotInfo> allSnapshotInfoByDuration = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.DURATION)
            .get()
            .getSnapshots();

        final long duration1 = allSnapshotInfoByDuration.get(0).endTime() - allSnapshotInfoByDuration.get(0).startTime();
        final long duration2 = allSnapshotInfoByDuration.get(1).endTime() - allSnapshotInfoByDuration.get(1).startTime();
        final long duration3 = allSnapshotInfoByDuration.get(2).endTime() - allSnapshotInfoByDuration.get(2).startTime();

        assertThat(allAfterDurationAscending(duration1 - 1), is(allSnapshotInfoByDuration));
        assertThat(allAfterDurationAscending(duration1), is(allSnapshotInfoByDuration));
        assertThat(allAfterDurationAscending(duration2), is(allSnapshotInfoByDuration.subList(1, 3)));
        assertThat(allAfterDurationAscending(duration3), is(List.of(allSnapshotInfoByDuration.get(2))));
        assertThat(allAfterDurationAscending(duration3 + 1), empty());

        final List<SnapshotInfo> allSnapshotInfoByDurationDesc = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(GetSnapshotsRequest.SortBy.DURATION)
            .setOrder(SortOrder.DESC)
            .get()
            .getSnapshots();

        assertThat(allBeforeDurationDescending(duration3 + 1), is(allSnapshotInfoByDurationDesc));
        assertThat(allBeforeDurationDescending(duration3), is(allSnapshotInfoByDurationDesc));
        assertThat(allBeforeDurationDescending(duration2), is(allSnapshotInfoByDurationDesc.subList(1, 3)));
        assertThat(allBeforeDurationDescending(duration1), is(List.of(allSnapshotInfoByDurationDesc.get(2))));
        assertThat(allBeforeDurationDescending(duration1 - 1), empty());

        final SnapshotInfo otherSnapshot = createFullSnapshot(repoName, "other-snapshot");

        assertThat(allSnapshots(new String[] { "snap*" }, GetSnapshotsRequest.SortBy.NAME, SortOrder.ASC, "a"), is(allSnapshotInfo));
        assertThat(allSnapshots(new String[] { "o*" }, GetSnapshotsRequest.SortBy.NAME, SortOrder.ASC, "a"), is(List.of(otherSnapshot)));

        final GetSnapshotsResponse paginatedResponse = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots("snap*")
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .setFromSortValue("a")
            .setOffset(1)
            .setSize(1)
            .get();
        assertThat(paginatedResponse.getSnapshots(), is(List.of(snapshot2)));
        assertThat(paginatedResponse.totalCount(), is(3));
        final GetSnapshotsResponse paginatedResponse2 = clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots("snap*")
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .setFromSortValue("a")
            .setOffset(0)
            .setSize(2)
            .get();
        assertThat(paginatedResponse2.getSnapshots(), is(List.of(snapshot1, snapshot2)));
        assertThat(paginatedResponse2.totalCount(), is(3));
    }

    // Create a snapshot that is guaranteed to have a unique start time and duration for tests around ordering by either.
    // Don't use this with more than 3 snapshots on platforms with low-resolution clocks as the durations could always collide there
    // causing an infinite loop
    private SnapshotInfo createFullSnapshotWithUniqueTimestamps(
        String repoName,
        String snapshotName,
        Set<Long> forbiddenStartTimes,
        Set<Long> forbiddenDurations
    ) throws Exception {
        while (true) {
            final SnapshotInfo snapshotInfo = createFullSnapshot(repoName, snapshotName);
            final long duration = snapshotInfo.endTime() - snapshotInfo.startTime();
            if (forbiddenStartTimes.contains(snapshotInfo.startTime()) || forbiddenDurations.contains(duration)) {
                logger.info("--> snapshot start time or duration collided");
                assertAcked(startDeleteSnapshot(repoName, snapshotName).get());
            } else {
                assertTrue(forbiddenStartTimes.add(snapshotInfo.startTime()));
                assertTrue(forbiddenDurations.add(duration));
                return snapshotInfo;
            }
        }
    }

    private List<SnapshotInfo> allAfterStartTimeAscending(long timestamp) {
        return allSnapshots(matchAllPattern(), GetSnapshotsRequest.SortBy.START_TIME, SortOrder.ASC, timestamp);
    }

    private List<SnapshotInfo> allBeforeStartTimeDescending(long timestamp) {
        return allSnapshots(matchAllPattern(), GetSnapshotsRequest.SortBy.START_TIME, SortOrder.DESC, timestamp);
    }

    private List<SnapshotInfo> allAfterNameAscending(String name) {
        return allSnapshots(matchAllPattern(), GetSnapshotsRequest.SortBy.NAME, SortOrder.ASC, name);
    }

    private List<SnapshotInfo> allBeforeNameDescending(String name) {
        return allSnapshots(matchAllPattern(), GetSnapshotsRequest.SortBy.NAME, SortOrder.DESC, name);
    }

    private List<SnapshotInfo> allAfterDurationAscending(long duration) {
        return allSnapshots(matchAllPattern(), GetSnapshotsRequest.SortBy.DURATION, SortOrder.ASC, duration);
    }

    private List<SnapshotInfo> allBeforeDurationDescending(long duration) {
        return allSnapshots(matchAllPattern(), GetSnapshotsRequest.SortBy.DURATION, SortOrder.DESC, duration);
    }

    private static List<SnapshotInfo> allSnapshots(
        String[] snapshotNames,
        GetSnapshotsRequest.SortBy sortBy,
        SortOrder order,
        Object fromSortValue
    ) {
        return clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(snapshotNames)
            .setSort(sortBy)
            .setFromSortValue(fromSortValue.toString())
            .setOrder(order)
            .get()
            .getSnapshots();
    }

    private static List<SnapshotInfo> getAllSnapshotsForPolicies(String... policies) {
        return clusterAdmin().prepareGetSnapshots(matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setPolicies(policies)
            .setSort(GetSnapshotsRequest.SortBy.NAME)
            .get()
            .getSnapshots();
    }

    private static void assertStablePagination(String[] repoNames, Collection<String> allSnapshotNames, GetSnapshotsRequest.SortBy sort) {
        final SortOrder order = randomFrom(SortOrder.values());
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(allSnapshotNames, repoNames, sort, order);

        for (int i = 1; i <= allSnapshotNames.size(); i++) {
            final GetSnapshotsResponse subsetSorted = sortedWithLimit(repoNames, sort, null, i, order);
            assertEquals(allSorted.subList(0, i), subsetSorted.getSnapshots());
        }

        for (int j = 0; j < allSnapshotNames.size(); j++) {
            final SnapshotInfo after = allSorted.get(j);
            for (int i = 1; i < allSnapshotNames.size() - j; i++) {
                final GetSnapshotsResponse getSnapshotsResponse = sortedWithLimit(
                    repoNames,
                    sort,
                    GetSnapshotsRequest.After.from(after, sort).asQueryParam(),
                    i,
                    order
                );
                final GetSnapshotsResponse getSnapshotsResponseNumeric = sortedWithLimit(repoNames, sort, j + 1, i, order);
                final List<SnapshotInfo> subsetSorted = getSnapshotsResponse.getSnapshots();
                assertEquals(subsetSorted, getSnapshotsResponseNumeric.getSnapshots());
                assertEquals(subsetSorted, allSorted.subList(j + 1, j + i + 1));
                assertEquals(allSnapshotNames.size(), getSnapshotsResponse.totalCount());
                assertEquals(allSnapshotNames.size() - (j + i + 1), getSnapshotsResponse.remaining());
                assertEquals(subsetSorted, allSorted.subList(j + 1, j + i + 1));
                assertEquals(getSnapshotsResponseNumeric.totalCount(), getSnapshotsResponse.totalCount());
                assertEquals(getSnapshotsResponseNumeric.remaining(), getSnapshotsResponse.remaining());
            }
        }
    }

    private static List<SnapshotInfo> allSnapshotsSorted(
        Collection<String> allSnapshotNames,
        String[] repoNames,
        GetSnapshotsRequest.SortBy sortBy,
        SortOrder order,
        String... namePatterns
    ) {
        final GetSnapshotsResponse getSnapshotsResponse = sortedWithLimit(
            repoNames,
            sortBy,
            null,
            GetSnapshotsRequest.NO_LIMIT,
            order,
            namePatterns
        );
        final List<SnapshotInfo> snapshotInfos = getSnapshotsResponse.getSnapshots();
        assertEquals(snapshotInfos.size(), allSnapshotNames.size());
        assertEquals(getSnapshotsResponse.totalCount(), allSnapshotNames.size());
        assertEquals(0, getSnapshotsResponse.remaining());
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            assertThat(snapshotInfo.snapshotId().getName(), is(in(allSnapshotNames)));
        }
        return snapshotInfos;
    }

    private static GetSnapshotsResponse sortedWithLimit(
        String[] repoNames,
        GetSnapshotsRequest.SortBy sortBy,
        String after,
        int size,
        SortOrder order,
        String... namePatterns
    ) {
        return baseGetSnapshotsRequest(repoNames).setAfter(after)
            .setSort(sortBy)
            .setSize(size)
            .setOrder(order)
            .addSnapshots(namePatterns)
            .get();
    }

    private static GetSnapshotsResponse sortedWithLimit(
        String[] repoNames,
        GetSnapshotsRequest.SortBy sortBy,
        int offset,
        int size,
        SortOrder order
    ) {
        return baseGetSnapshotsRequest(repoNames).setOffset(offset).setSort(sortBy).setSize(size).setOrder(order).get();
    }

    private static GetSnapshotsRequestBuilder baseGetSnapshotsRequest(String[] repoNames) {
        return clusterAdmin().prepareGetSnapshots(repoNames)
            .setSnapshots("*", "-" + AbstractSnapshotIntegTestCase.OLD_VERSION_SNAPSHOT_PREFIX + "*");
    }
}
