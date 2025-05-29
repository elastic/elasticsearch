/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.SnapshotSortKey;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.getRepositoryDataBlobName;
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
        final List<SnapshotInfo> defaultSorting = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName)
            .setOrder(order)
            .get()
            .getSnapshots();
        assertSnapshotListSorted(defaultSorting, null, order);
        final String[] repos = { repoName };
        assertSnapshotListSorted(allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.NAME, order), SnapshotSortKey.NAME, order);
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.DURATION, order),
            SnapshotSortKey.DURATION,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.INDICES, order),
            SnapshotSortKey.INDICES,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.START_TIME, order),
            SnapshotSortKey.START_TIME,
            order
        );
        assertSnapshotListSorted(allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.SHARDS, order), SnapshotSortKey.SHARDS, order);
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.FAILED_SHARDS, order),
            SnapshotSortKey.FAILED_SHARDS,
            order
        );
        assertSnapshotListSorted(
            allSnapshotsSorted(allSnapshotNames, repos, SnapshotSortKey.REPOSITORY, order),
            SnapshotSortKey.REPOSITORY,
            order
        );
    }

    public void testResponseSizeLimit() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        maybeInitWithOldSnapshotVersion(repoName, repoPath);
        final List<String> names = createNSnapshots(repoName, randomIntBetween(6, 20));
        for (SnapshotSortKey sort : SnapshotSortKey.values()) {
            for (SortOrder order : SortOrder.values()) {
                logger.info("--> testing pagination for [{}] [{}]", sort, order);
                doTestPagination(repoName, names, sort, order);
            }
        }
    }

    private void doTestPagination(String repoName, List<String> names, SnapshotSortKey sort, SortOrder order) {
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
            state -> SnapshotsInProgress.get(state)
                .asStream()
                .flatMap(s -> s.shards().entrySet().stream())
                .allMatch(
                    e -> e.getKey().getIndexName().equals("test-index-1") == false
                        || e.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS
                )
        );
        final String[] repos = { repoName };
        assertStablePagination(repos, allSnapshotNames, SnapshotSortKey.START_TIME);
        assertStablePagination(repos, allSnapshotNames, SnapshotSortKey.NAME);
        assertStablePagination(repos, allSnapshotNames, SnapshotSortKey.INDICES);
        final List<SnapshotInfo> currentSnapshots = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT)
            .get()
            .getSnapshots();
        for (SnapshotInfo currentSnapshot : currentSnapshots) {
            assertThat(currentSnapshot.toString(), currentSnapshot.failedShards(), is(0));
        }

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
                .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT, "-snap*")
                .get()
                .getSnapshots(),
            empty()
        );

        unblockAllDataNodes(repoName);
        for (ActionFuture<CreateSnapshotResponse> inProgressSnapshot : inProgressSnapshots) {
            assertSuccessful(inProgressSnapshot);
        }

        assertStablePagination(repos, allSnapshotNames, SnapshotSortKey.START_TIME);
        assertStablePagination(repos, allSnapshotNames, SnapshotSortKey.NAME);
        assertStablePagination(repos, allSnapshotNames, SnapshotSortKey.INDICES);
    }

    public void testPaginationRequiresVerboseListing() throws Exception {
        final String repoName = "tst-repo";
        createRepository(repoName, "fs");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        expectThrows(
            ActionRequestValidationException.class,
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName)
                .setVerbose(false)
                .setSort(SnapshotSortKey.DURATION)
                .setSize(GetSnapshotsRequest.NO_LIMIT)
        );
        expectThrows(
            ActionRequestValidationException.class,
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName)
                .setVerbose(false)
                .setSort(SnapshotSortKey.START_TIME)
                .setSize(randomIntBetween(1, 100))
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
        final List<SnapshotInfo> allSorted = allSnapshotsSorted(allSnapshotNames, new String[] { "*" }, SnapshotSortKey.REPOSITORY, order);
        final List<SnapshotInfo> allSortedWithoutOther = allSnapshotsSorted(
            allSnapshotNamesWithoutOther,
            new String[] { "*", "-" + otherRepo },
            SnapshotSortKey.REPOSITORY,
            order
        );
        assertThat(allSortedWithoutOther, is(allSorted.subList(0, allSnapshotNamesWithoutOther.size())));

        final List<SnapshotInfo> allInOther = allSnapshotsSorted(
            namesOtherRepo,
            new String[] { "*", "-test-repo-*" },
            SnapshotSortKey.REPOSITORY,
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
            SnapshotSortKey.REPOSITORY,
            order,
            "-other*"
        );
        assertThat(allInOtherWithoutOtherPrefix, is(allInOther));

        final List<SnapshotInfo> allInOtherWithoutOtherExplicit = allSnapshotsSorted(
            namesOtherRepo,
            patternOtherRepo,
            SnapshotSortKey.REPOSITORY,
            order,
            "-" + otherPrefixSnapshot1,
            "-" + otherPrefixSnapshot2
        );
        assertThat(allInOtherWithoutOtherExplicit, is(allInOther));

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern()).setSnapshots("other*", "-o*").get().getSnapshots(),
            empty()
        );
        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "other*", "-o*").setSnapshots(matchAllPattern()).get().getSnapshots(),
            empty()
        );
        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "other*", otherRepo, "-o*")
                .setSnapshots(matchAllPattern())
                .get()
                .getSnapshots(),
            empty()
        );
        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
                .setSnapshots("non-existing*", otherPrefixSnapshot1, "-o*")
                .setIgnoreUnavailable(true)
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

        final List<SnapshotInfo> allSnapshots = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSort(SnapshotSortKey.REPOSITORY)
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
        return clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repos)
            .setSnapshots(snapshots)
            .setSort(SnapshotSortKey.REPOSITORY)
            .get()
            .getSnapshots();
    }

    public void testFilterBySLMPolicy() throws Exception {
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        final List<SnapshotInfo> snapshotsWithoutPolicy = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(SnapshotSortKey.NAME)
            .get()
            .getSnapshots();
        final String snapshotWithPolicy = "snapshot-with-policy";
        final String policyName = "some-policy";
        final SnapshotInfo withPolicy = assertSuccessful(
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotWithPolicy)
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
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotWithOtherPolicy)
                .setUserMetadata(Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, otherPolicyName))
                .setWaitForCompletion(true)
                .execute()
        );
        assertThat(getAllSnapshotsForPolicies("*"), is(List.of(withOtherPolicy, withPolicy)));
        assertThat(getAllSnapshotsForPolicies(policyName, otherPolicyName), is(List.of(withOtherPolicy, withPolicy)));
        assertThat(getAllSnapshotsForPolicies(policyName, otherPolicyName, "no-such-policy*"), is(List.of(withOtherPolicy, withPolicy)));

        final List<SnapshotInfo> allSnapshots = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(SnapshotSortKey.NAME)
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

        final List<SnapshotInfo> allSnapshotInfo = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
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

        final String name1 = snapshot1.snapshotId().getName();
        final String name2 = snapshot2.snapshotId().getName();
        final String name3 = snapshot3.snapshotId().getName();

        assertThat(allAfterNameAscending("a"), is(allSnapshotInfo));
        assertThat(allAfterNameAscending(name1), is(allSnapshotInfo));
        assertThat(allAfterNameAscending(name2), is(List.of(snapshot2, snapshot3)));
        assertThat(allAfterNameAscending(name3), is(List.of(snapshot3)));
        assertThat(allAfterNameAscending("z"), empty());

        final List<SnapshotInfo> allSnapshotInfoDesc = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
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

        assertThat(allSnapshotInfoDesc, is(List.of(snapshot3, snapshot2, snapshot1)));
        assertThat(allBeforeNameDescending("z"), is(allSnapshotInfoDesc));
        assertThat(allBeforeNameDescending(name3), is(allSnapshotInfoDesc));
        assertThat(allBeforeNameDescending(name2), is(List.of(snapshot2, snapshot1)));
        assertThat(allBeforeNameDescending(name1), is(List.of(snapshot1)));
        assertThat(allBeforeNameDescending("a"), empty());

        final List<SnapshotInfo> allSnapshotInfoByDuration = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(SnapshotSortKey.DURATION)
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

        final List<SnapshotInfo> allSnapshotInfoByDurationDesc = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setSort(SnapshotSortKey.DURATION)
            .setOrder(SortOrder.DESC)
            .get()
            .getSnapshots();

        assertThat(allBeforeDurationDescending(duration3 + 1), is(allSnapshotInfoByDurationDesc));
        assertThat(allBeforeDurationDescending(duration3), is(allSnapshotInfoByDurationDesc));
        assertThat(allBeforeDurationDescending(duration2), is(allSnapshotInfoByDurationDesc.subList(1, 3)));
        assertThat(allBeforeDurationDescending(duration1), is(List.of(allSnapshotInfoByDurationDesc.get(2))));
        assertThat(allBeforeDurationDescending(duration1 - 1), empty());

        final SnapshotInfo otherSnapshot = createFullSnapshot(repoName, "other-snapshot");

        assertThat(allSnapshots(new String[] { "snap*" }, SnapshotSortKey.NAME, SortOrder.ASC, "a"), is(allSnapshotInfo));
        assertThat(allSnapshots(new String[] { "o*" }, SnapshotSortKey.NAME, SortOrder.ASC, "a"), is(List.of(otherSnapshot)));

        final GetSnapshotsResponse paginatedResponse = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots("snap*")
            .setSort(SnapshotSortKey.NAME)
            .setFromSortValue("a")
            .setOffset(1)
            .setSize(1)
            .get();
        assertThat(paginatedResponse.getSnapshots(), is(List.of(snapshot2)));
        assertThat(paginatedResponse.totalCount(), is(3));
        final GetSnapshotsResponse paginatedResponse2 = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots("snap*")
            .setSort(SnapshotSortKey.NAME)
            .setFromSortValue("a")
            .setOffset(0)
            .setSize(2)
            .get();
        assertThat(paginatedResponse2.getSnapshots(), is(List.of(snapshot1, snapshot2)));
        assertThat(paginatedResponse2.totalCount(), is(3));
    }

    public void testRetrievingSnapshotsWhenRepositoryIsMissing() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        final String missingRepoName = "missing";

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(1, 10));
        snapshotNames.sort(String::compareTo);

        final var oneRepoFuture = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName, missingRepoName)
            .setSort(SnapshotSortKey.NAME)
            .setIgnoreUnavailable(randomBoolean())
            .execute();
        expectThrows(RepositoryMissingException.class, oneRepoFuture::actionGet);

        final var multiRepoFuture = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName, missingRepoName)
            .setSort(SnapshotSortKey.NAME)
            .setIgnoreUnavailable(randomBoolean())
            .execute();
        expectThrows(RepositoryMissingException.class, multiRepoFuture::actionGet);
    }

    public void testFilterByState() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);

        // Create a successful snapshot
        createFullSnapshot(repoName, "snapshot-success");

        final Function<EnumSet<SnapshotState>, List<SnapshotInfo>> getSnapshotsForStates = (states) -> {
            return clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).setStates(states).get().getSnapshots();
        };

        // Fetch snapshots with state=SUCCESS
        var snapshots = getSnapshotsForStates.apply(EnumSet.of(SnapshotState.SUCCESS));
        assertThat(snapshots, hasSize(1));
        assertThat(snapshots.getFirst().state(), is(SnapshotState.SUCCESS));

        // Create a snapshot in progress
        blockAllDataNodes(repoName);
        startFullSnapshot(repoName, "snapshot-in-progress");
        awaitNumberOfSnapshotsInProgress(1);

        // Fetch snapshots with state=IN_PROGRESS
        snapshots = getSnapshotsForStates.apply(EnumSet.of(SnapshotState.IN_PROGRESS));
        assertThat(snapshots, hasSize(1));
        assertThat(snapshots.getFirst().state(), is(SnapshotState.IN_PROGRESS));

        // Fetch snapshots with multiple states (SUCCESS, IN_PROGRESS)
        snapshots = getSnapshotsForStates.apply(EnumSet.of(SnapshotState.SUCCESS, SnapshotState.IN_PROGRESS));
        assertThat(snapshots, hasSize(2));
        var states = snapshots.stream().map(SnapshotInfo::state).collect(Collectors.toSet());
        assertTrue(states.contains(SnapshotState.SUCCESS));
        assertTrue(states.contains(SnapshotState.IN_PROGRESS));

        // Fetch all snapshots (without state)
        snapshots = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).get().getSnapshots();
        assertThat(snapshots, hasSize(2));

        // Fetch snapshots with an invalid state
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getSnapshotsForStates.apply(EnumSet.of(SnapshotState.valueOf("FOO")))
        );
        assertThat(e.getMessage(), is("No enum constant org.elasticsearch.snapshots.SnapshotState.FOO"));

        // Allow the IN_PROGRESS snapshot to finish, then verify GET using SUCCESS has results and IN_PROGRESS does not.
        unblockAllDataNodes(repoName);
        awaitNumberOfSnapshotsInProgress(0);
        snapshots = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).get().getSnapshots();
        assertThat(snapshots, hasSize(2));
        states = snapshots.stream().map(SnapshotInfo::state).collect(Collectors.toSet());
        assertThat(states, hasSize(1));
        assertTrue(states.contains(SnapshotState.SUCCESS));
        snapshots = getSnapshotsForStates.apply(EnumSet.of(SnapshotState.IN_PROGRESS));
        assertThat(snapshots, hasSize(0));
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
        return allSnapshots(matchAllPattern(), SnapshotSortKey.START_TIME, SortOrder.ASC, timestamp);
    }

    private List<SnapshotInfo> allBeforeStartTimeDescending(long timestamp) {
        return allSnapshots(matchAllPattern(), SnapshotSortKey.START_TIME, SortOrder.DESC, timestamp);
    }

    private List<SnapshotInfo> allAfterNameAscending(String name) {
        return allSnapshots(matchAllPattern(), SnapshotSortKey.NAME, SortOrder.ASC, name);
    }

    private List<SnapshotInfo> allBeforeNameDescending(String name) {
        return allSnapshots(matchAllPattern(), SnapshotSortKey.NAME, SortOrder.DESC, name);
    }

    private List<SnapshotInfo> allAfterDurationAscending(long duration) {
        return allSnapshots(matchAllPattern(), SnapshotSortKey.DURATION, SortOrder.ASC, duration);
    }

    private List<SnapshotInfo> allBeforeDurationDescending(long duration) {
        return allSnapshots(matchAllPattern(), SnapshotSortKey.DURATION, SortOrder.DESC, duration);
    }

    private static List<SnapshotInfo> allSnapshots(String[] snapshotNames, SnapshotSortKey sortBy, SortOrder order, Object fromSortValue) {
        return clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(snapshotNames)
            .setSort(sortBy)
            .setFromSortValue(fromSortValue.toString())
            .setOrder(order)
            .get()
            .getSnapshots();
    }

    private static List<SnapshotInfo> getAllSnapshotsForPolicies(String... policies) {
        return clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, matchAllPattern())
            .setSnapshots(matchAllPattern())
            .setPolicies(policies)
            .setSort(SnapshotSortKey.NAME)
            .get()
            .getSnapshots();
    }

    private static void assertStablePagination(String[] repoNames, Collection<String> allSnapshotNames, SnapshotSortKey sort) {
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
                    sort.encodeAfterQueryParam(after),
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
        SnapshotSortKey sortBy,
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
        SnapshotSortKey sortBy,
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

    private static GetSnapshotsResponse sortedWithLimit(String[] repoNames, SnapshotSortKey sortBy, int offset, int size, SortOrder order) {
        return baseGetSnapshotsRequest(repoNames).setOffset(offset).setSort(sortBy).setSize(size).setOrder(order).get();
    }

    private static GetSnapshotsRequestBuilder baseGetSnapshotsRequest(String[] repoNames) {
        return clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoNames)
            .setSnapshots("*", "-" + AbstractSnapshotIntegTestCase.OLD_VERSION_SNAPSHOT_PREFIX + "*");
    }

    public void testAllFeatures() {
        // A test that uses (potentially) as many of the features of the get-snapshots API at once as possible, to verify that they interact
        // in the expected order etc.

        // Create a few repositories and a few indices
        final var repositories = randomList(1, 4, ESTestCase::randomIdentifier);
        final var indices = randomList(1, 4, ESTestCase::randomIdentifier);
        final var slmPolicies = randomList(1, 4, ESTestCase::randomIdentifier);

        safeAwait(l -> {
            try (var listeners = new RefCountingListener(l.map(v -> null))) {
                for (final var repository : repositories) {
                    client().execute(
                        TransportPutRepositoryAction.TYPE,
                        new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repository).type(FsRepository.TYPE)
                            .settings(Settings.builder().put("location", randomRepoPath()).build()),
                        listeners.acquire(ElasticsearchAssertions::assertAcked)
                    );
                }

                for (final var index : indices) {
                    client().execute(
                        TransportCreateIndexAction.TYPE,
                        new CreateIndexRequest(index, indexSettings(1, 0).build()),
                        listeners.acquire(ElasticsearchAssertions::assertAcked)
                    );
                }
            }
        });
        ensureGreen();

        // Create a few snapshots
        final var snapshotInfos = Collections.synchronizedList(new ArrayList<SnapshotInfo>());
        safeAwait(l -> {
            try (var listeners = new RefCountingListener(l.map(v -> null))) {
                for (int i = 0; i < 10; i++) {
                    client().execute(
                        TransportCreateSnapshotAction.TYPE,
                        new CreateSnapshotRequest(
                            TEST_REQUEST_TIMEOUT,
                            // at least one snapshot per repository to satisfy consistency checks
                            i < repositories.size() ? repositories.get(i) : randomFrom(repositories),
                            randomIdentifier()
                        ).indices(randomNonEmptySubsetOf(indices))
                            .userMetadata(
                                randomBoolean() ? Map.of() : Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, randomFrom(slmPolicies))
                            )
                            .waitForCompletion(true),
                        listeners.acquire(
                            createSnapshotResponse -> snapshotInfos.add(Objects.requireNonNull(createSnapshotResponse.getSnapshotInfo()))
                        )
                    );
                }
            }
        });

        if (randomBoolean()) {
            // Sometimes also simulate bwc repository contents where some details are missing from the root blob
            safeAwait(l -> {
                try (var listeners = new RefCountingListener(l.map(v -> null))) {
                    for (final var repositoryName : randomSubsetOf(repositories)) {
                        removeDetailsForRandomSnapshots(repositoryName, listeners.acquire());
                    }
                }
            });
        }

        Predicate<SnapshotInfo> snapshotInfoPredicate = Predicates.always();

        // {repository} path parameter
        final String[] requestedRepositories;
        if (randomBoolean()) {
            requestedRepositories = new String[] { randomFrom("_all", "*") };
        } else {
            final var selectedRepositories = Set.copyOf(randomNonEmptySubsetOf(repositories));
            snapshotInfoPredicate = snapshotInfoPredicate.and(si -> selectedRepositories.contains(si.repository()));
            requestedRepositories = selectedRepositories.toArray(new String[0]);
        }

        // {snapshot} path parameter
        final String[] requestedSnapshots;
        if (randomBoolean()) {
            requestedSnapshots = randomBoolean() ? Strings.EMPTY_ARRAY : new String[] { randomFrom("_all", "*") };
        } else {
            final var selectedSnapshots = randomNonEmptySubsetOf(snapshotInfos).stream()
                .map(si -> si.snapshotId().getName())
                .collect(Collectors.toSet());
            snapshotInfoPredicate = snapshotInfoPredicate.and(si -> selectedSnapshots.contains(si.snapshotId().getName()));
            requestedSnapshots = selectedSnapshots.stream()
                // if we have multiple repositories, add a trailing wildcard to each requested snapshot name, because if we specify exact
                // names then there must be a snapshot with that name in every requested repository
                .map(n -> repositories.size() == 1 && randomBoolean() ? n : n + "*")
                .toArray(String[]::new);
        }

        // ?slm_policy_filter parameter
        final String[] requestedSlmPolicies;
        switch (between(0, 3)) {
            default -> requestedSlmPolicies = Strings.EMPTY_ARRAY;
            case 1 -> {
                requestedSlmPolicies = new String[] { "*" };
                snapshotInfoPredicate = snapshotInfoPredicate.and(
                    si -> si.userMetadata().get(SnapshotsService.POLICY_ID_METADATA_FIELD) != null
                );
            }
            case 2 -> {
                requestedSlmPolicies = new String[] { "_none" };
                snapshotInfoPredicate = snapshotInfoPredicate.and(
                    si -> si.userMetadata().get(SnapshotsService.POLICY_ID_METADATA_FIELD) == null
                );
            }
            case 3 -> {
                final var selectedPolicies = Set.copyOf(randomNonEmptySubsetOf(slmPolicies));
                requestedSlmPolicies = selectedPolicies.stream()
                    .map(policy -> randomBoolean() ? policy : policy + "*")
                    .toArray(String[]::new);
                snapshotInfoPredicate = snapshotInfoPredicate.and(
                    si -> si.userMetadata().get(SnapshotsService.POLICY_ID_METADATA_FIELD) instanceof String policy
                        && selectedPolicies.contains(policy)
                );
            }
        }

        // ?sort and ?order parameters
        final var sortKey = randomFrom(SnapshotSortKey.values());
        final var order = randomFrom(SortOrder.values());
        // NB we sometimes choose to sort by FAILED_SHARDS, but there are no failed shards in these snapshots. We're still testing the
        // fallback sorting by snapshot ID in this case. We also have no multi-shard indices so there's no difference between sorting by
        // INDICES and by SHARDS. The actual sorting behaviour for these cases is tested elsewhere, here we're just checking that sorting
        // interacts correctly with the other parameters to the API.

        final EnumSet<SnapshotState> states = EnumSet.copyOf(randomNonEmptySubsetOf(Arrays.asList(SnapshotState.values())));
        // Note: The selected state(s) may not match any existing snapshots.
        // The actual filtering behaviour for such cases is tested in the dedicated test.
        // Here we're just checking that states interacts correctly with the other parameters to the API.
        snapshotInfoPredicate = snapshotInfoPredicate.and(si -> states.contains(si.state()));

        // compute the ordered sequence of snapshots which match the repository/snapshot name filters and SLM policy filter
        final var selectedSnapshots = snapshotInfos.stream()
            .filter(snapshotInfoPredicate)
            .sorted(sortKey.getSnapshotInfoComparator(order))
            .toList();

        final var getSnapshotsRequest = new GetSnapshotsRequest(TEST_REQUEST_TIMEOUT, requestedRepositories, requestedSnapshots).policies(
            requestedSlmPolicies
        )
            // apply sorting params
            .sort(sortKey)
            .order(order)
            .states(states);

        // sometimes use ?from_sort_value to skip some items; note that snapshots skipped in this way are subtracted from
        // GetSnapshotsResponse.totalCount whereas snapshots skipped by ?after and ?offset are not
        final int skippedByFromSortValue;
        if (randomBoolean()) {
            final var startingSnapshot = randomFrom(snapshotInfos);
            getSnapshotsRequest.fromSortValue(switch (sortKey) {
                case START_TIME -> Long.toString(startingSnapshot.startTime());
                case NAME -> startingSnapshot.snapshotId().getName();
                case DURATION -> Long.toString(startingSnapshot.endTime() - startingSnapshot.startTime());
                case INDICES, SHARDS -> Integer.toString(startingSnapshot.indices().size());
                case FAILED_SHARDS -> "0";
                case REPOSITORY -> startingSnapshot.repository();
            });
            final Predicate<SnapshotInfo> fromSortValuePredicate = snapshotInfo -> {
                final var comparison = switch (sortKey) {
                    case START_TIME -> Long.compare(snapshotInfo.startTime(), startingSnapshot.startTime());
                    case NAME -> snapshotInfo.snapshotId().getName().compareTo(startingSnapshot.snapshotId().getName());
                    case DURATION -> Long.compare(
                        snapshotInfo.endTime() - snapshotInfo.startTime(),
                        startingSnapshot.endTime() - startingSnapshot.startTime()
                    );
                    case INDICES, SHARDS -> Integer.compare(snapshotInfo.indices().size(), startingSnapshot.indices().size());
                    case FAILED_SHARDS -> 0;
                    case REPOSITORY -> snapshotInfo.repository().compareTo(startingSnapshot.repository());
                };
                return order == SortOrder.ASC ? comparison < 0 : comparison > 0;
            };

            int skipCount = 0;
            for (final var snapshotInfo : selectedSnapshots) {
                if (fromSortValuePredicate.test(snapshotInfo)) {
                    skipCount += 1;
                } else {
                    break;
                }
            }
            skippedByFromSortValue = skipCount;
        } else {
            skippedByFromSortValue = 0;
        }

        // ?offset parameter
        if (randomBoolean()) {
            getSnapshotsRequest.offset(between(0, selectedSnapshots.size() + 1));
        }

        // ?size parameter
        if (randomBoolean()) {
            getSnapshotsRequest.size(between(1, selectedSnapshots.size() + 1));
        }

        // compute the expected offset and size of the returned snapshots as indices in selectedSnapshots:
        final var expectedOffset = Math.min(selectedSnapshots.size(), skippedByFromSortValue + getSnapshotsRequest.offset());
        final var expectedSize = Math.min(
            selectedSnapshots.size() - expectedOffset,
            getSnapshotsRequest.size() == GetSnapshotsRequest.NO_LIMIT ? Integer.MAX_VALUE : getSnapshotsRequest.size()
        );

        // get the actual response
        final GetSnapshotsResponse getSnapshotsResponse = safeAwait(
            l -> client().execute(TransportGetSnapshotsAction.TYPE, getSnapshotsRequest, l)
        );

        // verify it returns the expected results
        assertEquals(
            selectedSnapshots.stream().skip(expectedOffset).limit(expectedSize).map(SnapshotInfo::snapshotId).toList(),
            getSnapshotsResponse.getSnapshots().stream().map(SnapshotInfo::snapshotId).toList()
        );
        assertEquals(expectedSize, getSnapshotsResponse.getSnapshots().size());
        assertEquals(selectedSnapshots.size() - skippedByFromSortValue, getSnapshotsResponse.totalCount());
        assertEquals(selectedSnapshots.size() - expectedOffset - expectedSize, getSnapshotsResponse.remaining());
        assertEquals(getSnapshotsResponse.remaining() > 0, getSnapshotsResponse.next() != null);

        // now use ?after to page through the rest of the results
        var nextRequestAfter = getSnapshotsResponse.next();
        var nextExpectedOffset = expectedOffset + expectedSize;
        var remaining = getSnapshotsResponse.remaining();
        while (nextRequestAfter != null) {
            final var nextSize = between(1, remaining);
            final var nextRequest = new GetSnapshotsRequest(TEST_REQUEST_TIMEOUT, requestedRepositories, requestedSnapshots)
                // same name/policy filters, same ?sort and ?order params, new ?size, but no ?offset or ?from_sort_value because of ?after
                .policies(requestedSlmPolicies)
                .sort(sortKey)
                .order(order)
                .size(nextSize)
                .after(SnapshotSortKey.decodeAfterQueryParam(nextRequestAfter))
                .states(states);
            final GetSnapshotsResponse nextResponse = safeAwait(l -> client().execute(TransportGetSnapshotsAction.TYPE, nextRequest, l));

            assertEquals(
                selectedSnapshots.stream().skip(nextExpectedOffset).limit(nextSize).map(SnapshotInfo::snapshotId).toList(),
                nextResponse.getSnapshots().stream().map(SnapshotInfo::snapshotId).toList()
            );
            assertEquals(nextSize, nextResponse.getSnapshots().size());
            assertEquals(selectedSnapshots.size(), nextResponse.totalCount());
            assertEquals(remaining - nextSize, nextResponse.remaining());
            assertEquals(nextResponse.remaining() > 0, nextResponse.next() != null);

            nextRequestAfter = nextResponse.next();
            nextExpectedOffset += nextSize;
            remaining -= nextSize;
        }

        assertEquals(0, remaining);
    }

    /**
     * Older versions of Elasticsearch don't record in {@link RepositoryData} all the details needed for the get-snapshots API to pick out
     * the right snapshots, so in this case the API must fall back to reading those details from each candidate {@link SnapshotInfo} blob.
     * Simulate this situation by manipulating the {@link RepositoryData} blob directly to remove all the optional details from some subset
     * of its snapshots.
     */
    private static void removeDetailsForRandomSnapshots(String repositoryName, ActionListener<Void> listener) {
        final Set<SnapshotId> snapshotsWithoutDetails = ConcurrentCollections.newConcurrentSet();
        final var masterRepositoriesService = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class);
        final var repository = asInstanceOf(FsRepository.class, masterRepositoriesService.repository(repositoryName));
        final var repositoryMetadata = repository.getMetadata();
        final var repositorySettings = repositoryMetadata.settings();
        final var repositoryDataBlobPath = asInstanceOf(FsBlobStore.class, repository.blobStore()).path()
            .resolve(getRepositoryDataBlobName(repositoryMetadata.generation()));

        SubscribableListener

            // unregister the repository while we're mucking around with its internals
            .<AcknowledgedResponse>newForked(
                l -> client().execute(
                    TransportDeleteRepositoryAction.TYPE,
                    new DeleteRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName),
                    l
                )
            )
            .andThenAccept(ElasticsearchAssertions::assertAcked)

            // rewrite the RepositoryData blob with some details removed
            .andThenAccept(ignored -> {
                // load the existing RepositoryData JSON blob as raw maps/lists/etc.
                final var repositoryDataBytes = Files.readAllBytes(repositoryDataBlobPath);
                final var repositoryDataMap = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    repositoryDataBytes,
                    0,
                    repositoryDataBytes.length,
                    true
                );

                // modify the contents
                final var snapshotsList = asInstanceOf(List.class, repositoryDataMap.get("snapshots"));
                for (final var snapshotObj : snapshotsList) {
                    if (randomBoolean()) {
                        continue;
                    }
                    final var snapshotMap = asInstanceOf(Map.class, snapshotObj);
                    snapshotsWithoutDetails.add(
                        new SnapshotId(
                            asInstanceOf(String.class, snapshotMap.get("name")),
                            asInstanceOf(String.class, snapshotMap.get("uuid"))
                        )
                    );

                    // remove the optional details fields
                    assertNotNull(snapshotMap.remove("start_time_millis"));
                    assertNotNull(snapshotMap.remove("end_time_millis"));
                    assertNotNull(snapshotMap.remove("slm_policy"));
                }

                // overwrite the RepositoryData JSON blob with its new contents
                final var updatedRepositoryDataBytes = XContentTestUtils.convertToXContent(repositoryDataMap, XContentType.JSON);
                try (var outputStream = Files.newOutputStream(repositoryDataBlobPath)) {
                    BytesRef bytesRef;
                    final var iterator = updatedRepositoryDataBytes.iterator();
                    while ((bytesRef = iterator.next()) != null) {
                        outputStream.write(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                    }
                }
            })

            // re-register the repository
            .<AcknowledgedResponse>andThen(
                l -> client().execute(
                    TransportPutRepositoryAction.TYPE,
                    new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName).type(FsRepository.TYPE)
                        .settings(repositorySettings),
                    l
                )
            )
            .andThenAccept(ElasticsearchAssertions::assertAcked)

            // verify that the details are indeed now missing
            .<RepositoryData>andThen(
                l -> masterRepositoriesService.repository(repositoryName).getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, l)
            )
            .andThenAccept(repositoryData -> {
                for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                    assertEquals(
                        repositoryName + "/" + snapshotId.toString() + ": " + repositoryData.getSnapshotDetails(snapshotId),
                        snapshotsWithoutDetails.contains(snapshotId),
                        repositoryData.hasMissingDetails(snapshotId)
                    );
                }
            })

            .addListener(listener);
    }
}
