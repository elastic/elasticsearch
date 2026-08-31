/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RecoverySourceDiscovery.RecoverySource;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class RecoverySourceDiscoveryTests extends ESTestCase {

    private static final String REPO = "test-repo";

    private static Snapshot snapshot() {
        return new Snapshot(REPO, new SnapshotId("snap", randomAlphaOfLength(8)));
    }

    private static Index index(String name) {
        return new Index(name, randomAlphaOfLength(8));
    }

    private static RecoverySource idx(String name) {
        return new RecoverySource(name, RecoverySource.Type.INDEX);
    }

    private static RecoverySource ds(String name) {
        return new RecoverySource(name, RecoverySource.Type.DATA_STREAM);
    }

    private static SortedSet<RecoverySource> candidates(RecoverySource... sources) {
        SortedSet<RecoverySource> set = new TreeSet<>();
        for (RecoverySource s : sources) {
            set.add(s);
        }
        return set;
    }

    private static SnapshotInfo.IndexSnapshotDetails successDetails() {
        return new SnapshotInfo.IndexSnapshotDetails(randomIntBetween(1, 3), ByteSizeValue.ofBytes(512), 1);
    }

    private static SnapshotShardFailure shardFailure(String indexName) {
        return new SnapshotShardFailure(null, new ShardId(indexName, randomAlphaOfLength(8), 0), "node left");
    }

    private static IndexMetadata userIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static IndexMetadata systemIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();
    }

    private static SnapshotInfo successSnap(
        List<String> indices,
        List<String> dataStreams,
        Map<String, SnapshotInfo.IndexSnapshotDetails> details
    ) {
        return new SnapshotInfo(
            snapshot(),
            indices,
            dataStreams,
            Collections.emptyList(),
            null,
            0L,
            indices.size(),
            Collections.emptyList(),
            null,
            null,
            0L,
            details
        );
    }

    private static SnapshotInfo successSnap(List<String> indexNames) {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        for (String name : indexNames) {
            details.put(name, successDetails());
        }
        return successSnap(indexNames, List.of(), details);
    }

    private static SnapshotInfo partialSnap(
        List<String> indices,
        List<String> dataStreams,
        Map<String, SnapshotInfo.IndexSnapshotDetails> details,
        List<SnapshotShardFailure> failures
    ) {
        return new SnapshotInfo(
            snapshot(),
            indices,
            dataStreams,
            Collections.emptyList(),
            null,
            0L,
            indices.size() * 2,
            failures,
            null,
            null,
            0L,
            details
        );
    }

    private static ProjectMetadata projectMetaFor(List<String> indexNames) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        for (String name : indexNames) {
            builder.put(userIndex(name), false);
        }
        return builder.build();
    }

    private static ProjectMetadata projectMeta(List<IndexMetadata> indices, List<DataStream> dataStreams) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        for (IndexMetadata meta : indices) {
            builder.put(meta, false);
        }
        for (DataStream ds : dataStreams) {
            builder.put(ds);
        }
        return builder.build();
    }

    // -------------------------------------------------------------------------
    // buildCandidates — candidate filtering
    // -------------------------------------------------------------------------

    public void testEmptySnapshotProducesNoCandidates() {
        SnapshotInfo snap = successSnap(List.of(), List.of(), Map.of());
        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(snap, projectMeta(List.of(), List.of()));
        assertThat(result, empty());
    }

    public void testCompleteUserIndexIsIncluded() {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = Map.of("my-index", successDetails());
        SnapshotInfo snap = successSnap(List.of("my-index"), List.of(), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(
            snap,
            projectMeta(List.of(userIndex("my-index")), List.of())
        );

        assertThat(result, hasSize(1));
        assertEquals(new RecoverySource("my-index", RecoverySource.Type.INDEX), result.first());
    }

    public void testSystemIndexIsExcluded() {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = Map.of(".system-idx", successDetails());
        SnapshotInfo snap = successSnap(List.of(".system-idx"), List.of(), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(
            snap,
            projectMeta(List.of(systemIndex(".system-idx")), List.of())
        );

        assertThat(result, empty());
    }

    public void testIncompleteIndexIsExcluded() {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put("my-index", successDetails());
        details.put("bad-index", SnapshotInfo.IndexSnapshotDetails.SKIPPED);
        SnapshotInfo snap = partialSnap(List.of("my-index", "bad-index"), List.of(), details, List.of(shardFailure("bad-index")));

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(
            snap,
            projectMeta(List.of(userIndex("my-index"), userIndex("bad-index")), List.of())
        );

        assertThat(result, hasSize(1));
        assertEquals("my-index", result.first().name());
    }

    public void testBackingIndexIsExcludedEvenWhenComplete() {
        String backing = ".ds-logs-app-000001";
        DataStream ds = DataStreamTestHelper.newInstance("logs-app", List.of(index(backing)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = Map.of(backing, successDetails());
        SnapshotInfo snap = successSnap(List.of(backing), List.of("logs-app"), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(
            snap,
            projectMeta(List.of(userIndex(backing)), List.of(ds))
        );

        assertTrue(result.stream().noneMatch(s -> s.name().equals(backing)));
    }

    public void testFailureStoreIndexIsExcluded() {
        String backing = ".ds-logs-app-000001";
        String failure = ".fs-logs-app-000001";
        DataStream ds = DataStreamTestHelper.newInstance("logs-app", List.of(index(backing)), List.of(index(failure)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(backing, successDetails());
        details.put(failure, successDetails());
        SnapshotInfo snap = successSnap(List.of(backing, failure), List.of("logs-app"), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(
            snap,
            projectMeta(List.of(userIndex(backing), userIndex(failure)), List.of(ds))
        );

        assertTrue(result.stream().noneMatch(s -> s.name().equals(failure)));
    }

    public void testCompleteDataStreamIsIncluded() {
        String backing = ".ds-logs-app-000001";
        DataStream ds = DataStreamTestHelper.newInstance("logs-app", List.of(index(backing)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = Map.of(backing, successDetails());
        SnapshotInfo snap = successSnap(List.of(backing), List.of("logs-app"), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(snap, projectMeta(List.of(), List.of(ds)));

        assertThat(result, hasSize(1));
        assertEquals(new RecoverySource("logs-app", RecoverySource.Type.DATA_STREAM), result.first());
    }

    public void testSystemDataStreamIsExcluded() {
        String backing = ".ds-.system-stream-000001";
        DataStream ds = DataStream.builder(".system-stream", List.of(index(backing))).setSystem(true).setHidden(true).build();
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = Map.of(backing, successDetails());
        SnapshotInfo snap = successSnap(List.of(backing), List.of(".system-stream"), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(snap, projectMeta(List.of(), List.of(ds)));

        assertThat(result, empty());
    }

    public void testIncompleteDataStreamIsExcluded() {
        String backing1 = ".ds-logs-app-000001";
        String backing2 = ".ds-logs-app-000002";
        DataStream ds = DataStreamTestHelper.newInstance("logs-app", List.of(index(backing1), index(backing2)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(backing1, successDetails());
        details.put(backing2, SnapshotInfo.IndexSnapshotDetails.SKIPPED);
        SnapshotInfo snap = partialSnap(List.of(backing1, backing2), List.of("logs-app"), details, List.of(shardFailure(backing2)));

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(snap, projectMeta(List.of(), List.of(ds)));

        assertThat(result, empty());
    }

    public void testCandidatesOrderedByName() {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put("zebra-index", successDetails());
        details.put("apple-index", successDetails());
        details.put("mango-index", successDetails());
        SnapshotInfo snap = successSnap(List.of("zebra-index", "apple-index", "mango-index"), List.of(), details);

        SortedSet<RecoverySource> result = RecoverySourceDiscovery.buildCandidates(
            snap,
            projectMeta(List.of(userIndex("zebra-index"), userIndex("apple-index"), userIndex("mango-index")), List.of())
        );

        assertThat(result, hasSize(3));
        assertThat(result.stream().map(RecoverySource::name).toList(), contains("apple-index", "mango-index", "zebra-index"));
    }

    // -------------------------------------------------------------------------
    // discover — full pipeline (size validation, pagination, expressions)
    // -------------------------------------------------------------------------

    public void testZeroSizeThrows() {
        SnapshotInfo snap = successSnap(List.of("a"));
        expectThrows(
            IllegalArgumentException.class,
            () -> RecoverySourceDiscovery.discover(snap, projectMetaFor(List.of("a")), List.of(), 0)
        );
    }

    public void testNegativeSizeThrows() {
        SnapshotInfo snap = successSnap(List.of("a"));
        expectThrows(
            IllegalArgumentException.class,
            () -> RecoverySourceDiscovery.discover(snap, projectMetaFor(List.of("a")), List.of(), -1)
        );
    }

    public void testEmptySnapshotReturnsEmptyResult() {
        SnapshotInfo snap = successSnap(List.of());
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMeta(List.of(), List.of()), List.of(), 10);
        assertThat(result.sources(), empty());
        assertFalse(result.hasMore());
    }

    public void testResultsWithinSizeHasMoreFalse() {
        List<String> names = List.of("a", "b", "c");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of(), 10);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("a", "b", "c"));
        assertFalse(result.hasMore());
    }

    public void testResultsExceedingSizeHasMoreTrue() {
        List<String> names = List.of("a", "b", "c", "d", "e");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of(), 3);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("a", "b", "c"));
        assertTrue(result.hasMore());
    }

    public void testExactlyAtSizeHasMoreFalse() {
        List<String> names = List.of("a", "b", "c");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of(), 3);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("a", "b", "c"));
        assertFalse(result.hasMore());
    }

    public void testSizeOneReturnsFirstSourceOnly() {
        List<String> names = List.of("a", "b", "c");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of(), 1);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("a"));
        assertTrue(result.hasMore());
    }

    public void testExactNameDoesNotMatchLongerNames() {
        List<String> names = List.of("logs-app", "logs-app-v2", "logs-app-2024");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of("logs-app"), 10);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("logs-app"));
        assertFalse(result.hasMore());
    }

    public void testNegativeWildcardExcludesMatchingNames() {
        List<String> names = List.of("logs-app", "logs-audit", "metrics-app");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of("*", "-logs-*"), 10);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("metrics-app"));
        assertFalse(result.hasMore());
    }

    public void testLaterPositiveExpressionReincludesExcludedSources() {
        List<String> names = List.of("logs-app", "metrics-app");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(
            snap,
            projectMetaFor(names),
            List.of("*", "-logs-*", "*"),
            10
        );
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("logs-app", "metrics-app"));
        assertFalse(result.hasMore());
    }

    public void testExpressionWithNoMatchesReturnsEmpty() {
        List<String> names = List.of("logs-app", "metrics-app");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of("missing-*"), 10);
        assertThat(result.sources(), empty());
        assertFalse(result.hasMore());
    }

    public void testExactNameExclusionDoesNotExcludeLongerNames() {
        List<String> names = List.of("logs-app", "logs-app-v2", "logs-app-2024");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(
            snap,
            projectMetaFor(names),
            List.of("logs-app*", "-logs-app"),
            10
        );
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("logs-app-2024", "logs-app-v2"));
        assertFalse(result.hasMore());
    }

    public void testExpressionFilterNarrowsResults() {
        List<String> names = List.of("logs-app", "metrics-app", "traces-app");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(
            snap,
            projectMetaFor(names),
            List.of("logs-*", "metrics-*"),
            10
        );
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("logs-app", "metrics-app"));
        assertFalse(result.hasMore());
    }

    public void testCompleteDataStreamIncludedBackingIndexExcluded() {
        String backing = ".ds-logs-stream-000001";
        DataStream ds = DataStreamTestHelper.newInstance("logs-stream", List.of(index(backing)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(backing, successDetails());
        SnapshotInfo snap = new SnapshotInfo(
            snapshot(),
            List.of(backing),
            List.of("logs-stream"),
            Collections.emptyList(),
            null,
            0L,
            1,
            Collections.emptyList(),
            null,
            null,
            0L,
            details
        );

        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(
            snap,
            projectMeta(List.of(userIndex(backing)), List.of(ds)),
            List.of(),
            10
        );

        assertEquals(1, result.sources().size());
        assertEquals("logs-stream", result.sources().get(0).name());
        assertEquals(RecoverySource.Type.DATA_STREAM, result.sources().get(0).type());
        assertFalse(result.hasMore());
    }

    public void testSourcesOrderedByName() {
        List<String> names = List.of("zebra", "apple", "mango");
        SnapshotInfo snap = successSnap(names);
        RecoverySourceDiscovery.Result result = RecoverySourceDiscovery.discover(snap, projectMetaFor(names), List.of(), 10);
        assertThat(result.sources().stream().map(RecoverySource::name).toList(), contains("apple", "mango", "zebra"));
    }

}
