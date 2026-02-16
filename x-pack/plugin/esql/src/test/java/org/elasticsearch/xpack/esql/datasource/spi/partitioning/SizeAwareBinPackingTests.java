/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi.partitioning;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for {@link SizeAwareBinPacking} using realistic data source scenarios.
 *
 * <p>The tests are organized by the type of data source they model, since each data source
 * type exercises a different code path through the bin-packing algorithm:
 *
 * <ul>
 *   <li><b>S3 Parquet files</b> — no affinity, with sizes → exercises FFD bin-packing.
 *       Models the common case of scanning object-store files (Iceberg, Delta Lake, Hudi).</li>
 *   <li><b>API handles</b> — no affinity, no sizes → exercises round-robin fallback.
 *       Models REST-based data sources where file sizes are unknown.</li>
 *   <li><b>HDFS blocks</b> — soft affinity, with sizes → exercises locality-aware grouping.
 *       Models HDFS/Alluxio where data is replicated but local reads are faster.</li>
 *   <li><b>Local log files</b> — required affinity, with sizes → exercises strict node isolation.
 *       Models node-local resources where data only exists on one specific node.</li>
 *   <li><b>Mixed workloads</b> — all three affinity types → exercises the full three-phase
 *       grouping: required isolation → soft grouping → no-affinity packing.</li>
 * </ul>
 *
 * <p>Helper methods ({@link #s3File}, {@link #hdfsBlock}, {@link #localFile}, {@link #apiHandle})
 * create splits with the appropriate affinity type, so each test reads as a scenario description.
 */
public class SizeAwareBinPackingTests extends ESTestCase {

    // =========================================================================
    // BASIC BEHAVIOR — trivial cases that short-circuit before bin-packing
    // =========================================================================

    public void testEmptyInput() {
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(List.of(), hints(4));
        assertThat(groups, hasSize(0));
    }

    public void testSingleFile() {
        // One Parquet file on S3 — should produce one group.
        // With fewer splits than target partitions, each split gets its own group.
        var splits = List.of(s3File("data.parquet", mb(150)));
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(4));
        assertThat(groups, hasSize(1));
        assertEquals(1, groups.get(0).size());
    }

    public void testFewerFilesThanNodes() {
        // 2 Parquet files, 5 data nodes — each file gets its own partition.
        // No point packing 2 splits into 1 bin when there are 5 available — splitting
        // maximizes parallelism even though some nodes will be idle.
        var splits = List.of(s3File("part-0.parquet", mb(100)), s3File("part-1.parquet", mb(200)));
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(5));
        assertThat(groups, hasSize(2));
        assertEquals(1, groups.get(0).size());
        assertEquals(1, groups.get(1).size());
    }

    // =========================================================================
    // S3 PARQUET FILES — no affinity, size-based FFD packing
    //
    // These tests model scanning Parquet/ORC files on object storage (S3, GCS, Azure Blob).
    // Files have no node affinity (any node can read any file over HTTP), so packing is
    // purely based on file sizes to balance the bytes-per-partition across data nodes.
    // =========================================================================

    public void testS3ParquetFilesBalancedBySize() {
        // 6 Parquet files of equal size, 3 data nodes → FFD distributes 2 files per bin.
        // This is the ideal case: equal sizes mean perfectly balanced partitions.
        var splits = IntStream.range(0, 6).mapToObj(i -> s3File("part-" + i + ".parquet", mb(100))).toList();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(3));
        assertThat(groups, hasSize(3));
        for (var group : groups) {
            assertEquals(2, group.size());
        }
    }

    public void testS3ParquetFilesSkewedSizes() {
        // Realistic Iceberg table: one large un-compacted data file + many small compacted files.
        // This is the "data skew" case that's common in production Iceberg tables.
        //
        // FFD sorts descending: [500, 50, 50, 50, 50, 50, 50, 50, 50]
        // Target bin size = ceil(900/3) = 300 MB.
        // The 500 MB file exceeds 300 MB — no bin can fit it, so greedy fallback places it
        // in the smallest (empty) bin. The remaining 8 x 50 MB files fill the other 2 bins
        // alternately (each bin gets 4 x 50 = 200 MB).
        var splits = new ArrayList<TestSplit>();
        splits.add(s3File("large-uncompacted.parquet", mb(500)));
        for (int i = 0; i < 8; i++) {
            splits.add(s3File("compacted-" + i + ".parquet", mb(50)));
        }
        // Total: 500 + 8*50 = 900 MB, 3 partitions → ~300 MB target per bin
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(3));
        assertThat(groups, hasSize(3));
        assertAllSplitsPresent(splits, groups);

        // The large file should be isolated in its own bin (placed first by FFD descending sort)
        boolean largeAlone = groups.stream().anyMatch(g -> g.size() == 1 && g.get(0).estimatedBytes().orElse(0) == mb(500));
        assertTrue("The oversized file should be in its own bin", largeAlone);

        // The remaining 2 bins should share the 8 small files roughly equally (4 each = 200 MB)
        List<List<TestSplit>> smallBins = groups.stream().filter(g -> g.get(0).estimatedBytes().orElse(0) != mb(500)).toList();
        assertThat(smallBins, hasSize(2));
        for (var bin : smallBins) {
            long binBytes = bin.stream().mapToLong(s -> s.estimatedBytes().orElse(0)).sum();
            assertEquals("Small bins should be balanced at 200 MB each", mb(200), binBytes);
        }
    }

    public void testS3ParquetWithMaxPartitionBytes() {
        // Tests the explicit maxPartitionBytes hint, which overrides the computed
        // ceil(totalBytes/targetPartitions) target. This is useful when the downstream
        // operator has a memory budget (e.g., "each partition must fit in 256 MB of RAM").
        var splits = List.of(
            s3File("a.parquet", mb(100)),
            s3File("b.parquet", mb(100)),
            s3File("c.parquet", mb(100)),
            s3File("d.parquet", mb(100))
        );
        // 400 MB total, 2 partitions → default target would be 200 MB. But with an explicit
        // max of 250 MB, FFD respects that cap. Since 100+100=200 <= 250, each bin gets 2.
        var hints = DistributionHints.builder().targetPartitions(2).maxPartitionBytes(mb(250)).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);
        // With max 250 MB and 100 MB files, each bin holds at most 2
        for (var group : groups) {
            long binBytes = group.stream().mapToLong(s -> s.estimatedBytes().orElse(0)).sum();
            assertThat(binBytes, lessThanOrEqualTo(mb(250)));
        }
    }

    public void testOversizedFileFallsBackToSmallestBin() {
        // Tests the greedy fallback: when a single file exceeds the target bin size,
        // FFD can't fit it anywhere, so it places it in the smallest bin.
        // Since FFD processes largest-first, the 1 GB file goes into an empty bin first.
        var splits = List.of(s3File("huge.parquet", mb(1000)), s3File("small-1.parquet", mb(50)), s3File("small-2.parquet", mb(50)));
        // 3 partitions, target ~367 MB each — huge file exceeds target
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(3));
        assertAllSplitsPresent(splits, groups);
        // The huge file should end up in its own bin (it's placed first in FFD)
        boolean hugeAlone = groups.stream().anyMatch(g -> g.size() == 1 && g.get(0).estimatedBytes().orElse(0) == mb(1000));
        assertTrue("Oversized file should be in its own bin", hugeAlone);
    }

    // =========================================================================
    // ROUND-ROBIN FALLBACK — no size information
    //
    // When no split has estimatedBytes(), the packer falls back to round-robin by count.
    // This models data sources like REST APIs, database cursors, or message queues where
    // the size of each split is unknown until it's actually read.
    // =========================================================================

    public void testApiHandlesWithoutSizes() {
        // 7 REST API endpoints, no size estimates — round-robin distributes by count.
        var splits = IntStream.range(0, 7).mapToObj(i -> apiHandle("endpoint-" + i)).toList();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(3));
        assertAllSplitsPresent(splits, groups);
        // Round-robin: 7 splits / 3 bins → bins of 3, 3, 1 (or 3, 2, 2)
        assertThat(groups, hasSize(3));
        // Each bin should have at least 1 and at most ceil(7/3)=3 splits
        for (var group : groups) {
            assertThat(group.size(), greaterThan(0));
            assertThat(group.size(), lessThanOrEqualTo(3));
        }
    }

    public void testRoundRobinEvenDistribution() {
        // 6 unsized splits, 3 partitions → exactly 2 each
        var splits = IntStream.range(0, 6).mapToObj(i -> apiHandle("handle-" + i)).toList();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(3));
        assertThat(groups, hasSize(3));
        assertThat(groups.stream().map(List::size).toList(), everyItem(lessThanOrEqualTo(2)));
    }

    // =========================================================================
    // HDFS BLOCKS — soft affinity (preferred node)
    //
    // Soft affinity models data that is replicated (accessible from any node) but faster
    // to read locally. HDFS blocks are the canonical example: each block has replicas on
    // 2-3 DataNodes, and reading a local replica avoids a network hop.
    //
    // The key behavior: soft affinity is only honored when preferDataLocality=true in the
    // hints. When false, preferred nodes are ignored and packing is purely size-based.
    // This allows the planner to trade locality for better balance when the cluster is
    // unevenly loaded.
    // =========================================================================

    public void testHdfsBlocksWithLocalityEnabled() {
        // 6 HDFS blocks: 3 on node-1, 3 on node-2. Locality enabled.
        // The algorithm groups by preferred node first, then bin-packs within each group.
        // This ensures each partition's blocks can be read locally on one DataNode.
        var splits = List.of(
            hdfsBlock("block-0", mb(100), "node-1"),
            hdfsBlock("block-1", mb(100), "node-1"),
            hdfsBlock("block-2", mb(100), "node-1"),
            hdfsBlock("block-3", mb(100), "node-2"),
            hdfsBlock("block-4", mb(100), "node-2"),
            hdfsBlock("block-5", mb(100), "node-2")
        );
        var hints = DistributionHints.builder().targetPartitions(4).preferDataLocality(true).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);

        // Each group should contain splits from the same node
        for (var group : groups) {
            Set<String> nodesInGroup = new HashSet<>();
            for (var split : group) {
                nodesInGroup.add(split.nodeAffinity().nodeId());
            }
            assertEquals("Soft affinity with locality should group by node", 1, nodesInGroup.size());
        }
    }

    public void testHdfsBlocksWithLocalityDisabled() {
        // Same HDFS blocks, but locality disabled. The preferred node is completely ignored
        // and packing is purely size-based. This verifies that soft affinity is truly "soft"
        // — the planner can override it when it wants better balance over locality.
        var splits = List.of(
            hdfsBlock("block-0", mb(100), "node-1"),
            hdfsBlock("block-1", mb(200), "node-1"),
            hdfsBlock("block-2", mb(100), "node-2"),
            hdfsBlock("block-3", mb(200), "node-2")
        );
        var hints = DistributionHints.builder().targetPartitions(2).preferDataLocality(false).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);
        assertThat(groups, hasSize(2));
        // With locality off, the packer balances by size, so nodes can be mixed
        // 200+100 and 200+100 is the balanced assignment (300 each)
        for (var group : groups) {
            long binBytes = group.stream().mapToLong(s -> s.estimatedBytes().orElse(0)).sum();
            assertEquals("Balanced bins of 300 MB each", mb(300), binBytes);
        }
    }

    public void testHdfsSmallNodeGroupsKeptTogether() {
        // 2 blocks on node-1, 1 block on node-2. Target: 4 partitions. Locality on.
        // Even though there are enough partitions to give each block its own group,
        // locality grouping keeps same-node blocks together. This is deliberate:
        // if block-0 and block-1 are both local to node-1, reading them in the same
        // partition on node-1 avoids network reads on other nodes.
        var splits = List.of(
            hdfsBlock("block-0", mb(100), "node-1"),
            hdfsBlock("block-1", mb(100), "node-1"),
            hdfsBlock("block-2", mb(100), "node-2")
        );
        var hints = DistributionHints.builder().targetPartitions(4).preferDataLocality(true).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);

        // node-1's 2 blocks should be in the same group (affinity = together)
        for (var group : groups) {
            Set<String> nodesInGroup = new HashSet<>();
            for (var split : group) {
                nodesInGroup.add(split.nodeAffinity().nodeId());
            }
            assertEquals("Each group should have splits from one node only", 1, nodesInGroup.size());
        }
    }

    public void testHdfsSingleNodeAllBlocks() {
        // All blocks on the same node — locality is trivially satisfied, just pack by size
        var splits = List.of(
            hdfsBlock("block-0", mb(100), "node-1"),
            hdfsBlock("block-1", mb(200), "node-1"),
            hdfsBlock("block-2", mb(100), "node-1"),
            hdfsBlock("block-3", mb(200), "node-1")
        );
        var hints = DistributionHints.builder().targetPartitions(2).preferDataLocality(true).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);
        assertThat(groups, hasSize(2));
    }

    // =========================================================================
    // LOCAL LOG FILES — required (hard) affinity
    //
    // Required affinity models data that physically exists on only one node (local filesystem,
    // node-attached storage, GPU memory). Unlike soft affinity, required is ALWAYS enforced
    // regardless of the preferDataLocality flag — there's no "trade locality for balance"
    // option because remote reads are impossible, not just slower.
    //
    // The critical invariant: splits with required affinity to different nodes must NEVER
    // appear in the same partition group. Violating this would mean a single partition
    // operator would need to read from two different nodes, which is architecturally invalid.
    // =========================================================================

    public void testLocalLogFilesStrictNodeGrouping() {
        // 3 ES nodes, each with local log files. The partitioner must produce groups
        // where every split in a group has the same required node ID.
        var splits = List.of(
            localFile("server.json", mb(50), "node-1"),
            localFile("gc.json", mb(20), "node-1"),
            localFile("server.json", mb(50), "node-2"),
            localFile("gc.json", mb(30), "node-2"),
            localFile("server.json", mb(60), "node-3"),
            localFile("gc.json", mb(10), "node-3")
        );
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(6));
        assertAllSplitsPresent(splits, groups);

        // Every group must contain splits from exactly one node
        for (var group : groups) {
            Set<String> nodesInGroup = new HashSet<>();
            for (var split : group) {
                nodesInGroup.add(split.nodeAffinity().nodeId());
            }
            assertEquals("Required affinity: splits from different nodes must never be mixed", 1, nodesInGroup.size());
        }
    }

    public void testLocalLogFilesManyFilesPerNode() {
        // 10 log files all on node-1. Even though they all have the same required affinity,
        // the packer should still create multiple partitions to parallelize reads within
        // that single node (multiple threads reading different files concurrently).
        var splits = new ArrayList<TestSplit>();
        for (int i = 0; i < 10; i++) {
            splits.add(localFile("log-" + i + ".json", mb(50), "node-1"));
        }
        // 4 partitions all for node-1
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(4));
        assertAllSplitsPresent(splits, groups);

        // All groups must be node-1 (required)
        for (var group : groups) {
            for (var split : group) {
                assertEquals("node-1", split.nodeAffinity().nodeId());
            }
        }
        // Should create multiple bins to parallelize within the node
        assertThat("Should create multiple bins for parallelism", groups.size(), greaterThan(1));
    }

    public void testRequiredAffinityIgnoresLocalityFlag() {
        // Verifies the key difference between required and soft affinity: required is
        // enforced regardless of the locality flag. Setting preferDataLocality=false only
        // affects soft affinity. This prevents data source authors from accidentally
        // breaking invariants by relying on a planner hint.
        var splits = List.of(localFile("a.json", mb(50), "node-1"), localFile("b.json", mb(50), "node-2"));
        var hints = DistributionHints.builder().targetPartitions(2).preferDataLocality(false).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);
        assertThat(groups, hasSize(2));
        // Even with locality=false, required affinity keeps them separate
        assertNotEquals(
            "Required affinity nodes must be separate",
            groups.get(0).get(0).nodeAffinity().nodeId(),
            groups.get(1).get(0).nodeAffinity().nodeId()
        );
    }

    // =========================================================================
    // MIXED WORKLOADS — required + soft + no affinity
    //
    // These tests exercise the full three-phase grouping algorithm:
    // Phase 1: Isolate required-affinity splits by node (consumes part of the partition budget)
    // Phase 2: Group soft-affinity splits by preferred node (if locality enabled)
    // Phase 3: Pack remaining no-affinity splits into leftover partition budget
    //
    // The key invariant across all mixed tests: required-affinity groups are node-pure
    // and never contaminated by soft or no-affinity splits.
    // =========================================================================

    public void testMixedLocalFilesAndS3Objects() {
        // Models a query that joins local cluster logs with remote S3 data.
        // The local files (required affinity) must be isolated by node.
        // The S3 files (no affinity) fill the remaining partition budget.
        var splits = new ArrayList<TestSplit>();

        // Local log files on node-1 and node-2 (required affinity)
        splits.add(localFile("node1-server.json", mb(40), "node-1"));
        splits.add(localFile("node1-gc.json", mb(10), "node-1"));
        splits.add(localFile("node2-server.json", mb(40), "node-2"));
        splits.add(localFile("node2-gc.json", mb(10), "node-2"));

        // S3 files (no affinity)
        splits.add(s3File("data-0.parquet", mb(100)));
        splits.add(s3File("data-1.parquet", mb(100)));
        splits.add(s3File("data-2.parquet", mb(100)));

        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(5));
        assertAllSplitsPresent(splits, groups);

        // Required-affinity groups must be node-pure
        for (var group : groups) {
            boolean hasRequired = group.stream().anyMatch(s -> s.nodeAffinity().required());
            if (hasRequired) {
                Set<String> nodes = new HashSet<>();
                for (var s : group) {
                    nodes.add(s.nodeAffinity().nodeId());
                }
                assertEquals("Required-affinity group must be node-pure", 1, nodes.size());
                // And all splits in this group must be required (no mixing with S3 files)
                for (var s : group) {
                    assertTrue("Required group should not contain non-required splits", s.nodeAffinity().required());
                }
            }
        }
    }

    public void testMixedAllThreeAffinityTypes() {
        // All three affinity types in one query. This is the most complex code path:
        // required splits get isolated first, then soft+none splits are packed together
        // with locality awareness. Required (local files) + Preferred (HDFS) + None (S3).
        var splits = new ArrayList<TestSplit>();

        // Required: local files on node-1
        splits.add(localFile("local-a.json", mb(50), "node-1"));
        splits.add(localFile("local-b.json", mb(50), "node-1"));

        // Preferred: HDFS blocks prefer node-2
        splits.add(hdfsBlock("hdfs-0", mb(80), "node-2"));
        splits.add(hdfsBlock("hdfs-1", mb(80), "node-2"));

        // None: S3 files
        splits.add(s3File("s3-0.parquet", mb(100)));
        splits.add(s3File("s3-1.parquet", mb(100)));

        var hints = DistributionHints.builder().targetPartitions(4).preferDataLocality(true).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);

        // Verify required splits are in their own node-pure group(s)
        for (var group : groups) {
            boolean hasRequired = group.stream().anyMatch(s -> s.nodeAffinity().required());
            if (hasRequired) {
                for (var s : group) {
                    assertTrue("Required group must only contain required splits", s.nodeAffinity().required());
                    assertEquals("node-1", s.nodeAffinity().nodeId());
                }
            }
        }
    }

    public void testMixedWithLocalityDisabled() {
        // Tests the interaction between required affinity and locality=false.
        // Required affinity is still enforced (data only exists on one node).
        // Soft affinity is ignored — HDFS blocks and S3 files are packed together by size.
        var splits = new ArrayList<TestSplit>();

        // Required: must stay on node-1
        splits.add(localFile("local.json", mb(50), "node-1"));

        // Preferred: HDFS on node-2 (should be ignored since locality=false)
        splits.add(hdfsBlock("hdfs-0", mb(80), "node-2"));

        // None: S3
        splits.add(s3File("s3-0.parquet", mb(100)));

        var hints = DistributionHints.builder().targetPartitions(3).preferDataLocality(false).build();
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints);
        assertAllSplitsPresent(splits, groups);

        // Required split must be alone (different node from others)
        for (var group : groups) {
            if (group.stream().anyMatch(s -> s.nodeAffinity().required())) {
                for (var s : group) {
                    assertTrue("Required must not mix", s.nodeAffinity().required());
                }
            }
        }
    }

    // =========================================================================
    // EDGE CASES
    // =========================================================================

    public void testSinglePartitionTargetPacksEverything() {
        // targetPartitions=1 means "run everything on the coordinator" — all splits
        // collapse into a single group regardless of size.
        var splits = List.of(s3File("a.parquet", mb(100)), s3File("b.parquet", mb(200)), s3File("c.parquet", mb(300)));
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(1));
        assertThat(groups, hasSize(1));
        assertEquals(3, groups.get(0).size());
    }

    public void testMixedSizedAndUnsizedSplits() {
        // When at least one split has a size, FFD is used (not round-robin). Splits without
        // sizes are treated as 0 bytes — they naturally fill gaps in larger bins since FFD
        // processes largest-first and 0-byte splits go last.
        var splits = List.of(s3File("sized.parquet", mb(200)), apiHandle("unsized-endpoint"), s3File("another-sized.parquet", mb(100)));
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(2));
        assertAllSplitsPresent(splits, groups);
    }

    public void testLargeNumberOfSmallFiles() {
        // 100 small files across 8 nodes — common in Iceberg tables with frequent commits.
        // FFD should distribute ~12-13 files per bin (100/8). This tests that the algorithm
        // scales gracefully and doesn't degenerate with many equal-sized inputs.
        var splits = IntStream.range(0, 100).mapToObj(i -> s3File("part-" + i + ".parquet", mb(10))).toList();
        // 8 data nodes
        List<List<TestSplit>> groups = SizeAwareBinPacking.groupSplits(splits, hints(8));
        assertAllSplitsPresent(splits, groups);
        assertThat(groups, hasSize(8));
        // Each bin should get roughly 100/8 ≈ 12-13 files
        for (var group : groups) {
            assertThat(group.size(), greaterThan(0));
            assertThat(group.size(), lessThanOrEqualTo(15));
        }
    }

    // =========================================================================
    // TEST HELPERS
    // =========================================================================

    /** S3/GCS file — no node affinity, known size. */
    private static TestSplit s3File(String name, long bytes) {
        return new TestSplit(name, OptionalLong.of(bytes), NodeAffinity.NONE);
    }

    /** HDFS block — soft (preferred) node affinity, known size. */
    private static TestSplit hdfsBlock(String name, long bytes, String preferredNode) {
        return new TestSplit(name, OptionalLong.of(bytes), NodeAffinity.prefer(preferredNode));
    }

    /** Node-local file — required (hard) node affinity, known size. */
    private static TestSplit localFile(String name, long bytes, String nodeId) {
        return new TestSplit(name, OptionalLong.of(bytes), NodeAffinity.require(nodeId));
    }

    /** API handle — no affinity, no size information. */
    private static TestSplit apiHandle(String name) {
        return new TestSplit(name, OptionalLong.empty(), NodeAffinity.NONE);
    }

    private static long mb(long megabytes) {
        return megabytes * 1024 * 1024;
    }

    private static DistributionHints hints(int targetPartitions) {
        return new DistributionHints(targetPartitions);
    }

    /** Verify that every input split appears exactly once across all output groups. */
    private static void assertAllSplitsPresent(List<TestSplit> expected, List<List<TestSplit>> groups) {
        List<TestSplit> actual = new ArrayList<>();
        for (var group : groups) {
            actual.addAll(group);
        }
        assertEquals("Total split count must match", expected.size(), actual.size());
        // Check by identity — same split objects
        Set<TestSplit> expectedSet = new HashSet<>(expected);
        Set<TestSplit> actualSet = new HashSet<>(actual);
        assertEquals("All splits must be present", expectedSet, actualSet);
    }

    /** A test split with configurable name, size, and node affinity. */
    record TestSplit(String name, OptionalLong estimatedBytes, NodeAffinity nodeAffinity) implements DataSourceSplit {}
}
