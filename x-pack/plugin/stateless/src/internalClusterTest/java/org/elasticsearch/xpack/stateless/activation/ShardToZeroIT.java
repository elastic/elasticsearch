/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.activation;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.engine.HollowShardsMetrics;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Measurement experiment (not a feature test): how close does a stateless shard get to zero heap / zero local disk when the
 * search tier goes to 0 replicas and the indexing shard is hollowed, and what does bringing it back cost.
 * All measurements are printed with the prefix {@code Z2|} to the test log.
 */
public class ShardToZeroIT extends AbstractStatelessPluginIntegTestCase {

    private static final int ROUNDS = 4;
    private static final int DOCS_PER_ROUND = 5000;
    private static final int NUM_DOCS = ROUNDS * DOCS_PER_ROUND;
    private static final org.apache.logging.log4j.Logger LOG = org.apache.logging.log4j.LogManager.getLogger(ShardToZeroIT.class);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1))

            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // so that the hollow/unhollow time metrics are not rounded to the cached clock
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0);
    }

    public void testOneShardToZeroAndBack() throws Exception {
        runScenario(1);
    }

    public void testFiveShardsToZeroAndBack() throws Exception {
        runScenario(5);
    }

    private void runScenario(int numberOfShards) throws Exception {
        final String tag = "shards=" + numberOfShards;
        final String master = startMasterOnlyNode();
        // pin the shared cache geometry on data nodes (tests otherwise randomize it per node); 4MiB regions, 64MiB per node
        final Settings cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "4mb")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "64mb")
            .build();
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(cacheSettings)
            .build();
        final String indexNodeA = startIndexNode(indexNodeSettings);
        final String indexNodeB = startIndexNode(indexNodeSettings);
        final String searchNode = startSearchNode(cacheSettings);
        ensureStableCluster(4);
        final List<String> nodes = List.of(master, indexNodeA, indexNodeB, searchNode);
        final Map<String, String> tiers = Map.of(master, "master", indexNodeA, "index-A", indexNodeB, "index-B", searchNode, "search");

        final String indexName = "cheap-" + numberOfShards;
        createIndex(
            indexName,
            indexSettings(numberOfShards, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeB).build()
        );
        ensureGreen(indexName);
        final Index index = resolveIndex(indexName);
        log("%s index uuid=%s", tag, index.getUUID());
        Settings sn = internalCluster().getInstance(ClusterService.class, searchNode).getSettings();
        log(
            "%s randomized settings on search node: offlineWarming=%s replicatedContentForSearch=%s uploadMaxCommits=%s",
            tag,
            org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.get(sn),
            sn.get("stateless.search.use_internal_files_replicated_content"),
            getUploadMaxCommits()
        );

        for (int r = 0; r < ROUNDS; r++) {
            indexDocs(
                indexName,
                DOCS_PER_ROUND,
                b -> b,
                null,
                () -> Map.of(
                    "text",
                    randomAlphaOfLength(200),
                    "kw",
                    randomAlphaOfLength(8),
                    "num",
                    randomLong(),
                    "ts",
                    System.currentTimeMillis()
                )
            );
            flush(indexName);
        }
        refresh(indexName);
        assertHitCount(indexName, NUM_DOCS);

        // ---------------- Phase 1: baseline ----------------
        record(tag, "1-baseline", nodes, tiers, index, numberOfShards, true);

        // ---------------- Phase 2: search tier to zero ----------------
        final long t2 = System.nanoTime();
        setReplicaCount(0, indexName);
        assertBusy(() -> assertThat(indexService(searchNode, index), nullValue()));
        log("%s phase2 search IndexService gone after %d ms", tag, nanosToMillis(System.nanoTime() - t2));
        record(tag, "2a-replicas0-immediate", nodes, tiers, index, numberOfShards, false);
        // Give IndicesStore another routing-table change: create and delete an unrelated index.
        triggerRoutingChange();
        record(tag, "2b-replicas0-after-routing-change", nodes, tiers, index, numberOfShards, false);

        // ---------------- Phase 5 (while replicas=0): what does a search do? ----------------
        try {
            SearchResponse response = client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true).get();
            try {
                log(
                    "%s phase5 search with replicas=0 returned: total=%d successful=%d failed=%d skipped=%d hits=%s failures=%s",
                    tag,
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getFailedShards(),
                    response.getSkippedShards(),
                    response.getHits().getTotalHits(),
                    Stream.of(response.getShardFailures()).map(f -> f.getCause().getClass().getSimpleName() + ":" + f.reason()).toList()
                );
            } finally {
                response.decRef();
            }
        } catch (Exception e) {
            log(
                "%s phase5 search with replicas=0 threw %s: %s (root %s)",
                tag,
                e.getClass().getName(),
                e.getMessage(),
                org.elasticsearch.ExceptionsHelper.unwrapCause(e).getClass().getName()
            );
        }
        try {
            var get = client().prepareGet(indexName, "does-not-matter").get();
            log("%s phase5 realtime GET with replicas=0 returned exists=%s", tag, get.isExists());
        } catch (Exception e) {
            log("%s phase5 realtime GET with replicas=0 threw %s: %s", tag, e.getClass().getName(), e.getMessage());
        }

        // ---------------- Phase 3: hollow the indexing tier (relocate A -> B) ----------------
        final long t3 = System.nanoTime();
        hollowShards(indexName, numberOfShards, indexNodeA, indexNodeB);
        log("%s phase3 relocation+hollow took %d ms", tag, nanosToMillis(System.nanoTime() - t3));
        for (int i = 0; i < numberOfShards; i++) {
            assertThat(findIndexShard(index, i).getEngineOrNull(), instanceOf(HollowIndexEngine.class));
        }
        record(tag, "3a-hollow-immediate", nodes, tiers, index, numberOfShards, true);
        triggerRoutingChange();
        record(tag, "3b-hollow-after-routing-change", nodes, tiers, index, numberOfShards, true);

        // ---------------- Phase 4: reactivate the search tier ----------------
        // Simulate true zero on the search node: drop whatever demoted cache regions of this index remain from phase 1.
        var searchCache = cacheService(searchNode);
        int evicted = searchCache.forceEvict(k -> k.shardId().getIndex().equals(index));
        log("%s phase4 force-evicted %d leftover cache regions on the search node before reactivation", tag, evicted);

        final Map<String, ObjectStoreCounter> counters = new HashMap<>();
        for (String node : nodes) {
            var c = new ObjectStoreCounter();
            counters.put(node, c);
            setNodeRepositoryStrategy(node, c);
        }
        final var shardTimes = new ConcurrentHashMap<String, Long>();
        final ClusterService searchClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
        final ClusterStateListener listener = event -> {
            var table = event.state().routingTable(org.elasticsearch.cluster.metadata.ProjectId.DEFAULT).index(index);
            if (table == null) {
                return;
            }
            long now = System.nanoTime();
            for (int i = 0; i < table.size(); i++) {
                IndexShardRoutingTable shardTable = table.shard(i);
                for (int c = 0; c < shardTable.size(); c++) {
                    ShardRouting sr = shardTable.shard(c);
                    if (sr.isSearchable()) {
                        shardTimes.putIfAbsent(i + "-" + sr.state(), now);
                    }
                }
            }
        };
        searchClusterService.addListener(listener);
        final long t4 = System.nanoTime();
        setReplicaCount(1, indexName);
        final long tAck = System.nanoTime();
        assertBusy(() -> {
            for (int i = 0; i < numberOfShards; i++) {
                assertNotNull(shardTimes.get(i + "-STARTED"));
            }
        });
        final long tStarted = System.nanoTime();
        searchClusterService.removeListener(listener);
        Map<String, Long> afterStartCounts = snapshot(counters);
        // first search
        long firstSearchStart = System.nanoTime();
        long hits = searchHits(indexName);
        long firstSearchMs = nanosToMillis(System.nanoTime() - firstSearchStart);
        long secondSearchStart = System.nanoTime();
        long hits2 = searchHits(indexName);
        long secondSearchMs = nanosToMillis(System.nanoTime() - secondSearchStart);
        assertThat(hits, equalTo((long) NUM_DOCS));
        assertThat(hits2, equalTo((long) NUM_DOCS));
        log("%s phase4 settings-update-ack=%d ms", tag, nanosToMillis(tAck - t4));
        for (int i = 0; i < numberOfShards; i++) {
            Long init = shardTimes.get(i + "-INITIALIZING");
            Long started = shardTimes.get(i + "-STARTED");
            log(
                "%s phase4 shard %d: INITIALIZING applied on search node at +%s ms, STARTED applied at +%s ms",
                tag,
                i,
                init == null ? "n/a" : nanosToMillis(init - t4),
                started == null ? "n/a" : nanosToMillis(started - t4)
            );
            IndexShard ss = findSearchShard(index, i);
            var rs = ss.recoveryState();
            log(
                "%s phase4 shard %d recoveryState: stage=%s total=%d ms index=%d ms verify=%d ms translog=%d ms; json=%s",
                tag,
                i,
                rs.getStage(),
                rs.getTimer().time(),
                rs.getIndex().time(),
                rs.getVerifyIndex().time(),
                rs.getTranslog().time(),
                Strings.toString(rs)
            );
        }
        log(
            "%s phase4 all search shards STARTED (observed) at +%d ms; first search (cold) %d ms -> %d hits; total to first hit +%d ms;"
                + " second search %d ms",
            tag,
            nanosToMillis(tStarted - t4),
            firstSearchMs,
            hits,
            nanosToMillis(System.nanoTime() - t4) - secondSearchMs,
            secondSearchMs
        );
        Map<String, Long> afterSearchCounts = snapshot(counters);
        for (String node : nodes) {
            log("%s phase4 object-store (to STARTED) node=%s %s", tag, tiers.get(node), filterNode(afterStartCounts, node));
            log("%s phase4 object-store (to STARTED + 2 searches) node=%s %s", tag, tiers.get(node), filterNode(afterSearchCounts, node));
        }
        record(tag, "4-reactivated", nodes, tiers, index, numberOfShards, true);

        // ---------------- Phase 4b: write one doc -> unhollow ----------------
        for (var c : counters.values()) {
            c.reset();
        }
        var telemetry = getTelemetryPlugin(indexNodeB);
        long tw = System.nanoTime();
        client().prepareIndex(indexName).setId("wake-up").setSource("field", "value").get();
        long writeMs = nanosToMillis(System.nanoTime() - tw);
        List<Measurement> unhollowTimes = telemetry.getLongHistogramMeasurement(HollowShardsMetrics.UNHOLLOW_TIME_MILLIS);
        log(
            "%s phase4b first write took %d ms; unhollow metric samples=%s",
            tag,
            writeMs,
            unhollowTimes.stream().map(Measurement::getLong).toList()
        );
        for (String node : nodes) {
            log("%s phase4b object-store (first write) node=%s %s", tag, tiers.get(node), counters.get(node).summary());
        }
        long tw2 = System.nanoTime();
        client().prepareIndex(indexName).setId("second-write").setSource("field", "value").get();
        log("%s phase4b second write (shard already unhollowed or other shard) took %d ms", tag, nanosToMillis(System.nanoTime() - tw2));
        int hollowLeft = 0;
        for (int i = 0; i < numberOfShards; i++) {
            if (findIndexShard(index, i).getEngineOrNull() instanceof HollowIndexEngine) {
                hollowLeft++;
            }
        }
        log("%s phase4b shards still hollow after one write: %d of %d", tag, hollowLeft, numberOfShards);
        record(tag, "4b-after-unhollow", nodes, tiers, index, numberOfShards, true);

        for (String node : nodes) {
            setNodeRepositoryStrategy(node, StatelessMockRepositoryStrategy.DEFAULT);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // measurement helpers
    // ------------------------------------------------------------------------------------------------------------------

    private static void log(String format, Object... args) {
        LOG.info("Z2| " + String.format(Locale.ROOT, format, args));
    }

    private static long nanosToMillis(long nanos) {
        return TimeValue.nsecToMSec(nanos);
    }

    private static void assertHitCount(String indexName, long expected) {
        assertThat(searchHits(indexName), equalTo(expected));
    }

    private static long searchHits(String indexName) {
        var response = client().prepareSearch(indexName).setSize(1).setTrackTotalHits(true).get();
        try {
            return response.getHits().getTotalHits().value();
        } finally {
            response.decRef();
        }
    }

    private void triggerRoutingChange() throws Exception {
        String other = "routing-poke-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        createIndex(other, indexSettings(1, 0).build());
        ensureGreen(other);
        org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked(indicesAdmin().prepareDelete(other));
        // make sure every node applied the latest state
        final long version = internalCluster().clusterService(internalCluster().getMasterName()).state().version();
        assertBusy(() -> {
            for (ClusterService cs : internalCluster().getInstances(ClusterService.class)) {
                assertTrue(cs.state().version() >= version);
            }
        });
    }

    private static IndexService indexService(String node, Index index) {
        return internalCluster().getInstance(IndicesService.class, node).indexService(index);
    }

    private static StatelessSharedBlobCacheService cacheService(String node) {
        return internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(StatelessPlugin.class)
            .findFirst()
            .orElseThrow()
            .getStatelessSharedBlobCacheService();
    }

    private void record(
        String tag,
        String phase,
        List<String> nodes,
        Map<String, String> tiers,
        Index index,
        int numberOfShards,
        boolean measureHeap
    ) throws IOException {
        for (String node : nodes) {
            String tier = tiers.get(node);
            // ---- disk
            NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, node);
            long totalBytes = 0;
            int files = 0;
            int dirs = 0;
            List<String> listing = new ArrayList<>();
            for (Path dataPath : env.nodeDataPaths()) {
                Path indexDir = dataPath.resolve("indices").resolve(index.getUUID());
                if (Files.exists(indexDir) == false) {
                    continue;
                }
                long[] totals = new long[3];
                walkTree(indexDir, indexDir, listing, totals);
                files += (int) totals[0];
                dirs += (int) totals[1];
                totalBytes += totals[2];
            }
            // ---- services
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(index);
            List<String> shardDescr = new ArrayList<>();
            if (indexService != null) {
                for (IndexShard shard : indexService) {
                    var engine = shard.getEngineOrNull();
                    shardDescr.add(
                        shard.shardId().id()
                            + ":"
                            + shard.routingEntry().role()
                            + ":"
                            + shard.state()
                            + ":"
                            + (engine == null ? "null" : engine.getClass().getSimpleName())
                    );
                }
            }
            // ---- cache regions
            Map<Integer, Integer> regionsByFreq = Map.of();
            long regions = 0;
            int regionSize = 0;
            if (env.nodeDataPaths().length > 0 && hasDataRole(node)) {
                var cache = cacheService(node);
                regionSize = cache.getRegionSize();
                Predicate<FileCacheKey> pred = k -> k.shardId().getIndex().equals(index);
                regions = cache.countCachedRegions(pred);
                regionsByFreq = new TreeMap<>(SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cache, pred));
            }
            // ---- per-ShardId node maps
            String nodeMaps = hasDataRole(node) ? nodeMapResidue(node, index) : "n/a";
            log(
                "%s %s node=%s tier=%s disk: files=%d dirs=%d bytes=%d | IndexService=%s shards=%s | cacheRegions=%d (regionSize=%d)"
                    + " byFreq=%s | nodeMaps=%s",
                tag,
                phase,
                node,
                tier,
                files,
                dirs,
                totalBytes,
                indexService != null,
                shardDescr,
                regions,
                regionSize,
                regionsByFreq,
                nodeMaps
            );
            log("%s %s node=%s tier=%s listing=%s", tag, phase, node, tier, listing);
            // ---- heap
            if (measureHeap && indexService != null) {
                try {
                    measureHeap(tag, phase, node, tier, indexService);
                } catch (Throwable t) {
                    log("%s %s node=%s tier=%s heap measurement failed: %s", tag, phase, node, tier, t);
                }
            }
        }
    }

    /** Walks a directory tolerating concurrent deletion (IndicesStore may delete while we look). totals = files, dirs, bytes. */
    private static void walkTree(Path root, Path p, List<String> listing, long[] totals) {
        try {
            if (Files.isDirectory(p)) {
                totals[1]++;
                listing.add(root.relativize(p) + "/");
                List<Path> children;
                try (Stream<Path> s = Files.list(p)) {
                    children = s.sorted().toList();
                }
                for (Path c : children) {
                    walkTree(root, c, listing, totals);
                }
            } else {
                long size = Files.size(p);
                totals[0]++;
                totals[2] += size;
                listing.add(root.relativize(p) + "(" + size + ")");
            }
        } catch (IOException | java.io.UncheckedIOException e) {
            listing.add(root.relativize(p) + "(vanished)");
        }
    }

    private static boolean hasDataRole(String node) {
        var dn = internalCluster().clusterService(node).localNode();
        return dn.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) || dn.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName());
    }

    /** Counts per-ShardId entries in node-level stateless maps (by reflection, test only). */
    private static String nodeMapResidue(String node, Index index) {
        StringBuilder sb = new StringBuilder();
        try {
            var commitService = internalCluster().getInstance(StatelessCommitService.class, node);
            sb.append("commitStates=").append(countKeys(commitService, StatelessCommitService.class, "shardsCommitsStates", index));
        } catch (Exception e) {
            sb.append("commitStates=?(").append(e.getClass().getSimpleName()).append(")");
        }
        try {
            var tr = getTranslogReplicator(node);
            sb.append(" translogSyncStates=").append(countKeys(tr, TranslogReplicator.class, "shardSyncStates", index));
        } catch (Exception e) {
            sb.append(" translogSyncStates=?(").append(e.getClass().getSimpleName()).append(")");
        }
        try {
            var hs = internalCluster().getInstance(HollowShardsService.class, node);
            sb.append(" hollowShards=").append(countKeys(hs, HollowShardsService.class, "hollowShards", index));
        } catch (Exception e) {
            sb.append(" hollowShards=?(").append(e.getClass().getSimpleName()).append(")");
        }
        return sb.toString();
    }

    private static long countKeys(Object target, Class<?> clazz, String fieldName, Index index) throws Exception {
        Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        Map<?, ?> map = (Map<?, ?>) f.get(target);
        return map.keySet().stream().filter(k -> k instanceof ShardId s && s.getIndex().equals(index)).count();
    }

    /**
     * Marginal heap: objects reachable from the root but (a) not past node-level services (see {@link ScopedAccumulator}) and
     * (b) not reachable from the node's IndicesService with all IndexServices cut off (node-wide registries, settings, features).
     * IndexShard is measured excluding everything reachable from its IndexService, and ShardCommitState excluding both.
     */
    private void measureHeap(String tag, String phase, String node, String tier, IndexService indexService) {
        Set<Object> shards = identitySet();
        for (IndexShard s : indexService) {
            shards.add(s);
        }
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        Set<Object> allIndexServices = identitySet();
        indicesService.forEach(allIndexServices::add);
        var accNode = new ScopedAccumulator(node, allIndexServices, null);
        long nodeBytes = RamUsageTester.ramUsed(indicesService, accNode);
        Set<Object> nodeShared = accNode.collected;
        log(
            "%s %s node=%s tier=%s heap node-baseline(IndicesService minus IndexServices)=%d bytes objects=%d",
            tag,
            phase,
            node,
            tier,
            nodeBytes,
            nodeShared.size()
        );

        Set<Object> excl = identitySet();
        excl.addAll(nodeShared);
        excl.addAll(shards);
        var accService = new ScopedAccumulator(node, excl, null);
        long serviceBytes = RamUsageTester.ramUsed(indexService, accService);
        log(
            "%s %s node=%s tier=%s heap IndexService(marginal, excl shards)=%d bytes %s",
            tag,
            phase,
            node,
            tier,
            serviceBytes,
            accService.report()
        );
        Set<Object> serviceReach = accService.collected;

        Set<Object> shardReach = identitySet();
        for (IndexShard s : indexService) {
            Set<Object> ex = identitySet();
            ex.addAll(nodeShared);
            ex.addAll(serviceReach);
            ex.add(indexService);
            ex.addAll(shards);
            ex.remove(s);
            var accShard = new ScopedAccumulator(node, ex, null);
            long shardBytes = RamUsageTester.ramUsed(s, accShard);
            shardReach.addAll(accShard.collected);
            log(
                "%s %s node=%s tier=%s heap IndexShard[%d %s engine=%s](marginal)=%d bytes %s",
                tag,
                phase,
                node,
                tier,
                s.shardId().id(),
                s.routingEntry().role(),
                s.getEngineOrNull() == null ? "null" : s.getEngineOrNull().getClass().getSimpleName(),
                shardBytes,
                accShard.report()
            );
        }
        // per-shard state in node-level stateless services (indexing tier)
        if (internalCluster().clusterService(node).localNode().hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName())) {
            try {
                var commitService = internalCluster().getInstance(StatelessCommitService.class, node);
                Field f = StatelessCommitService.class.getDeclaredField("shardsCommitsStates");
                f.setAccessible(true);
                Map<?, ?> map = (Map<?, ?>) f.get(commitService);
                for (var e : map.entrySet()) {
                    if (e.getKey() instanceof ShardId sid && sid.getIndex().equals(indexService.index())) {
                        Set<Object> ex = identitySet();
                        ex.addAll(nodeShared);
                        ex.addAll(serviceReach);
                        ex.addAll(shardReach);
                        ex.add(indexService);
                        ex.addAll(shards);
                        var acc = new ScopedAccumulator(node, ex, null);
                        long bytes = RamUsageTester.ramUsed(e.getValue(), acc);
                        log(
                            "%s %s node=%s tier=%s heap ShardCommitState[%d](marginal)=%d bytes %s",
                            tag,
                            phase,
                            node,
                            tier,
                            sid.id(),
                            bytes,
                            acc.report()
                        );
                    }
                }
            } catch (Exception e) {
                log("%s %s node=%s ShardCommitState heap failed: %s", tag, phase, node, e);
            }
        }
    }

    private static Set<Object> identitySet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * Accumulator that stops at node-level shared services so the measured number approximates the heap retained by one
     * index/shard. JDK-internal objects that are not collections/boxed primitives/strings are counted shallowly and not traversed
     * (their fields are not reflectively accessible).
     */
    private static final class ScopedAccumulator extends RamUsageTester.Accumulator {
        private static final List<Class<?>> EXCLUDED_TYPES = List.of(
            ThreadPool.class,
            ClusterService.class,
            ClusterState.class,
            IndicesService.class,
            Client.class,
            TransportService.class,
            NodeEnvironment.class,
            BigArrays.class,
            PageCacheRecycler.class,
            CircuitBreakerService.class,
            ScriptService.class,
            PluginsService.class,
            Plugin.class,
            SharedBlobCacheService.class,
            StatelessCommitService.class,
            ObjectStoreService.class,
            TranslogReplicator.class,
            HollowShardsService.class,
            ClusterSettings.class,
            NamedXContentRegistry.class,
            org.elasticsearch.common.io.stream.NamedWriteableRegistry.class,
            org.apache.logging.log4j.Logger.class,
            Thread.class,
            Executor.class,
            IndexMetadata.class,
            Repository.class,
            BlobStore.class,
            BlobContainer.class,
            ClassLoader.class,
            Class.class,
            org.elasticsearch.indices.IndicesQueryCache.class,
            org.elasticsearch.indices.IndicesRequestCache.class,
            org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache.class,
            org.elasticsearch.index.analysis.AnalysisRegistry.class,
            org.elasticsearch.index.mapper.MapperRegistry.class,
            org.elasticsearch.telemetry.metric.MeterRegistry.class,
            org.elasticsearch.telemetry.TelemetryProvider.class
        );
        private static final List<String> EXCLUDED_PREFIXES = List.of(
            "org.elasticsearch.threadpool.",
            "org.elasticsearch.transport.",
            "org.elasticsearch.cluster.service.",
            "org.elasticsearch.cluster.coordination.",
            "org.elasticsearch.cluster.routing.allocation.",
            "org.elasticsearch.node.",
            "org.elasticsearch.plugins.",
            "org.elasticsearch.telemetry.",
            "org.elasticsearch.tasks.",
            "org.elasticsearch.repositories.",
            "org.elasticsearch.client.",
            "org.elasticsearch.xpack.stateless.objectstore.",
            "org.apache.logging.",
            "co.elastic."
        );

        private final Set<Object> excludedIdentities;
        private final Set<Object> opaqueSeen = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Map<String, long[]> byClass = new HashMap<>();
        private final Map<String, Integer> excludedHits = new HashMap<>();
        private long opaqueBytes;
        private long objects;

        final Set<Object> collected = Collections.newSetFromMap(new IdentityHashMap<>());

        ScopedAccumulator(String node, Set<Object> excludedIdentities, Object unused) {
            this.excludedIdentities = excludedIdentities;
            this.excludedIdentities.add(internalCluster().clusterService(node).getSettings());
        }

        private boolean admit(Object v) {
            if (v == null || excludedIdentities.contains(v)) {
                return false;
            }
            for (Class<?> t : EXCLUDED_TYPES) {
                if (t.isInstance(v)) {
                    excludedHits.merge(t.getSimpleName(), 1, Integer::sum);
                    return false;
                }
            }
            Class<?> c = v.getClass();
            String n = c.getName();
            for (String p : EXCLUDED_PREFIXES) {
                if (n.startsWith(p)) {
                    excludedHits.merge(p, 1, Integer::sum);
                    return false;
                }
            }
            if (c.isArray() || v instanceof Enum<?>) {
                return true;
            }
            boolean jdk = n.startsWith("java.") || n.startsWith("javax.") || n.startsWith("jdk.") || n.startsWith("sun.");
            boolean ok;
            if (jdk) {
                // RamUsageTester special-cases java.* Maps/Iterables and simple types; everything else would need reflective access
                ok = (n.startsWith("java.") && (v instanceof Map || v instanceof Collection))
                    || v instanceof String
                    || v instanceof Long
                    || v instanceof Integer
                    || v instanceof Short
                    || v instanceof Byte
                    || v instanceof Double
                    || v instanceof Float
                    || v instanceof Boolean
                    || v instanceof Character;
            } else {
                ok = TRAVERSABLE.computeIfAbsent(c, ScopedAccumulator::fieldsAccessible);
            }
            if (ok == false) {
                if (opaqueSeen.add(v)) {
                    opaqueBytes += RamUsageEstimator.shallowSizeOf(v);
                }
                return false;
            }
            return true;
        }

        private static final Map<Class<?>, Boolean> TRAVERSABLE = new ConcurrentHashMap<>();

        /** True if all non-static reference fields in the hierarchy can be made accessible (e.g. not an ES class extending a JDK one). */
        private static boolean fieldsAccessible(Class<?> clazz) {
            for (Class<?> k = clazz; k != null && k != Object.class; k = k.getSuperclass()) {
                for (Field f : k.getDeclaredFields()) {
                    if (java.lang.reflect.Modifier.isStatic(f.getModifiers()) == false && f.getType().isPrimitive() == false) {
                        if (f.trySetAccessible() == false) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        private void account(Object o, long size) {
            objects++;
            collected.add(o);
            if (objects > 5_000_000L) {
                throw new IllegalStateException("object graph too large, scope leak");
            }
            String key = o.getClass().isHidden() ? "<lambda>" : o.getClass().getName();
            long[] v = byClass.computeIfAbsent(key, k -> new long[2]);
            v[0] += size;
            v[1]++;
        }

        @Override
        public long accumulateObject(Object o, long shallowSize, Map<Field, Object> fieldValues, Collection<Object> queue) {
            account(o, shallowSize);
            for (Object v : fieldValues.values()) {
                if (admit(v)) {
                    queue.add(v);
                }
            }
            return shallowSize;
        }

        @Override
        public long accumulateArray(Object array, long shallowSize, List<Object> values, Collection<Object> queue) {
            account(array, shallowSize);
            for (Object v : values) {
                if (admit(v)) {
                    queue.add(v);
                }
            }
            return shallowSize;
        }

        String report() {
            List<Map.Entry<String, long[]>> top = new ArrayList<>(byClass.entrySet());
            top.sort((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]));
            StringBuilder sb = new StringBuilder();
            sb.append("[objects=").append(objects).append(" opaqueShallowBytes(not traversed)=").append(opaqueBytes).append(" top=");
            for (int i = 0; i < Math.min(15, top.size()); i++) {
                var e = top.get(i);
                sb.append(e.getKey()).append('=').append(e.getValue()[0]).append('/').append(e.getValue()[1]).append(' ');
            }
            sb.append(" excludedHits=").append(new TreeMap<>(excludedHits)).append(']');
            return sb.toString();
        }
    }

    private static Map<String, Long> snapshot(Map<String, ObjectStoreCounter> counters) {
        Map<String, Long> m = new TreeMap<>();
        counters.forEach((node, c) -> c.snapshotInto(node, m));
        return m;
    }

    private static Map<String, Long> filterNode(Map<String, Long> m, String node) {
        Map<String, Long> out = new TreeMap<>();
        m.forEach((k, v) -> {
            if (k.startsWith(node + "|")) {
                out.put(k.substring(node.length() + 1), v);
            }
        });
        return out;
    }

    /** Counts object-store operations seen through the mock repository on one node. */
    private static final class ObjectStoreCounter extends StatelessMockRepositoryStrategy {
        final AtomicLong lists = new AtomicLong();
        final AtomicLong gets = new AtomicLong();
        final AtomicLong getBytesRequested = new AtomicLong();
        final AtomicLong getBytesRead = new AtomicLong();
        final AtomicLong puts = new AtomicLong();
        final Map<String, AtomicLong> getsByBlobKind = new ConcurrentHashMap<>();

        void reset() {
            lists.set(0);
            gets.set(0);
            getBytesRequested.set(0);
            getBytesRead.set(0);
            puts.set(0);
            getsByBlobKind.clear();
        }

        String summary() {
            Map<String, Long> m = new TreeMap<>();
            snapshotInto("", m);
            return m.toString();
        }

        void snapshotInto(String node, Map<String, Long> m) {
            String p = node.isEmpty() ? "" : node + "|";
            m.put(p + "LIST", lists.get());
            m.put(p + "GET", gets.get());
            m.put(p + "GET_bytes_requested", getBytesRequested.get());
            m.put(p + "GET_bytes_read", getBytesRead.get());
            m.put(p + "PUT", puts.get());
            getsByBlobKind.forEach((k, v) -> m.put(p + "GET_" + k, v.get()));
        }

        private static String kind(String blobName) {
            int idx = blobName.indexOf('-');
            String prefix = idx > 0 ? blobName.substring(0, idx) : blobName;
            return prefix.length() > 24 ? prefix.substring(0, 24) : prefix;
        }

        private InputStream count(InputStream in, String blobName) {
            gets.incrementAndGet();
            getsByBlobKind.computeIfAbsent(kind(blobName), k -> new AtomicLong()).incrementAndGet();
            return new FilterInputStream(in) {
                @Override
                public int read() throws IOException {
                    int r = super.read();
                    if (r >= 0) {
                        getBytesRead.incrementAndGet();
                    }
                    return r;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int r = super.read(b, off, len);
                    if (r > 0) {
                        getBytesRead.addAndGet(r);
                    }
                    return r;
                }

                @Override
                public long skip(long n) throws IOException {
                    return super.skip(n);
                }
            };
        }

        @Override
        public Map<String, BlobContainer> blobContainerChildren(
            CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            lists.incrementAndGet();
            return originalSupplier.get();
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobs(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            lists.incrementAndGet();
            return originalSupplier.get();
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobsByPrefix(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobNamePrefix
        ) throws IOException {
            lists.incrementAndGet();
            return originalSupplier.get();
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            return count(originalSupplier.get(), blobName);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            getBytesRequested.addAndGet(length);
            final InputStream in;
            try {
                in = originalSupplier.get();
            } catch (IOException | RuntimeException e) {
                getsByBlobKind.computeIfAbsent("FAILED_" + e.getClass().getSimpleName(), k -> new AtomicLong()).incrementAndGet();
                throw e;
            }
            return count(in, blobName);
        }

        @Override
        public void blobContainerWriteBlob(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            puts.incrementAndGet();
            originalRunnable.run();
        }

        @Override
        public void blobContainerWriteBlob(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            BytesReference bytes,
            boolean failIfAlreadyExists
        ) throws IOException {
            puts.incrementAndGet();
            originalRunnable.run();
        }

        @Override
        public void blobContainerWriteBlobAtomic(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            puts.incrementAndGet();
            originalRunnable.run();
        }
    }
}
