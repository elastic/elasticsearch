/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cheapshard;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.DefaultWarmingRatioProviderFactory;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.FastSearchActivationSettings;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.stateless.cheapshard.ActivationLatencyIT.EVENTS;
import static org.elasticsearch.xpack.stateless.cheapshard.ActivationLatencyIT.REQUESTS;
import static org.elasticsearch.xpack.stateless.cheapshard.ActivationLatencyIT.ev;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Step-1 experiment (not a feature test): node-local cold-start latency of a stateless search shard as a function of shard size.
 * <p>
 * The node-local clock starts when the search node receives the {@code commit_state} of the publication that assigns the search copy
 * (i.e. after the master has persisted the cluster state to the object store and the publication has been acked by a quorum; the
 * search node starts applying the state at this point) and ends when the first query with all shards successful returns on the
 * search node. Cluster-state persistence PUTs therefore happen strictly before the clock starts; the STARTED publication (P2) is
 * not on the query path (the query is held by the INITIALIZING copy until POST_RECOVERY) and its master PUTs go through the
 * master's own pipe, so they never compete with the search node's reads. Both are counted and reported.
 * <p>
 * Reuses the latency/bandwidth repository, step recorder and log capture of {@link ActivationLatencyIT}. One profile per method;
 * each profile runs (reps x L x query type) activations from the zero state (no search copy, hollow indexing shard, all cache
 * regions of the index evicted on every node). Output lines are prefixed {@code NL|}; CSV rows {@code NLCSV|}.
 */
@TimeoutSuite(millis = 90 * TimeUnits.MINUTE)
public class NodeLocalActivationSweepIT extends AbstractStatelessPluginIntegTestCase {

    private static final Logger LOG = LogManager.getLogger(NodeLocalActivationSweepIT.class);
    private static final long BANDWIDTH_BYTES_PER_SEC = 100L * 1024 * 1024;
    private static final int[] LATENCIES_MS = { 30, 80, 100 };
    private static final int[] FAST_LATENCIES_MS = { 30, 100 };
    private static final int VOCABULARY = 5000;
    private static final int ROUNDS = 6;
    private static final double UPDATE_FRACTION = 0.1;
    private static final int WORDS_PER_DOC = 30;
    private static final int VECTOR_DIMS = 16;

    private static final List<String> CAPTURED_LOGGERS = List.of(
        "org.elasticsearch.xpack.stateless.recovery.TransportSendRecoveryCommitRegistrationAction",
        "org.elasticsearch.xpack.stateless.recovery.StatelessSearchNodeRecoveryListener",
        "org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService"
    );

    enum QueryType {
        MATCH,
        SORT,
        KNN
    }

    /**
     * Activation path. {@code BASE}: today's recovery (fast activation disabled). {@code FAST_*}: step-2 fast activation
     * ({@link FastSearchActivationSettings}) with the given first-fill range size.
     */
    enum Mode {
        BASE(null, false),
        FAST_128K("128kb", false),
        FAST_1M("1mb", false),
        // step 3: fast activation + parallel first-query reads (vector fill policy, fetch-phase stored fields prefetch)
        PAR_128K("128kb", true),
        PAR_1M("1mb", true),
        // step 2b: step-3 config, commit found by one pointer GET, indexing shard unable to serve the activation (registration never
        // answered, reads from the indexing node rejected)
        PTR_NOENG("1mb", true, true),
        // phase 4: PTR_NOENG + the pointer's open-time bytes written in the cache (no header/footer/metadata warming from the store);
        // PTR_NOENG ignores them (stateless.cheap_shard.fast_activation.use_pointer_open_bytes: false) on the same pointer
        PTR_META("1mb", true, true);

        final String rangeSize;
        final boolean parallel;
        final boolean pointerNoEngine;

        Mode(String rangeSize, boolean parallel) {
            this(rangeSize, parallel, false);
        }

        Mode(String rangeSize, boolean parallel, boolean pointerNoEngine) {
            this.rangeSize = rangeSize;
            this.parallel = parallel;
            this.pointerNoEngine = pointerNoEngine;
        }
    }

    /** Step 2b harness brackets: object store bandwidth model. */
    enum Bandwidth {
        /** one 100 MiB/s pipe shared by all requests of a node (steps 1-3) */
        SHARED,
        /** 100 MB/s per request, 1.25 GB/s (10 Gbit/s) per node */
        PERCONN
    }

    private static final long PER_CONNECTION_BPS = 100_000_000L;
    private static final long PER_NODE_AGGREGATE_BPS = 1_250_000_000L;
    /** lognormal first-byte latency with median L and p99 = 3 L */
    private static final double JITTER_SIGMA = Math.log(3) / 2.326;

    /** {@code merges}: false keeps every flushed segment (merges disabled); true uses the default merge policy during the load. */
    record Profile(String name, int docs, String cacheSize, boolean knn, int reps, boolean merges, List<Mode> modes, int[] latencies) {
        Profile(String name, int docs, String cacheSize, boolean knn, int reps, boolean merges) {
            this(name, docs, cacheSize, knn, reps, merges, List.of(Mode.BASE), LATENCIES_MS);
        }

        /** Step 2: baseline and fast activation on the same data, in the same JVM, at L = 30 and 100 ms. */
        static Profile fast(String name, int docs, String cacheSize, int reps, boolean merges) {
            return new Profile(
                name,
                docs,
                cacheSize,
                true,
                reps,
                merges,
                List.of(Mode.BASE, Mode.FAST_128K, Mode.FAST_1M, Mode.PAR_128K, Mode.PAR_1M),
                FAST_LATENCIES_MS
            );
        }

        /** Step 2b: BASE vs step-3 config with registration discovery vs pointer discovery with no indexing engine. */
        static Profile pointer(String name, int docs, String cacheSize, int reps, boolean merges, int... latencies) {
            return new Profile(
                name,
                docs,
                cacheSize,
                true,
                reps,
                merges,
                List.of(Mode.BASE, Mode.PAR_1M, Mode.PTR_NOENG, Mode.PTR_META),
                latencies
            );
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(ActivationLatencyIT.StepRecorderPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING.getKey(), 0.0d)
            // step 2b: indexing shards publish a commit pointer after each upload (only read in PTR_* modes)
            .put(FastSearchActivationSettings.COMMIT_POINTER_ENABLED_SETTING.getKey(), true)
            // phase 4: the pointer also carries the open-time bytes up to this bound (-Dtests.sweep.open_bytes_max, 0 = none)
            .put(
                FastSearchActivationSettings.COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING.getKey(),
                System.getProperty("tests.sweep.open_bytes_max", "4mb")
            );
    }

    // ~0.5 KB/doc on disk incl. a 16-dim vector: 2k ~ 1 MB, 20k ~ 10 MB, 200k ~ 100 MB, 2M ~ 1 GB
    public void testDocs2k() throws Exception {
        run(new Profile("d2k", 2_000, "512mb", true, 2, false));
    }

    public void testDocs10k() throws Exception {
        run(new Profile("d10k", 10_000, "512mb", true, 2, false));
    }

    public void testDocs20k() throws Exception {
        run(new Profile("d20k", 20_000, "512mb", true, 2, false));
    }

    public void testDocs100k() throws Exception {
        run(new Profile("d100k", 100_000, "512mb", true, 2, false));
    }

    public void testDocs200k() throws Exception {
        run(new Profile("d200k", 200_000, "1gb", true, 2, false));
    }

    public void testDocs1m() throws Exception {
        run(new Profile("d1m", 1_000_000, "2gb", true, 2, false));
    }

    public void testDocs200kMerged() throws Exception {
        run(new Profile("d200k-merged", 200_000, "1gb", true, 2, true));
    }

    public void testDocs1mMerged() throws Exception {
        run(new Profile("d1m-merged", 1_000_000, "2gb", true, 2, true));
    }

    /** ~1.3 GB; merges enabled (with merges disabled the 1M profile already has ~200 segments / 36 BCCs). */
    public void testDocs2mMerged() throws Exception {
        run(new Profile("d2m-merged", 2_000_000, "3gb", true, 1, true));
    }

    // step 2: same profiles, baseline vs fast activation (FastSearchActivationSettings) paired per (rep, L, query)
    public void testFastDocs2k() throws Exception {
        run(Profile.fast("d2k", 2_000, "512mb", 2, false));
    }

    public void testFastDocs10k() throws Exception {
        run(Profile.fast("d10k", 10_000, "512mb", 2, false));
    }

    public void testFastDocs100k() throws Exception {
        run(Profile.fast("d100k", 100_000, "512mb", 2, false));
    }

    public void testFastDocs200kMerged() throws Exception {
        run(Profile.fast("d200k-merged", 200_000, "1gb", 2, true));
    }

    public void testFastDocs1mMerged() throws Exception {
        run(Profile.fast("d1m-merged", 1_000_000, "2gb", 2, true));
    }

    // step 2b: BASE / PAR_1M (registration) / PTR_NOENG (pointer, no indexing engine); harness brackets via
    // -Dtests.sweep.bw=SHARED,PERCONN,
    // -Dtests.sweep.jitter=true and -Dtests.sweep.processors=4,8 (one search node per processor count)
    public void testPointerDocs2k() throws Exception {
        run(Profile.pointer("d2k", 2_000, "512mb", 2, false, 30, 100));
    }

    public void testPointerDocs10k() throws Exception {
        run(Profile.pointer("d10k", 10_000, "512mb", 2, false, 30, 100));
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.commits.CommitPointerPublisher:DEBUG",
        reason = "phase 4: pointer size and open-bytes write cost per commit"
    )
    public void testPointerDocs100k() throws Exception {
        run(Profile.pointer("d100k", 100_000, "512mb", 1, false, 100));
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.commits.CommitPointerPublisher:DEBUG",
        reason = "phase 4: pointer size and open-bytes write cost per commit"
    )
    public void testPointerDocs200kMerged() throws Exception {
        run(Profile.pointer("d200k-merged", 200_000, "1gb", 1, true, 100));
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.commits.CommitPointerPublisher:DEBUG",
        reason = "phase 4: pointer size and open-bytes write cost per commit"
    )
    public void testPointerDocs1mMerged() throws Exception {
        run(Profile.pointer("d1m-merged", 1_000_000, "2gb", 1, true, 100));
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // step 3: several cold shards activated at once while queries run at a steady rate
    // ---------------------------------------------------------------------------------------------------------------------------

    private static final int CONCURRENT_SHARDS = 6;

    record Sample(boolean hot, QueryType q, long start, long end, boolean full, boolean failed) {}

    /**
     * Step-3 scenario (not a feature test). One index with {@link #CONCURRENT_SHARDS} shards (hollow indexing shards, no search copy,
     * cache evicted) gets a search copy of every shard at once while an open-loop client sends MATCH and KNN queries to it and MATCH
     * queries to a warm "hot" index on the same search node, at a fixed rate. Reports, per activation mode, the time to the first query
     * on the cold index with all shards successful, latency percentiles of cold and hot queries, and the search pool's active threads and
     * queue sampled every 10 ms. Options: {@code -Dtests.sweep.modes}, {@code -Dtests.sweep.latencies}, {@code -Dtests.sweep.reps},
     * {@code -Dtests.concurrent.docs} (default 300000), {@code -Dtests.concurrent.qps} (cold and hot each, default 20).
     */
    public void testConcurrentActivation() throws Exception {
        final int docs = Integer.getInteger("tests.concurrent.docs", 300_000);
        final int qps = Integer.getInteger("tests.concurrent.qps", 20);
        final Profile p = new Profile(
            "concurrent",
            docs,
            "2gb",
            true,
            1,
            true,
            List.of(Mode.BASE, Mode.FAST_1M, Mode.PAR_1M),
            new int[] { 100 }
        );
        final Random rnd = new Random(42);
        vocabulary = new String[VOCABULARY];
        for (int i = 0; i < VOCABULARY; i++) {
            StringBuilder sb = new StringBuilder();
            int len = 3 + rnd.nextInt(8);
            for (int j = 0; j < len; j++) {
                sb.append((char) ('a' + rnd.nextInt(26)));
            }
            vocabulary[i] = sb.toString() + i;
        }
        final String master = startMasterOnlyNode();
        final Settings cache = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "16mb")
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), "16mb")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), p.cacheSize())
            .build();
        final String indexNodeA = startIndexNode(
            Settings.builder().put(disableIndexingDiskAndMemoryControllersNodeSettings()).put(cache).build()
        );
        final String indexNodeB = startIndexNode(
            Settings.builder().put(disableIndexingDiskAndMemoryControllersNodeSettings()).put(cache).build()
        );
        final String searchNode = startSearchNode(
            Settings.builder().put(cache).put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 8).build()
        );
        ensureStableCluster(4);
        final List<String> nodes = List.of(master, indexNodeA, indexNodeB, searchNode);
        ActivationLatencyIT.knownNodes = nodes;
        final Map<String, ActivationLatencyIT.LatencyStore> stores = new HashMap<>();
        for (String n : nodes) {
            var st = new ActivationLatencyIT.LatencyStore(n);
            stores.put(n, st);
            setNodeRepositoryStrategy(n, st);
        }

        final String cold = "cold";
        final String hot = "hot";
        org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked(
            prepareCreate(cold).setSettings(
                indexSettings(CONCURRENT_SHARDS, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeB)
            ).setMapping(mapping(p))
        );
        org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked(
            prepareCreate(hot).setSettings(indexSettings(1, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeB))
                .setMapping(mapping(p))
        );
        ensureGreen(cold, hot);
        long tl = System.nanoTime();
        final int perRound = docs / ROUNDS;
        int nextId = 0;
        for (int r = 0; r < ROUNDS; r++) {
            List<Integer> batchIds = new ArrayList<>();
            int n = r == ROUNDS - 1 ? docs - nextId : perRound;
            for (int i = 0; i < n; i++) {
                batchIds.add(nextId++);
            }
            bulk(cold, batchIds, p, rnd);
            flush(cold);
        }
        List<Integer> hotIds = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            hotIds.add(i);
        }
        bulk(hot, hotIds, p, rnd);
        flush(hot);
        refresh(hot);
        for (int i = 0; i < 2; i++) {
            assertBusy(
                () -> assertThat(indicesAdmin().prepareStats(cold).setMerge(true).get().getTotal().getMerge().getCurrent(), equalTo(0L)),
                5,
                TimeUnit.MINUTES
            );
            flush(cold);
        }
        final Index coldIndex = resolveIndex(cold);
        final Inventory inv = inventory(cold, coldIndex);
        log(
            "concurrent loaded %d docs in %d ms; cold index shard 0 size=%s blobs=%d segments=%d",
            docs,
            ms(System.nanoTime() - tl),
            mb(inv.bytes),
            inv.blobs,
            inv.segments
        );
        hollowShards(cold, CONCURRENT_SHARDS, indexNodeA, indexNodeB);

        final float[] qv = vector(new Random(7));
        final String word = vocabulary[17];
        final ThreadPool searchThreadPool = internalCluster().getInstance(ThreadPool.class, searchNode);
        final List<Mode> modes = filter(p.modes(), "tests.sweep.modes");
        final List<Integer> latencies = filter(java.util.Arrays.stream(p.latencies()).boxed().toList(), "tests.sweep.latencies");
        final int reps = Integer.getInteger("tests.sweep.reps", 1);
        try {
            for (int rep = 1; rep <= reps; rep++) {
                for (int latency : latencies) {
                    for (Mode mode : modes) {
                        for (String n : nodes) {
                            stores.get(n).configure(0, 0);
                        }
                        assertBusy(() -> assertThat(indexService(searchNode, coldIndex), nullValue()));
                        setFastActivation(mode);
                        for (String n : List.of(indexNodeA, indexNodeB, searchNode)) {
                            cacheService(n).forceEvict(k -> k.shardId().getIndex().equals(coldIndex));
                        }
                        // warm the hot index
                        for (int i = 0; i < 5; i++) {
                            query(searchNode, hot, QueryType.MATCH, word, qv).get().decRef();
                        }
                        Thread.sleep(1000);
                        for (String n : nodes) {
                            stores.get(n).configure(latency, BANDWIDTH_BYTES_PER_SEC);
                        }

                        final ConcurrentLinkedQueue<Sample> samples = new ConcurrentLinkedQueue<>();
                        final List<int[]> poolSamples = java.util.Collections.synchronizedList(new ArrayList<>());
                        final java.util.concurrent.ScheduledExecutorService scheduler = java.util.concurrent.Executors
                            .newScheduledThreadPool(2);
                        final java.util.concurrent.atomic.AtomicInteger seq = new java.util.concurrent.atomic.AtomicInteger();
                        final java.util.concurrent.atomic.AtomicLong activatedAt = new java.util.concurrent.atomic.AtomicLong();
                        final long periodMicros = 1_000_000L / (2L * qps);
                        scheduler.scheduleAtFixedRate(() -> {
                            final int i = seq.getAndIncrement();
                            final boolean isHot = (i & 1) == 0;
                            final QueryType q = isHot ? QueryType.MATCH : ((i >> 1) & 1) == 0 ? QueryType.MATCH : QueryType.KNN;
                            final long start = System.nanoTime();
                            query(searchNode, isHot ? hot : cold, q, word, qv).execute(new org.elasticsearch.action.ActionListener<>() {
                                @Override
                                public void onResponse(SearchResponse r) {
                                    boolean full = r.getFailedShards() == 0 && r.getSuccessfulShards() == r.getTotalShards();
                                    samples.add(new Sample(isHot, q, start, System.nanoTime(), full, false));
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    samples.add(new Sample(isHot, q, start, System.nanoTime(), false, true));
                                }
                            });
                        }, 0, periodMicros, TimeUnit.MICROSECONDS);
                        scheduler.scheduleAtFixedRate(() -> {
                            for (var st : searchThreadPool.stats()) {
                                if (st.name().equals(ThreadPool.Names.SEARCH)) {
                                    long a = activatedAt.get();
                                    poolSamples.add(new int[] { a == 0 ? 0 : 1, st.active(), st.queue() });
                                }
                            }
                        }, 0, 10, TimeUnit.MILLISECONDS);

                        Thread.sleep(2000);
                        final long t0 = System.nanoTime();
                        activatedAt.set(t0);
                        client(master).admin()
                            .indices()
                            .prepareUpdateSettings(cold)
                            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                            .get();
                        // run until the cold index answered fully (at most 60 s: under overload it may never), then 8 more seconds
                        final long deadline = t0 + TimeUnit.SECONDS.toNanos(60);
                        while (System.nanoTime() < deadline
                            && samples.stream().noneMatch(sm -> sm.hot() == false && sm.full() && sm.start() > t0)) {
                            Thread.sleep(20);
                        }
                        final long firstFull = samples.stream()
                            .filter(sm -> sm.hot() == false && sm.full() && sm.start() > t0)
                            .mapToLong(Sample::end)
                            .min()
                            .orElse(t0 - 1_000_000L); // reported as -1 ms: no full answer within 60 s
                        Thread.sleep(8000);
                        final long tEnd = System.nanoTime();
                        scheduler.shutdown();
                        assertTrue(scheduler.awaitTermination(10, TimeUnit.SECONDS));
                        Thread.sleep(3000); // let in-flight queries finish

                        List<Sample> all = new ArrayList<>(samples);
                        String tag = String.format(Locale.ROOT, "concurrent mode=%s rep=%d L=%d qps=%d+%d", mode, rep, latency, qps, qps);
                        log(
                            "%s first full cold hit %d ms after activation | cold MATCH full after activation: %s | cold KNN full after "
                                + "activation: %s | cold partial/failed after activation: %d/%d | hot before: %s | hot after activation: %s",
                            tag,
                            ms(firstFull - t0),
                            pct(
                                all,
                                sm -> sm.hot() == false && sm.q() == QueryType.MATCH && sm.full() && sm.start() > t0 && sm.start() < tEnd
                            ),
                            pct(
                                all,
                                sm -> sm.hot() == false && sm.q() == QueryType.KNN && sm.full() && sm.start() > t0 && sm.start() < tEnd
                            ),
                            all.stream()
                                .filter(sm -> sm.hot() == false && sm.start() > t0 && sm.full() == false && sm.failed() == false)
                                .count(),
                            all.stream().filter(sm -> sm.hot() == false && sm.start() > t0 && sm.failed()).count(),
                            pct(all, sm -> sm.hot() && sm.start() < t0),
                            pct(all, sm -> sm.hot() && sm.start() > t0 && sm.start() < tEnd)
                        );
                        int maxActive = 0;
                        int maxQueue = 0;
                        long sumActive = 0;
                        int nAfter = 0;
                        int saturated = 0;
                        int poolSize = searchThreadPool.info(ThreadPool.Names.SEARCH).getMax();
                        synchronized (poolSamples) {
                            for (int[] ps : poolSamples) {
                                if (ps[0] == 1) {
                                    maxActive = Math.max(maxActive, ps[1]);
                                    maxQueue = Math.max(maxQueue, ps[2]);
                                    sumActive += ps[1];
                                    nAfter++;
                                    if (ps[1] >= poolSize) {
                                        saturated++;
                                    }
                                }
                            }
                        }
                        log(
                            "%s search pool (size %d) after activation: max active=%d mean active=%.1f max queue=%d saturated=%.0f%% of %d samples",
                            tag,
                            poolSize,
                            maxActive,
                            nAfter == 0 ? 0.0 : (double) sumActive / nAfter,
                            maxQueue,
                            nAfter == 0 ? 0.0 : 100.0 * saturated / nAfter,
                            nAfter
                        );
                        LOG.info(
                            "NLCCSV|"
                                + String.format(
                                    Locale.ROOT,
                                    "%s,%d,%d,%d,%d,%s,%s,%s,%s,%d,%.1f,%d,%.1f",
                                    mode,
                                    rep,
                                    latency,
                                    qps,
                                    ms(firstFull - t0),
                                    pct(
                                        all,
                                        sm -> sm.hot() == false
                                            && sm.q() == QueryType.MATCH
                                            && sm.full()
                                            && sm.start() > t0
                                            && sm.start() < tEnd
                                    ).replace(' ', ';'),
                                    pct(
                                        all,
                                        sm -> sm.hot() == false
                                            && sm.q() == QueryType.KNN
                                            && sm.full()
                                            && sm.start() > t0
                                            && sm.start() < tEnd
                                    ).replace(' ', ';'),
                                    pct(all, sm -> sm.hot() && sm.start() < t0).replace(' ', ';'),
                                    pct(all, sm -> sm.hot() && sm.start() > t0 && sm.start() < tEnd).replace(' ', ';'),
                                    maxActive,
                                    nAfter == 0 ? 0.0 : (double) sumActive / nAfter,
                                    maxQueue,
                                    nAfter == 0 ? 0.0 : 100.0 * saturated / nAfter
                                )
                        );

                        for (String n : nodes) {
                            stores.get(n).configure(0, 0);
                        }
                        setReplicaCount(0, cold);
                        assertBusy(() -> assertThat(indexService(searchNode, coldIndex), nullValue()));
                    }
                }
            }
            setFastActivation(Mode.BASE);
        } finally {
            for (String n : nodes) {
                setNodeRepositoryStrategy(n, StatelessMockRepositoryStrategy.DEFAULT);
            }
        }
    }

    /** "n=N p50=X p99=Y max=Z" (ms) of the samples matching {@code p}. */
    private static String pct(List<Sample> all, Predicate<Sample> p) {
        long[] l = all.stream().filter(p).mapToLong(sm -> ms(sm.end() - sm.start())).sorted().toArray();
        if (l.length == 0) {
            return "n=0";
        }
        return String.format(
            Locale.ROOT,
            "n=%d p50=%d p99=%d max=%d",
            l.length,
            l[(int) Math.floor(0.5 * (l.length - 1))],
            l[(int) Math.floor(0.99 * (l.length - 1))],
            l[l.length - 1]
        );
    }

    // ---------------------------------------------------------------------------------------------------------------------------

    private String[] vocabulary;

    /**
     * Optional subset filters for long profiles: {@code -Dtests.sweep.modes=FAST_1M,...}, {@code -Dtests.sweep.latencies=100},
     * {@code -Dtests.sweep.queries=MATCH,KNN}, {@code -Dtests.sweep.reps=1}. {@code -Dtests.sweep.trace=true} also prints every
     * cold read and prefetch of the first query (BlobCacheIndexInput TRACE) with its thread.
     */
    private static <T> List<T> filter(List<T> all, String prop) {
        String v = System.getProperty(prop);
        if (v == null || v.isBlank()) {
            return all;
        }
        Set<String> keep = Set.of(v.split(","));
        return all.stream().filter(t -> keep.contains(t.toString())).toList();
    }

    private static final boolean TRACE_READS = Boolean.getBoolean("tests.sweep.trace");

    record TraceEv(long t, String thread, String msg) {}

    static final ConcurrentLinkedQueue<TraceEv> TRACES = new ConcurrentLinkedQueue<>();

    static final class TraceCapture extends AbstractAppender {
        TraceCapture() {
            super("sweep-trace", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            if (ActivationLatencyIT.recording) {
                final String message = event.getMessage().getFormattedMessage();
                // only cold reads and prefetches (the per-read DEBUG "reading file" lines are too many to keep with a stack)
                if (message.startsWith("slow read") == false && message.startsWith("prefetch") == false) {
                    return;
                }
                // appenders run on the reading thread: its stack says which Lucene/ES code read (phase 4 attribution)
                TRACES.add(new TraceEv(System.nanoTime(), event.getThreadName(), message + " @ " + callerStack()));
            }
        }
    }

    /** The reader's stack, without logging, cache input and JDK frames: caller frames, innermost first, separated by {@code <}. */
    static String callerStack() {
        StringBuilder sb = new StringBuilder();
        int kept = 0;
        for (StackTraceElement f : Thread.currentThread().getStackTrace()) {
            String c = f.getClassName();
            if (c.startsWith("java.")
                || c.startsWith("jdk.")
                || c.startsWith("org.apache.logging.")
                || c.startsWith("org.elasticsearch.logging.")
                || c.startsWith("org.elasticsearch.common.logging.")
                || c.contains("NodeLocalActivationSweepIT")
                || c.contains("BlobCacheIndexInput")
                || c.contains("BlobCacheBufferedIndexInput")) {
                continue;
            }
            if (kept > 0) {
                sb.append(" < ");
            }
            sb.append(c.substring(c.lastIndexOf('.') + 1)).append('.').append(f.getMethodName()).append(':').append(f.getLineNumber());
            if (++kept == 16) {
                break;
            }
        }
        return sb.toString();
    }

    private void run(Profile p) throws Exception {
        final Random rnd = new Random(42);
        vocabulary = new String[VOCABULARY];
        for (int i = 0; i < VOCABULARY; i++) {
            StringBuilder sb = new StringBuilder();
            int len = 3 + rnd.nextInt(8);
            for (int j = 0; j < len; j++) {
                sb.append((char) ('a' + rnd.nextInt(26)));
            }
            vocabulary[i] = sb.toString() + i;
        }

        final String master = startMasterOnlyNode();
        // step 2: the test cluster randomizes node.processors in [1, 4], which sizes the prewarm and shard read pools (4 x
        // processors) and therefore the number of concurrent object store reads; pin a production-like search node (8 cpus:
        // 32 prewarm threads, 28 shard read threads) so that parallel fetches are not limited by the random pool size.
        // step 2b: -Dtests.sweep.processors=4,8 starts one search node per processor count (activations alternate between them)
        final List<Integer> processors = p.modes().size() > 1
            ? java.util.Arrays.stream(System.getProperty("tests.sweep.processors", "8").split(",")).map(Integer::valueOf).toList()
            : java.util.Collections.singletonList((Integer) null);
        final List<Bandwidth> bandwidths = java.util.Arrays.stream(System.getProperty("tests.sweep.bw", "SHARED").split(","))
            .map(Bandwidth::valueOf)
            .toList();
        final boolean jitter = Boolean.getBoolean("tests.sweep.jitter");
        // index nodes get the same cache size: with a smaller cache the _id lookups of updates thrash 16 MiB regions
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "16mb")
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), "16mb")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), p.cacheSize())
            .build();
        final String indexNodeA = startIndexNode(indexNodeSettings);
        final String indexNodeB = startIndexNode(indexNodeSettings);
        final Map<String, Integer> searchNodeProcessors = new java.util.LinkedHashMap<>();
        for (Integer procs : processors) {
            final Settings.Builder searchCacheBuilder = Settings.builder();
            if (procs != null) {
                searchCacheBuilder.put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), procs);
            }
            final Settings searchCache = searchCacheBuilder.put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "16mb")
                .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), "16mb")
                .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), p.cacheSize())
                .build();
            searchNodeProcessors.put(startSearchNode(searchCache), procs == null ? -1 : procs);
        }
        final List<String> searchNodes = List.copyOf(searchNodeProcessors.keySet());
        ensureStableCluster(3 + searchNodes.size());
        final List<String> nodes = new ArrayList<>(List.of(master, indexNodeA, indexNodeB));
        nodes.addAll(searchNodes);
        ActivationLatencyIT.knownNodes = nodes;

        final Map<String, ActivationLatencyIT.LatencyStore> stores = new HashMap<>();
        for (String n : nodes) {
            var s = new ActivationLatencyIT.LatencyStore(n);
            stores.put(n, s);
            setNodeRepositoryStrategy(n, s);
        }

        final String indexName = "nl-" + p.name();
        Settings.Builder is = indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeB)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, p.merges());
        org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked(
            prepareCreate(indexName).setSettings(is).setMapping(mapping(p))
        );
        ensureGreen(indexName);
        final Index index = resolveIndex(indexName);

        // ---------------- load: ROUNDS flushes, 10% updates per round after the first (soft deletes) ----------------
        long tl = System.nanoTime();
        final int perRound = Math.max(1, p.docs() / ROUNDS);
        int nextId = 0;
        Set<Integer> ids = new HashSet<>();
        for (int r = 0; r < ROUNDS; r++) {
            List<Integer> batchIds = new ArrayList<>();
            int n = r == ROUNDS - 1 ? p.docs() - nextId : perRound;
            for (int i = 0; i < n; i++) {
                batchIds.add(nextId++);
            }
            if (r > 0) {
                int updates = (int) (n * UPDATE_FRACTION);
                int bound = nextId - n;
                for (int i = 0; i < updates; i++) {
                    batchIds.add(rnd.nextInt(bound));
                }
            }
            bulk(indexName, batchIds, p, rnd);
            ids.addAll(batchIds);
            flush(indexName);
        }
        if (p.merges()) {
            // let background merges finish and commit their result, as an idle shard would before going to zero
            for (int i = 0; i < 2; i++) {
                assertBusy(
                    () -> assertThat(
                        indicesAdmin().prepareStats(indexName).setMerge(true).get().getTotal().getMerge().getCurrent(),
                        equalTo(0L)
                    ),
                    5,
                    TimeUnit.MINUTES
                );
                flush(indexName);
            }
        }
        final long expectedDocs = ids.size();
        log("%s loaded %d unique docs in %d ms", p.name(), expectedDocs, ms(System.nanoTime() - tl));
        final Inventory inv = inventory(indexName, index);
        log("%s shard size=%s blobs=%d segments=%d deletedDocs=%d", p.name(), mb(inv.bytes), inv.blobs, inv.segments, inv.deletedDocs);

        hollowShards(indexName, 1, indexNodeA, indexNodeB);
        logPointer(p, index);
        assertThat(findIndexShard(index, 0).getEngineOrNull(), instanceOf(HollowIndexEngine.class));

        ActivationLatencyIT.Capture capture = new ActivationLatencyIT.Capture();
        capture.start();
        for (String l : CAPTURED_LOGGERS) {
            Logger lg = LogManager.getLogger(l);
            Loggers.setLevel(lg, Level.DEBUG);
            Loggers.addAppender(lg, capture);
        }
        for (String sn : searchNodes) {
            installProbes(sn);
        }

        final List<QueryType> queries = filter(
            p.knn() ? List.of(QueryType.MATCH, QueryType.SORT, QueryType.KNN) : List.of(QueryType.MATCH, QueryType.SORT),
            "tests.sweep.queries"
        );
        final List<Mode> modes = filter(p.modes(), "tests.sweep.modes");
        final List<Integer> latencies = filter(java.util.Arrays.stream(p.latencies()).boxed().toList(), "tests.sweep.latencies");
        final int reps = Integer.getInteger("tests.sweep.reps", p.reps());
        final TraceCapture traceCapture = new TraceCapture();
        final Logger traceLogger = LogManager.getLogger("org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput");
        if (TRACE_READS) {
            traceCapture.start();
            Loggers.setLevel(traceLogger, Level.TRACE);
            Loggers.addAppender(traceLogger, traceCapture);
        }
        final float[] qv = vector(new Random(7));
        final String word = vocabulary[17];
        try {
            for (int rep = 1; rep <= reps; rep++) {
                for (int latency : latencies) {
                    for (QueryType q : queries) {
                        for (String searchNode : searchNodes) {
                            for (Bandwidth bw : bandwidths) {
                                String baseResult = null;
                                if (searchNodes.size() > 1) {
                                    // the search copy goes to searchNode: exclude the other search nodes (and the original indexing node)
                                    List<String> excluded = new ArrayList<>(List.of(indexNodeA));
                                    searchNodes.stream().filter(n -> n.equals(searchNode) == false).forEach(excluded::add);
                                    updateIndexSettings(
                                        Settings.builder()
                                            .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", String.join(",", excluded)),
                                        indexName
                                    );
                                }
                                for (Mode mode : modes) {
                                    // ---------------- zero state ----------------
                                    for (String n : nodes) {
                                        stores.get(n).configure(0, 0);
                                    }
                                    assertBusy(() -> assertThat(indexService(searchNode, index), nullValue()));
                                    assertThat(findIndexShard(index, 0).getEngineOrNull(), instanceOf(HollowIndexEngine.class));
                                    setFastActivation(mode);
                                    int evicted = 0;
                                    for (String n : nodes.subList(1, nodes.size())) {
                                        evicted += cacheService(n).forceEvict(k -> k.shardId().getIndex().equals(index));
                                    }
                                    quiesce(1000);
                                    ActivationLatencyIT.LatencyStore.jitterSigma = jitter ? JITTER_SIGMA : 0.0d;
                                    ActivationLatencyIT.LatencyStore.perConnectionBps = bw == Bandwidth.PERCONN ? PER_CONNECTION_BPS : 0L;
                                    for (String n : nodes) {
                                        stores.get(n)
                                            .configure(latency, bw == Bandwidth.PERCONN ? PER_NODE_AGGREGATE_BPS : BANDWIDTH_BYTES_PER_SEC);
                                    }
                                    final List<org.elasticsearch.transport.TransportChannel> heldRegistrations = new ArrayList<>();
                                    if (mode.pointerNoEngine) {
                                        blockIndexingShard(List.of(indexNodeA, indexNodeB), heldRegistrations);
                                    }

                                    // ---------------- activation ----------------
                                    EVENTS.clear();
                                    REQUESTS.clear();
                                    TRACES.clear();
                                    ActivationLatencyIT.recording = true;
                                    final long t0 = System.nanoTime();
                                    client(master).admin()
                                        .indices()
                                        .prepareUpdateSettings(indexName)
                                        .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                                        .execute();
                                    long firstHit = 0;
                                    long took = -1;
                                    String firstResult = null;
                                    int attempts = 0;
                                    while (firstHit == 0) {
                                        if (System.nanoTime() - t0 > TimeUnit.SECONDS.toNanos(300)) {
                                            fail("no full search hit within 300s");
                                        }
                                        attempts++;
                                        long ts = System.nanoTime();
                                        try {
                                            SearchResponse resp = query(searchNode, indexName, q, word, qv).get();
                                            try {
                                                long te = System.nanoTime();
                                                if (resp.getFailedShards() == 0 && resp.getSuccessfulShards() == 1) {
                                                    firstHit = te;
                                                    took = ms(te - ts);
                                                    firstResult = summary(resp);
                                                }
                                            } finally {
                                                resp.decRef();
                                            }
                                        } catch (Exception e) {
                                            // no searchable copy yet
                                        }
                                        if (firstHit == 0) {
                                            Thread.sleep(2);
                                        }
                                    }
                                    ev(searchNode, "client:firstFullHit", indexName, -1, "took=" + took);
                                    String second;
                                    SearchResponse resp2 = query(searchNode, indexName, q, word, qv).get();
                                    try {
                                        second = summary(resp2);
                                    } finally {
                                        resp2.decRef();
                                    }
                                    quiesce(500);
                                    ActivationLatencyIT.recording = false;
                                    if (mode.pointerNoEngine) {
                                        unblockIndexingShard(List.of(indexNodeA, indexNodeB), heldRegistrations);
                                    }
                                    if (q != QueryType.KNN) {
                                        assertThat(second, equalTo(firstResult));
                                    }
                                    // correctness of the fast path: same hits as the baseline activation on the same data
                                    if (mode == Mode.BASE) {
                                        baseResult = firstResult;
                                    } else if (baseResult != null) {
                                        log(
                                            "%s %s vs BASE L=%d q=%s: %s",
                                            p.name(),
                                            mode,
                                            latency,
                                            q,
                                            firstResult.equals(baseResult) ? "SAME" : "DIFF"
                                        );
                                        if (q != QueryType.KNN) {
                                            assertThat(mode + " " + q, firstResult, equalTo(baseResult));
                                        }
                                    }
                                    report(
                                        p,
                                        mode,
                                        inv,
                                        rep,
                                        latency,
                                        q,
                                        index,
                                        t0,
                                        firstHit,
                                        took,
                                        attempts,
                                        master,
                                        searchNode,
                                        evicted,
                                        firstResult,
                                        bw,
                                        jitter,
                                        searchNodeProcessors.get(searchNode)
                                    );

                                    // ---------------- back to zero ----------------
                                    for (String n : nodes) {
                                        stores.get(n).configure(0, 0);
                                    }
                                    setReplicaCount(0, indexName);
                                    assertBusy(() -> assertThat(indexService(searchNode, index), nullValue()));
                                }
                            }
                        }
                    }
                }
            }
            ActivationLatencyIT.LatencyStore.jitterSigma = 0.0d;
            ActivationLatencyIT.LatencyStore.perConnectionBps = 0L;
            setFastActivation(Mode.BASE);
            assertThat(countAllWithSearchCopy(indexName), equalTo(expectedDocs));
        } finally {
            ActivationLatencyIT.recording = false;
            if (TRACE_READS) {
                Loggers.removeAppender(traceLogger, traceCapture);
                Loggers.setLevel(traceLogger, (Level) null);
                traceCapture.stop();
            }
            for (String l : CAPTURED_LOGGERS) {
                Logger lg = LogManager.getLogger(l);
                Loggers.removeAppender(lg, capture);
                Loggers.setLevel(lg, (Level) null);
            }
            capture.stop();
            for (String n : nodes) {
                MockTransportService.getInstance(n).clearAllRules();
                setNodeRepositoryStrategy(n, StatelessMockRepositoryStrategy.DEFAULT);
            }
        }
    }

    /** Phase 4: size and content of the shard's latest commit pointer (after the hollow relocation re-published it). */
    private void logPointer(Profile p, Index index) throws Exception {
        var objectStore = getObjectStoreService(internalCluster().getMasterName());
        ShardId sid = new ShardId(index, 0);
        var container = objectStore.getProjectBlobContainer(sid);
        assertBusy(() -> {
            org.elasticsearch.xpack.stateless.commits.CommitPointer latest = null;
            long term = 0;
            for (long t = 1; t <= 8; t++) {
                var c = org.elasticsearch.xpack.stateless.commits.CommitPointer.read(container, t);
                if (c != null) {
                    latest = c;
                    term = t;
                }
            }
            assertNotNull(latest);
            log(
                "%s POINTER term=%d size=%d bytes, open ranges=%d open bytes=%d, replicated-range files=%d, commit files=%d, blobs=%d",
                p.name(),
                term,
                latest.toBytes().length(),
                latest.openRanges().size(),
                latest.openRangesSizeInBytes(),
                latest.replicatedFileRanges().size(),
                latest.compoundCommit().commitFiles().size(),
                latest.compoundCommit().commitFiles().values().stream().map(l -> l.blobFile()).distinct().count()
            );
            if (Integer.getInteger("tests.sweep.open_bytes_expected_min", 0) > 0) {
                assertTrue(latest.openRanges().isEmpty() == false);
            }
        });
    }

    private static void setFastActivation(Mode mode) {
        Settings.Builder settings = Settings.builder()
            .put(FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING.getKey(), mode != Mode.BASE);
        if (mode.rangeSize != null) {
            settings.put(FastSearchActivationSettings.FAST_ACTIVATION_RANGE_SIZE_SETTING.getKey(), mode.rangeSize);
        }
        settings.put(
            FastSearchActivationSettings.FAST_ACTIVATION_DISCOVERY_SETTING.getKey(),
            mode.pointerNoEngine ? "pointer" : "registration"
        );
        settings.put(FastSearchActivationSettings.FAST_ACTIVATION_USE_POINTER_OPEN_BYTES_SETTING.getKey(), mode == Mode.PTR_META);
        // phase 4: -Dtests.sweep.meta_whole_blob_threshold=0b drops the whole-small-blob warming in PTR_META (an ES warming policy: with
        // the open-time bytes in the pointer nothing else is needed to open the reader)
        final String metaWholeBlobThreshold = System.getProperty("tests.sweep.meta_whole_blob_threshold");
        if (mode == Mode.PTR_META && metaWholeBlobThreshold != null) {
            settings.put(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey(), metaWholeBlobThreshold);
        } else {
            settings.putNull(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey());
        }
        // step 2b: -Dtests.sweep.header_range=1mb overrides the activation header/footer warming fill (the step-2b sweep ran with 64kb)
        settings.put(
            FastSearchActivationSettings.FAST_ACTIVATION_HEADER_RANGE_SIZE_SETTING.getKey(),
            System.getProperty("tests.sweep.header_range", "64kb")
        );
        settings.put(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING.getKey(), mode.parallel ? "16mb" : "0b")
            .put(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING.getKey(), mode.parallel ? "64mb" : "0b")
            .put(SearchService.FETCH_PHASE_PREFETCH_STORED_FIELDS.getKey(), mode.parallel);
        updateClusterSettings(settings);
    }

    /**
     * Makes the indexing shard unable to serve an activation: registrations are never answered (held until
     * {@link #unblockIndexingShard}) and reads from the indexing node fail.
     */
    private static void blockIndexingShard(List<String> indexNodes, List<org.elasticsearch.transport.TransportChannel> held) {
        for (String n : indexNodes) {
            MockTransportService t = MockTransportService.getInstance(n);
            t.addRequestHandlingBehavior(
                org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction.NAME,
                (handler, request, channel, task) -> {
                    synchronized (held) {
                        held.add(channel);
                    }
                }
            );
            t.addRequestHandlingBehavior(
                org.elasticsearch.xpack.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> channel.sendResponse(new IllegalStateException("simulated: no indexing engine"))
            );
        }
    }

    private static void unblockIndexingShard(List<String> indexNodes, List<org.elasticsearch.transport.TransportChannel> held) {
        for (String n : indexNodes) {
            MockTransportService.getInstance(n).clearAllRules();
        }
        synchronized (held) {
            log("PTR_NOENG: %d registration(s) held unanswered during the activation", held.size());
            for (var channel : held) {
                channel.sendResponse(new IllegalStateException("simulated: no indexing engine"));
            }
            held.clear();
        }
    }

    private long countAllWithSearchCopy(String indexName) throws Exception {
        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        var resp = client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true).get();
        try {
            return resp.getHits().getTotalHits().value();
        } finally {
            resp.decRef();
        }
    }

    private static SearchRequestBuilder query(String node, String indexName, QueryType q, String word, float[] qv) {
        SearchRequestBuilder b = client(node).prepareSearch(indexName).setSize(10);
        return switch (q) {
            case MATCH -> b.setQuery(QueryBuilders.matchQuery("text", word)).setTrackTotalHits(true);
            case SORT -> b.setQuery(QueryBuilders.matchAllQuery()).addSort("ts", SortOrder.DESC);
            case KNN -> b.setKnnSearch(List.of(new KnnSearchBuilder("vec", qv, 10, 50, null, null, null)));
        };
    }

    private static String summary(SearchResponse resp) {
        StringBuilder sb = new StringBuilder();
        var th = resp.getHits().getTotalHits();
        sb.append("total=").append(th == null ? "-" : th.value() + th.relation().toString().substring(0, 2)).append(" top=");
        SearchHit[] hits = resp.getHits().getHits();
        for (int i = 0; i < Math.min(3, hits.length); i++) {
            sb.append(hits[i].getId()).append(',');
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // probes + report
    // ---------------------------------------------------------------------------------------------------------------------------

    private void installProbes(String searchNode) {
        MockTransportService search = MockTransportService.getInstance(searchNode);
        search.addRequestHandlingBehavior(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME, (handler, request, channel, task) -> {
            ev(searchNode, "transport:publish_state recv", "", -1, "");
            handler.messageReceived(request, channel, task);
        });
        search.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
            ev(searchNode, "transport:commit_state recv", "", -1, "");
            handler.messageReceived(request, channel, task);
        });
    }

    private static long first(Predicate<ActivationLatencyIT.Ev> p) {
        long best = 0;
        for (var e : EVENTS) {
            if (p.test(e) && (best == 0 || e.t() < best)) {
                best = e.t();
            }
        }
        return best;
    }

    private static long lastBefore(Predicate<ActivationLatencyIT.Ev> p, long bound) {
        long best = 0;
        for (var e : EVENTS) {
            if (p.test(e) && e.t() <= bound && e.t() > best) {
                best = e.t();
            }
        }
        return best;
    }

    /** [list, get, put, bytes] of requests started in [from, to). */
    private static long[] count(long from, long to, Predicate<ActivationLatencyIT.Req> p) {
        long[] c = new long[4];
        for (var r : REQUESTS) {
            if (r.tStart >= from && r.tStart < to && p.test(r)) {
                switch (r.op) {
                    case "LIST" -> c[0]++;
                    case "GET" -> {
                        c[1]++;
                        c[3] += r.bytes.get();
                    }
                    case "PUT" -> c[2]++;
                    default -> throw new AssertionError(r.op);
                }
            }
        }
        return c;
    }

    /**
     * Groups the GETs started in [from, to) into waves: a GET starting more than L/2 after the first GET of the current wave starts a
     * new wave. Returns "waves=W gets=N max_concurrent=C threads=T", i.e. how many dependent (or queued) round trips the window took.
     */
    private static String waves(long from, long to, Predicate<ActivationLatencyIT.Req> p, int latencyMs) {
        List<ActivationLatencyIT.Req> gets = new ArrayList<>();
        for (var r : REQUESTS) {
            if (r.tStart >= from && r.tStart < to && p.test(r) && r.op.equals("GET")) {
                gets.add(r);
            }
        }
        gets.sort((a, b) -> Long.compare(a.tStart, b.tStart));
        int waves = 0;
        long waveStart = 0L;
        int maxConcurrent = 0;
        Set<String> threads = new HashSet<>();
        for (var r : gets) {
            threads.add(r.thread);
            if (waves == 0 || r.tStart - waveStart > TimeUnit.MILLISECONDS.toNanos(latencyMs) / 2) {
                waves++;
                waveStart = r.tStart;
            }
            int concurrent = 0;
            for (var o : gets) {
                if (o.tStart <= r.tStart && (o.tEnd == 0 || o.tEnd > r.tStart)) {
                    concurrent++;
                }
            }
            maxConcurrent = Math.max(maxConcurrent, concurrent);
        }
        return String.format(
            Locale.ROOT,
            "waves=%d gets=%d max_concurrent=%d threads=%d",
            waves,
            gets.size(),
            maxConcurrent,
            threads.size()
        );
    }

    private static double d(long from, long to) {
        return from == 0 || to == 0 ? Double.NaN : (to - from) / 1_000_000.0;
    }

    private void report(
        Profile p,
        Mode mode,
        Inventory inv,
        int rep,
        int latency,
        QueryType q,
        Index index,
        long t0,
        long firstHit,
        long took,
        int attempts,
        String master,
        String searchNode,
        int evicted,
        String result,
        Bandwidth bw,
        boolean jitter,
        int processors
    ) {
        final String idx = index.getName();
        Predicate<ActivationLatencyIT.Ev> mine = e -> e.node().equals(searchNode) && e.index().equals(idx);
        long ixB = first(e -> mine.test(e) && e.kind().equals("beforeIndexCreated"));
        // the commit of the publication that assigns the shard is the last commit received on the search node before it creates the index
        long commit = lastBefore(e -> e.node().equals(searchNode) && e.kind().equals("transport:commit_state recv"), ixB);
        long pub = lastBefore(e -> e.node().equals(searchNode) && e.kind().equals("transport:publish_state recv"), commit);
        long recovering = first(e -> mine.test(e) && e.shard() == 0 && e.kind().equals("state:RECOVERING"));
        long regSend = first(e -> mine.test(e) && e.detail().contains("sending recovery commit registration"));
        long regRecv = first(e -> mine.test(e) && e.detail().contains("received registration response"));
        if (mode.pointerNoEngine) {
            // step 2b: the commit comes from one pointer GET; registration (if any) runs in the background, after the clock's steps
            regSend = first(e -> mine.test(e) && e.detail().contains("reading commit pointer"));
            regRecv = first(e -> mine.test(e) && e.detail().contains("read commit pointer"));
        }
        long postRec = first(e -> mine.test(e) && e.shard() == 0 && e.kind().equals("state:POST_RECOVERY"));
        long started = first(e -> mine.test(e) && e.shard() == 0 && e.kind().equals("state:STARTED"));

        IndexShard shard = indexService(searchNode, index).getShard(0);
        RecoveryState rs = shard.recoveryState();
        long openStart = timerStartNanos(rs.getTranslog());
        long openEnd = openStart == 0 ? 0 : openStart + TimeUnit.MILLISECONDS.toNanos(rs.getTranslog().time());

        Predicate<ActivationLatencyIT.Req> onSearch = r -> r.node.equals(searchNode);
        Predicate<ActivationLatencyIT.Req> onMaster = r -> r.node.equals(master);
        long[] local = count(commit, firstHit, onSearch);
        long[] localMaster = count(commit, firstHit, onMaster);
        long[] p1Master = count(t0, commit, onMaster);
        long[] p1Search = count(t0, commit, onSearch);
        long[] applyReqs = count(commit, recovering, onSearch);
        long[] discovery = count(recovering, regSend, onSearch);
        long[] queryReqs = count(postRec, firstHit, onSearch);

        double nodeLocal = d(commit, firstHit);
        double e2e = d(t0, firstHit);
        double[] steps = {
            d(commit, recovering), // apply: IndexService + mapping + IndexShard
            d(recovering, regSend), // S4+S5: 2 LIST + latest BCC header
            d(regSend, regRecv), // S6: registration with the indexing shard
            d(regRecv, openStart), // S7: referenced BCC headers, warming kick-off
            d(openStart, openEnd), // S9: SearchEngine / DirectoryReader open
            d(openEnd, postRec), // S10: refresh -> POST_RECOVERY
            d(postRec, firstHit) // S12: first query execution after POST_RECOVERY
        };
        String tag = String.format(
            Locale.ROOT,
            "%s mode=%s bw=%s jitter=%s procs=%d rep=%d L=%d q=%s",
            p.name(),
            mode,
            bw,
            jitter,
            processors,
            rep,
            latency,
            q
        );
        if (TRACE_READS) {
            List<TraceEv> traces = new ArrayList<>(TRACES);
            traces.sort((a, b) -> Long.compare(a.t(), b.t()));
            // phase 4: the whole activation after the commit is known (warming W, reader open O, first query Q), times from the pointer
            final long base = regRecv;
            for (TraceEv t : traces) {
                if (t.t() >= regRecv && t.t() <= firstHit && t.thread().contains(searchNode)) {
                    String ph = t.t() < openStart ? "W" : t.t() < postRec ? "O" : "Q";
                    log("%s TRACE %s +%.1f [%s] %s", tag, ph, (t.t() - base) / 1e6, t.thread(), t.msg());
                }
            }
            List<ActivationLatencyIT.Req> gets = new ArrayList<>();
            for (var r : REQUESTS) {
                if (r.tStart >= regRecv && r.tStart < firstHit && onSearch.test(r) && r.op.equals("GET")) {
                    gets.add(r);
                }
            }
            gets.sort((a, b) -> Long.compare(a.tStart, b.tStart));
            for (var r : gets) {
                String ph = r.tStart < openStart ? "W" : r.tStart < postRec ? "O" : "Q";
                log(
                    "%s GETLOG %s +%.1f dur=%.1f %s/%s req=%d bytes=%d [%s]",
                    tag,
                    ph,
                    (r.tStart - base) / 1e6,
                    (r.tEnd - r.tStart) / 1e6,
                    r.path,
                    r.blob,
                    r.requested,
                    r.bytes.get(),
                    r.thread
                );
            }
            log(
                "%s PHASES openStart=+%.1f openEnd=+%.1f postRec=+%.1f firstHit=+%.1f",
                tag,
                (openStart - base) / 1e6,
                (openEnd - base) / 1e6,
                (postRec - base) / 1e6,
                (firstHit - base) / 1e6
            );
        }
        log(
            "%s GET waves: before open %s | open %s | query %s",
            tag,
            waves(regRecv, openStart, onSearch, latency),
            waves(openStart, postRec, onSearch, latency),
            waves(postRec, firstHit, onSearch, latency)
        );
        log(
            "%s nodeLocal=%.1f e2e=%.1f publication(t0->commit)=%.1f [publish->commit %.1f] | apply=%.1f discovery=%.1f reg=%.1f "
                + "headers=%.1f open=%.1f refresh=%.1f query=%.1f | local LIST=%d GET=%d PUT=%d bytes=%s | P1 master PUT=%d "
                + "(search reqs before commit=%d) | master PUT inside local window=%d | apply reqs=%d discovery LIST=%d GET=%d "
                + "query GET=%d bytes=%s | STARTED at %.1f | took=%d attempts=%d evicted=%d result=%s",
            tag,
            nodeLocal,
            e2e,
            d(t0, commit),
            d(pub, commit),
            steps[0],
            steps[1],
            steps[2],
            steps[3],
            steps[4],
            steps[5],
            steps[6],
            local[0],
            local[1],
            local[2],
            mb(local[3]),
            p1Master[2],
            p1Search[0] + p1Search[1] + p1Search[2],
            localMaster[2],
            applyReqs[0] + applyReqs[1] + applyReqs[2],
            discovery[0],
            discovery[1],
            queryReqs[1],
            mb(queryReqs[3]),
            d(t0, started),
            took,
            attempts,
            evicted,
            result
        );
        LOG.info(
            "NLCSV|"
                + String.format(
                    Locale.ROOT,
                    "%s,%d,%d,%d,%d,%d,%s,%d,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%d,%d,%d,%d,%d,%s,%s,%s,%d,%s,%s,%s",
                    p.name(),
                    p.docs(),
                    inv.bytes,
                    inv.segments,
                    inv.deletedDocs,
                    inv.blobs,
                    q,
                    latency,
                    rep,
                    nodeLocal,
                    e2e,
                    d(t0, commit),
                    steps[0],
                    steps[1],
                    steps[2],
                    steps[3],
                    steps[4],
                    steps[5],
                    steps[6],
                    d(t0, started),
                    local[0],
                    local[1],
                    local[3],
                    queryReqs[1],
                    queryReqs[3],
                    p1Master[2] + localMaster[2],
                    mode,
                    bw,
                    jitter,
                    processors,
                    waves(regRecv, openStart, onSearch, latency).replace(' ', ';'),
                    waves(openStart, postRec, onSearch, latency).replace(' ', ';'),
                    waves(postRec, firstHit, onSearch, latency).replace(' ', ';')
                )
        );
    }

    private static long timerStartNanos(RecoveryState.Timer timer) {
        try {
            Field f = RecoveryState.Timer.class.getDeclaredField("startNanoTime");
            f.setAccessible(true);
            synchronized (timer) {
                return f.getLong(timer);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------------------------

    record Inventory(long bytes, int blobs, int segments, long deletedDocs) {}

    private Inventory inventory(String indexName, Index index) throws Exception {
        var objectStore = getObjectStoreService(internalCluster().getMasterName());
        ShardId sid = new ShardId(index, 0);
        var container = objectStore.getProjectBlobStore(sid).blobContainer(objectStore.shardBasePath(sid));
        long total = 0;
        int blobs = 0;
        for (var term : container.children(OperationPurpose.INDICES).entrySet()) {
            for (var b : term.getValue().listBlobs(OperationPurpose.INDICES).entrySet()) {
                total += b.getValue().length();
                blobs++;
            }
        }
        var segs = indicesAdmin().prepareSegments(indexName).get();
        int[] segments = new int[1];
        long[] deleted = new long[1];
        segs.getIndices().get(indexName).forEach(iss -> {
            for (var shardSegs : iss.shards()) {
                segments[0] = shardSegs.getSegments().size();
                deleted[0] = shardSegs.getSegments().stream().mapToLong(seg -> seg.getDeletedDocs()).sum();
            }
        });
        return new Inventory(total, blobs, segments[0], deleted[0]);
    }

    private static void quiesce(long quietMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            long lastStart = 0;
            boolean inFlight = false;
            for (var r : REQUESTS) {
                lastStart = Math.max(lastStart, r.tStart);
                if (r.tEnd == 0 && System.nanoTime() - r.tStart < TimeUnit.SECONDS.toNanos(30)) {
                    inFlight = true;
                }
            }
            if (inFlight == false && System.nanoTime() - lastStart > TimeUnit.MILLISECONDS.toNanos(quietMs)) {
                return;
            }
            Thread.sleep(50);
        }
    }

    private XContentBuilder mapping(Profile p) throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder().startObject().startObject("properties");
        b.startObject("text").field("type", "text").endObject();
        b.startObject("kw").field("type", "keyword").endObject();
        b.startObject("num").field("type", "long").endObject();
        b.startObject("ts").field("type", "date").endObject();
        if (p.knn()) {
            b.startObject("vec").field("type", "dense_vector").field("dims", VECTOR_DIMS).field("similarity", "l2_norm").endObject();
        }
        return b.endObject().endObject();
    }

    private static float[] vector(Random rnd) {
        float[] v = new float[VECTOR_DIMS];
        for (int i = 0; i < v.length; i++) {
            v[i] = rnd.nextFloat() * 2 - 1;
        }
        return v;
    }

    private Map<String, Object> doc(Profile p, Random rnd, int id) {
        Map<String, Object> d = new HashMap<>();
        StringBuilder text = new StringBuilder();
        for (int w = 0; w < WORDS_PER_DOC; w++) {
            if (w > 0) {
                text.append(' ');
            }
            text.append(vocabulary[rnd.nextInt(VOCABULARY)]);
        }
        d.put("text", text.toString());
        d.put("kw", vocabulary[rnd.nextInt(VOCABULARY)]);
        d.put("num", rnd.nextLong());
        d.put("ts", 1_700_000_000_000L + id);
        if (p.knn()) {
            d.put("vec", vector(rnd));
        }
        return d;
    }

    private void bulk(String indexName, List<Integer> ids, Profile p, Random rnd) {
        final int batch = 5000;
        for (int i = 0; i < ids.size(); i += batch) {
            BulkRequestBuilder b = client().prepareBulk();
            for (int j = i; j < Math.min(ids.size(), i + batch); j++) {
                int id = ids.get(j);
                b.add(client().prepareIndex(indexName).setId(Integer.toString(id)).setSource(doc(p, rnd, id)));
            }
            BulkResponse r = b.get();
            assertFalse(r.buildFailureMessage(), r.hasFailures());
        }
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

    private static String mb(long bytes) {
        return String.format(Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }

    private static long ms(long nanos) {
        return TimeValue.nsecToMSec(nanos);
    }

    private static void log(String format, Object... args) {
        LOG.info("NL| " + String.format(Locale.ROOT, format, args));
    }
}
