/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.activation;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
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
import org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * S0 baseline experiment (not a feature test): per-step latency of bringing a stateless shard online from object storage with a cold
 * cache, with first-byte latency and bandwidth injected into the (FS-backed) mock object store. Measures (a) activating a search copy
 * (replicas 0 -> 1) until the first full search hit and (b) the first acknowledged write on a hollow indexing shard (unhollow path).
 * No production changes. Every measurement is logged with prefix {@code S0|}.
 */
public class ActivationLatencyIT extends AbstractStatelessPluginIntegTestCase {

    private static final Logger LOG = LogManager.getLogger(ActivationLatencyIT.class);
    private static final long BANDWIDTH_BYTES_PER_SEC = 100L * 1024 * 1024;
    private static final int[] LATENCIES_MS = { 0, 30, 80 };
    private static final int VOCABULARY = 5000;

    // ---------------------------------------------------------------------------------------------------------------------------
    // global recorders (single JVM)
    // ---------------------------------------------------------------------------------------------------------------------------

    record Ev(long t, String node, String kind, String index, int shard, String detail) {}

    static final class Req {
        final String node;
        final String op;
        final String path;
        final String blob;
        final String thread;
        final long tStart;
        volatile long tFirstByte;
        volatile long tEnd;
        final long requested;
        final AtomicLong bytes = new AtomicLong();

        Req(String node, String op, String path, String blob, long requested) {
            this.node = node;
            this.op = op;
            this.path = path;
            this.blob = blob;
            this.thread = Thread.currentThread().getName();
            this.tStart = System.nanoTime();
            this.requested = requested;
        }

        String pool() {
            Matcher m = POOL.matcher(thread);
            return m.find() ? m.group(1) : thread.replaceAll("[0-9#]+", "");
        }
    }

    private static final Pattern POOL = Pattern.compile("\\]\\[([a-zA-Z0-9_\\-.]+)\\]\\[T#");
    static final ConcurrentLinkedQueue<Ev> EVENTS = new ConcurrentLinkedQueue<>();
    static final ConcurrentLinkedQueue<Req> REQUESTS = new ConcurrentLinkedQueue<>();
    static volatile boolean recording = false;
    static volatile List<String> knownNodes = List.of();

    static void ev(String node, String kind, String index, int shard, String detail) {
        if (recording) {
            EVENTS.add(new Ev(System.nanoTime(), node, kind, index, shard, detail));
        }
    }

    static String nodeOfThread(String threadName) {
        for (String n : knownNodes) {
            if (threadName.contains("[" + n + "]")) {
                return n;
            }
        }
        return "?";
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // test plugin: IndexEventListener step timestamps + merge-enabled setting
    // ---------------------------------------------------------------------------------------------------------------------------

    public static class StepRecorderPlugin extends Plugin {
        private final String nodeName;

        public StepRecorderPlugin(Settings settings) {
            this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        }

        @Override
        public List<Setting<?>> getSettings() {
            // the default ratio provider reads this setting only if some plugin registers it (serverless does; plain stateless does not)
            return List.of(
                Setting.boolSetting(MergePolicyConfig.INDEX_MERGE_ENABLED, true, Setting.Property.IndexScope),
                DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING
            );
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void beforeIndexCreated(Index index, Settings indexSettings) {
                    ev(nodeName, "beforeIndexCreated", index.getName(), -1, "");
                }

                @Override
                public void afterIndexCreated(IndexService indexService) {
                    ev(nodeName, "afterIndexCreated", indexService.index().getName(), -1, "");
                }

                @Override
                public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
                    ev(nodeName, "beforeIndexShardCreated", routing.getIndexName(), routing.id(), routing.role().toString());
                }

                @Override
                public void afterIndexShardCreated(IndexShard indexShard) {
                    ev(nodeName, "afterIndexShardCreated", indexShard.shardId().getIndexName(), indexShard.shardId().id(), "");
                }

                @Override
                public void indexShardStateChanged(
                    IndexShard indexShard,
                    IndexShardState previousState,
                    IndexShardState currentState,
                    String reason
                ) {
                    ev(
                        nodeName,
                        "state:" + currentState,
                        indexShard.shardId().getIndexName(),
                        indexShard.shardId().id(),
                        String.valueOf(reason)
                    );
                }

                @Override
                public void beforeIndexShardRecovery(
                    IndexShard indexShard,
                    org.elasticsearch.index.IndexSettings indexSettings,
                    ActionListener<Void> listener
                ) {
                    // registered after the stateless plugin's listeners: fires when their pre-recovery work has completed
                    ev(nodeName, "beforeIndexShardRecovery(last)", indexShard.shardId().getIndexName(), indexShard.shardId().id(), "");
                    listener.onResponse(null);
                }

                @Override
                public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
                    ev(nodeName, "afterIndexShardRecovery(last)", indexShard.shardId().getIndexName(), indexShard.shardId().id(), "");
                    listener.onResponse(null);
                }

                @Override
                public void afterIndexShardStarted(IndexShard indexShard) {
                    ev(nodeName, "afterIndexShardStarted", indexShard.shardId().getIndexName(), indexShard.shardId().id(), "");
                }
            });
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // log capture (debug logs of the recovery / warming / unhollow paths)
    // ---------------------------------------------------------------------------------------------------------------------------

    private static final List<String> CAPTURED_LOGGERS = List.of(
        "org.elasticsearch.xpack.stateless.recovery.TransportSendRecoveryCommitRegistrationAction",
        "org.elasticsearch.xpack.stateless.recovery.StatelessSearchNodeRecoveryListener",
        "org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService",
        "org.elasticsearch.xpack.stateless.commits.HollowShardsService",
        "org.elasticsearch.indices.cluster.IndicesClusterStateService"
    );

    private static final Pattern SHARD_IN_MSG = Pattern.compile("\\[([a-z0-9_\\-]+)\\]\\[(\\d+)\\]");

    static final class Capture extends AbstractAppender {
        Capture() {
            super("s0-capture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            if (recording == false) {
                return;
            }
            String msg = event.getMessage().getFormattedMessage();
            String index = "";
            int shard = -1;
            Matcher m = SHARD_IN_MSG.matcher(msg);
            if (m.find()) {
                index = m.group(1);
                shard = Integer.parseInt(m.group(2));
            }
            String logger = event.getLoggerName();
            logger = logger.substring(logger.lastIndexOf('.') + 1);
            String detail = msg.length() > 300 ? msg.substring(0, 300) : msg;
            EVENTS.add(new Ev(System.nanoTime(), nodeOfThread(event.getThreadName()), "log:" + logger, index, shard, detail));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // latency + bandwidth injecting object store
    // ---------------------------------------------------------------------------------------------------------------------------

    static final class LatencyStore extends StatelessMockRepositoryStrategy {
        final String node;
        volatile long latencyNanos;
        volatile long bytesPerSecond;
        final AtomicLong pipeFreeAt = new AtomicLong();

        LatencyStore(String node) {
            this.node = node;
        }

        void configure(long latencyMs, long bps) {
            this.latencyNanos = TimeUnit.MILLISECONDS.toNanos(latencyMs);
            this.bytesPerSecond = bps;
        }

        /**
         * Step 2b harness brackets. {@code perConnectionBps > 0}: every request streams at most {@code perConnectionBps} on its own
         * connection, and all requests of the node share {@code bytesPerSecond} (the configured bandwidth becomes the aggregate cap);
         * otherwise the configured bandwidth is one shared pipe (the default). {@code jitterSigma > 0}: the first-byte latency of each
         * request is lognormal with median L and shape sigma (sigma = ln(3) / 2.326 = 0.472 gives p99 = 3 L).
         */
        static volatile long perConnectionBps = 0L;
        static volatile double jitterSigma = 0.0d;

        /** Re-entrant calls (a wrapped container method delegating to another wrapped method) must not pay latency twice. */
        private static final ThreadLocal<Boolean> IN_CALL = ThreadLocal.withInitial(() -> false);

        private Req start(String op, String blob, long requested) {
            BlobPath p = CURRENT_BLOB_PATH.get();
            Req r = new Req(node, op, p == null ? "" : p.buildAsString(), blob, requested);
            if (recording) {
                REQUESTS.add(r);
            }
            return r;
        }

        private void firstByte() {
            long l = latencyNanos;
            final double sigma = jitterSigma;
            if (l > 0 && sigma > 0) {
                l = (long) (l * Math.exp(sigma * java.util.concurrent.ThreadLocalRandom.current().nextGaussian()));
            }
            if (l > 0) {
                sleepUntil(System.nanoTime() + l);
            }
        }

        /** Node-wide shared pipe: each chunk reserves size/BW on a timeline and waits for its slot. */
        private void pace(long n) {
            pace(n, null);
        }

        /**
         * As {@link #pace(long)}; with a per-connection bandwidth the chunk also reserves size/per-connection-BW on the request's own
         * timeline {@code connection}, and waits for the later of the two slots.
         */
        private void pace(long n, AtomicLong connection) {
            long bps = bytesPerSecond;
            if (bps <= 0 || n <= 0) {
                return;
            }
            final long cost = n * 1_000_000_000L / bps;
            final long now = System.nanoTime();
            long due = pipeFreeAt.accumulateAndGet(cost, (prev, c) -> Math.max(prev, now) + c);
            final long perConnection = perConnectionBps;
            if (perConnection > 0 && connection != null) {
                final long connCost = n * 1_000_000_000L / perConnection;
                due = Math.max(due, connection.accumulateAndGet(connCost, (prev, c) -> Math.max(prev, now) + c));
            }
            if (due - System.nanoTime() > 500_000L) {
                sleepUntil(due);
            }
        }

        private static void sleepUntil(long deadline) {
            long rem;
            while ((rem = deadline - System.nanoTime()) > 0) {
                LockSupport.parkNanos(rem);
            }
        }

        private <T> T list(String op, CheckedSupplier<T, IOException> s) throws IOException {
            if (IN_CALL.get()) {
                return s.get();
            }
            Req r = start(op, "", 0);
            firstByte();
            IN_CALL.set(true);
            try {
                return s.get();
            } finally {
                IN_CALL.set(false);
                r.tFirstByte = r.tEnd = System.nanoTime();
            }
        }

        private void put(String blob, long size, CheckedRunnable<IOException> run) throws IOException {
            if (IN_CALL.get()) {
                run.run();
                return;
            }
            Req r = start("PUT", blob, size);
            firstByte();
            pace(size, new AtomicLong());
            IN_CALL.set(true);
            try {
                run.run();
            } finally {
                IN_CALL.set(false);
                r.bytes.set(Math.max(size, 0));
                r.tFirstByte = r.tEnd = System.nanoTime();
            }
        }

        private InputStream get(String blob, long requested, CheckedSupplier<InputStream, IOException> s) throws IOException {
            if (IN_CALL.get()) {
                return s.get();
            }
            final Req r = start("GET", blob, requested);
            firstByte();
            final InputStream in;
            IN_CALL.set(true);
            try {
                in = s.get();
            } catch (IOException | RuntimeException e) {
                r.tFirstByte = r.tEnd = System.nanoTime();
                throw e;
            } finally {
                IN_CALL.set(false);
            }
            r.tFirstByte = System.nanoTime();
            final AtomicLong connection = new AtomicLong();
            return new FilterInputStream(in) {
                @Override
                public int read() throws IOException {
                    int b = super.read();
                    if (b >= 0) {
                        r.bytes.incrementAndGet();
                        pace(1, connection);
                    } else {
                        r.tEnd = System.nanoTime();
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int n = super.read(b, off, len);
                    if (n > 0) {
                        r.bytes.addAndGet(n);
                        pace(n, connection);
                    } else if (n < 0) {
                        r.tEnd = System.nanoTime();
                    }
                    return n;
                }

                @Override
                public void close() throws IOException {
                    if (r.tEnd == 0) {
                        r.tEnd = System.nanoTime();
                    }
                    super.close();
                }
            };
        }

        @Override
        public Map<String, BlobContainer> blobContainerChildren(
            CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            return list("LIST", originalSupplier);
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobs(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            return list("LIST", originalSupplier);
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobsByPrefix(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobNamePrefix
        ) throws IOException {
            return list("LIST", originalSupplier);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            return get(blobName, -1, originalSupplier);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            return get(blobName, length, originalSupplier);
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
            put(blobName, blobSize, originalRunnable);
        }

        @Override
        public void blobContainerWriteBlob(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            BytesReference bytes,
            boolean failIfAlreadyExists
        ) throws IOException {
            put(blobName, bytes.length(), originalRunnable);
        }

        @Override
        public void blobContainerWriteBlobAtomic(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            String blobName,
            long blobSize,
            BlobContainer.BlobMultiPartInputStreamProvider provider,
            boolean failIfAlreadyExists,
            java.util.concurrent.Executor executor
        ) throws IOException {
            put(blobName, blobSize, originalRunnable);
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
            put(blobName, blobSize, originalRunnable);
        }

        @Override
        public void blobContainerWriteMetadataBlob(
            CheckedRunnable<IOException> original,
            OperationPurpose purpose,
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) throws IOException {
            put(blobName, -1, original);
        }

        @Override
        public void blobContainerCopyBlob(
            CheckedRunnable<IOException> originalRunnable,
            OperationPurpose purpose,
            BlobContainer sourceBlobContainer,
            String sourceBlobName,
            String blobName,
            long blobSize
        ) throws IOException {
            put(blobName, -1, originalRunnable);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // profiles
    // ---------------------------------------------------------------------------------------------------------------------------

    record Profile(
        String name,
        int shards,
        int rounds,
        int docsPerRound,
        double updateFraction,
        int wideFields,
        String regionSize,
        String cacheSize,
        int wordsPerDoc
    ) {}

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(StepRecorderPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0)
            // pin the base class's randomized knobs to production defaults
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING.getKey(), warmingRatio);
    }

    /** Recovery warming ratio (stateless default 0; serverless uses 1.0 inside the boost window). Set per test before nodes start. */
    private double warmingRatio = 0.0d;

    /** Z2-like: 20k docs, 4 fields, 5 BCCs of ~3 MB, default 16 MiB regions. */
    public void testSmallOneShard() throws Exception {
        run(new Profile("small-1sh", 1, 4, 5000, 0.0, 0, "16mb", "256mb", 30));
    }

    public void testSmallFiveShards() throws Exception {
        run(new Profile("small-5sh", 5, 4, 5000, 0.0, 0, "16mb", "256mb", 30));
    }

    /** As above, but with a cache large enough (64 regions) that the 25 BCC blobs (one 16 MiB region each) do not thrash it. */
    public void testSmallFiveShardsLargeCache() throws Exception {
        run(new Profile("small-5sh-1g", 5, 4, 5000, 0.0, 0, "16mb", "1gb", 30));
    }

    /** Several segments, BCCs larger than a 16 MiB region, soft deletes from updates. */
    public void testMultiRegionDefaultRegion() throws Exception {
        run(new Profile("multi-1sh-16m", 1, 6, 40000, 0.1, 0, "16mb", "512mb", 40));
    }

    public void testMultiRegionSmallRegion() throws Exception {
        run(new Profile("multi-1sh-1m", 1, 6, 40000, 0.1, 0, "1mb", "512mb", 40));
    }

    /** Same multi-region shard, recovery warming ratio 1.0 (serverless inside the boost window). */
    public void testMultiRegionDefaultRegionWarmRatio1() throws Exception {
        warmingRatio = 1.0d;
        run(new Profile("multi-1sh-16m-ratio1", 1, 6, 40000, 0.1, 0, "16mb", "512mb", 40));
    }

    /** ~1000-field sparse mapping, small data volume, updates. */
    public void testWideMapping() throws Exception {
        run(new Profile("wide1000-1sh", 1, 4, 5000, 0.1, 1000, "16mb", "256mb", 30));
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // scenario
    // ---------------------------------------------------------------------------------------------------------------------------

    private String[] vocabulary;

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
        final Settings cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), p.regionSize())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), p.regionSize())
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), p.cacheSize())
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
        knownNodes = nodes;
        final Map<String, String> tiers = Map.of(master, "master", indexNodeA, "index-A", indexNodeB, "index-B", searchNode, "search");

        final Map<String, LatencyStore> stores = new HashMap<>();
        for (String n : nodes) {
            LatencyStore s = new LatencyStore(n);
            stores.put(n, s);
            setNodeRepositoryStrategy(n, s);
        }

        final String indexName = "s0-" + p.name();
        Settings.Builder is = indexSettings(p.shards(), 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeB)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put("index.mapping.total_fields.limit", 5000);
        var create = prepareCreate(indexName).setSettings(is).setMapping(mapping(p));
        long tc = System.nanoTime();
        org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked(create);
        ensureGreen(indexName);
        log("%s create index %d ms", p.name(), ms(System.nanoTime() - tc));
        final Index index = resolveIndex(indexName);

        // ---------------- load ----------------
        long tl = System.nanoTime();
        int nextId = 0;
        Set<Integer> ids = new HashSet<>();
        for (int r = 0; r < p.rounds(); r++) {
            List<Integer> batchIds = new ArrayList<>();
            for (int i = 0; i < p.docsPerRound(); i++) {
                batchIds.add(nextId++);
            }
            if (r > 0 && p.updateFraction() > 0) {
                int updates = (int) (p.docsPerRound() * p.updateFraction());
                for (int i = 0; i < updates; i++) {
                    batchIds.add(rnd.nextInt(nextId - p.docsPerRound()));
                }
            }
            bulk(indexName, batchIds, p, rnd);
            ids.addAll(batchIds);
            flush(indexName);
        }
        final long expectedDocs = ids.size();
        long extraDocs = 0;
        log("%s loaded %d unique docs (%d index ops) in %d ms", p.name(), expectedDocs, nextId, ms(System.nanoTime() - tl));

        // blob inventory
        logBlobInventory(p, indexName, index);

        // ---------------- go to zero: hollow the indexing shards (A -> B) ----------------
        hollowShards(indexName, p.shards(), indexNodeA, indexNodeB);
        String hollowNode = indexNodeB;
        String otherIndexNode = indexNodeA;

        Loggers.setLevel(LogManager.getLogger(CAPTURED_LOGGERS.get(0)), Level.DEBUG);
        Capture capture = new Capture();
        capture.start();
        for (String l : CAPTURED_LOGGERS) {
            Logger lg = LogManager.getLogger(l);
            Loggers.setLevel(lg, Level.DEBUG);
            Loggers.addAppender(lg, capture);
        }
        installTransportProbes(nodes, searchNode);

        try {
            for (int latency : LATENCIES_MS) {
                for (String n : nodes) {
                    stores.get(n).configure(0, 0);
                }
                assertThat(indexService(searchNode, index), nullValue());
                for (int i = 0; i < p.shards(); i++) {
                    assertThat(findIndexShard(index, i).getEngineOrNull(), instanceOf(HollowIndexEngine.class));
                }
                int evicted = 0;
                for (String n : List.of(indexNodeA, indexNodeB, searchNode)) {
                    evicted += cacheService(n).forceEvict(k -> k.shardId().getIndex().equals(index));
                }
                quiesce(2000);
                log(
                    "%s L=%d: zero state reached (indexing shards hollow on %s), force-evicted %d regions",
                    p.name(),
                    latency,
                    tiers.get(hollowNode),
                    evicted
                );
                for (String n : nodes) {
                    stores.get(n).configure(latency, BANDWIDTH_BYTES_PER_SEC);
                }

                // ---------------- (a) search activation ----------------
                EVENTS.clear();
                REQUESTS.clear();
                recording = true;
                final long t0 = System.nanoTime();
                final var ackFuture = new org.elasticsearch.action.support.PlainActionFuture<
                    org.elasticsearch.action.support.master.AcknowledgedResponse>();
                final AtomicLong ackTime = new AtomicLong();
                client(master).admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                    .execute(ActionListener.runBefore(ackFuture, () -> ackTime.set(System.nanoTime())));
                final String word = vocabulary[17];
                long firstPartial = 0;
                long firstHit = 0;
                long firstHitTook = -1;
                long hitsOnWord = -1;
                int attempts = 0;
                long tAck = 0;
                while (firstHit == 0) {
                    if (System.nanoTime() - t0 > TimeUnit.SECONDS.toNanos(120)) {
                        fail("no full search hit within 120s");
                    }
                    attempts++;
                    long ts = System.nanoTime();
                    try {
                        SearchResponse resp = client(searchNode).prepareSearch(indexName)
                            .setQuery(QueryBuilders.matchQuery("text", word))
                            .setSize(10)
                            .setTrackTotalHits(true)
                            .get();
                        try {
                            long te = System.nanoTime();
                            if (resp.getSuccessfulShards() > 0 && firstPartial == 0) {
                                firstPartial = te;
                            }
                            if (resp.getFailedShards() == 0 && resp.getSuccessfulShards() == p.shards()) {
                                firstHit = te;
                                firstHitTook = ms(te - ts);
                                hitsOnWord = resp.getHits().getTotalHits().value();
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
                ackFuture.actionGet();
                tAck = ackTime.get();
                ev(searchNode, "client:firstFullHit", indexName, -1, "took=" + firstHitTook + "ms attempts=" + attempts);
                long t2s = System.nanoTime();
                long hits2 = countMatch(searchNode, indexName, word);
                long secondMs = ms(System.nanoTime() - t2s);
                quiesce(1000);
                recording = false;
                // correctness
                long all = countAll(searchNode, indexName);
                assertThat(all, equalTo(expectedDocs + extraDocs));
                assertThat(hits2, equalTo(hitsOnWord));
                reportSearch(
                    p,
                    latency,
                    index,
                    t0,
                    tAck,
                    firstPartial,
                    firstHit,
                    firstHitTook,
                    secondMs,
                    attempts,
                    hitsOnWord,
                    nodes,
                    tiers,
                    searchNode,
                    hollowNode
                );

                // ---------------- (b) first write on a hollow indexing shard ----------------
                quiesce(500);
                EVENTS.clear();
                REQUESTS.clear();
                recording = true;
                final long tw0 = System.nanoTime();
                DocWriteResponse w1 = client(master).prepareIndex(indexName)
                    .setId("wake-" + latency)
                    .setRouting("r0")
                    .setSource(doc(p, rnd, nextId++))
                    .get();
                final long tw1 = System.nanoTime();
                ev(master, "client:firstWriteAck", indexName, w1.getShardId().id(), "");
                final long tw2s = System.nanoTime();
                client(master).prepareIndex(indexName).setId("wake2-" + latency).setRouting("r0").setSource(doc(p, rnd, nextId++)).get();
                extraDocs += 2;
                final long tw2 = System.nanoTime();
                quiesce(1000);
                recording = false;
                reportWrite(p, latency, index, w1.getShardId().id(), tw0, tw1, ms(tw2 - tw2s), nodes, tiers, hollowNode, searchNode);

                // unhollow the remaining shards (not measured), then go back to zero
                for (String n : nodes) {
                    stores.get(n).configure(0, 0);
                }
                Set<Integer> unhollowed = new HashSet<>();
                unhollowed.add(w1.getShardId().id());
                for (int r = 1; unhollowed.size() < p.shards() && r < 10_000; r++) {
                    DocWriteResponse wr = client(master).prepareIndex(indexName)
                        .setId("fill-" + latency + "-" + r)
                        .setRouting("r" + r)
                        .setSource(doc(p, rnd, nextId++))
                        .get();
                    unhollowed.add(wr.getShardId().id());
                    extraDocs++;
                }
                setReplicaCount(0, indexName);
                assertBusy(() -> assertThat(indexService(searchNode, index), nullValue()));
                // wait until every indexing shard is unhollowed before relocating
                assertBusy(() -> {
                    for (int i = 0; i < p.shards(); i++) {
                        assertThat(
                            findIndexShard(index, i).getEngineOrNull(),
                            instanceOf(org.elasticsearch.xpack.stateless.engine.IndexEngine.class)
                        );
                    }
                });
                flush(indexName);
                hollowShards(indexName, p.shards(), hollowNode, otherIndexNode);
                String tmp = hollowNode;
                hollowNode = otherIndexNode;
                otherIndexNode = tmp;
            }
        } finally {
            recording = false;
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

    // ---------------------------------------------------------------------------------------------------------------------------
    // transport probes
    // ---------------------------------------------------------------------------------------------------------------------------

    private void installTransportProbes(List<String> nodes, String searchNode) {
        MockTransportService search = MockTransportService.getInstance(searchNode);
        search.addRequestHandlingBehavior(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME, (handler, request, channel, task) -> {
            ev(searchNode, "transport:publish_state recv", "", -1, "");
            handler.messageReceived(request, channel, task);
        });
        search.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
            ev(searchNode, "transport:commit_state recv", "", -1, String.valueOf(request));
            handler.messageReceived(request, channel, task);
        });
        search.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(ShardStateAction.SHARD_STARTED_ACTION_NAME)) {
                String s = String.valueOf(request);
                Matcher m = SHARD_IN_MSG.matcher(s);
                if (m.find()) {
                    ev(searchNode, "transport:shard_started sent", m.group(1), Integer.parseInt(m.group(2)), "");
                } else {
                    ev(searchNode, "transport:shard_started sent", "", -1, s);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        for (String n : nodes) {
            if (n.equals(searchNode)) {
                continue;
            }
            MockTransportService.getInstance(n)
                .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                    String s = String.valueOf(request);
                    Matcher m = SHARD_IN_MSG.matcher(s);
                    final String idx = m.find() ? m.group(1) : "";
                    final int sh = idx.isEmpty() ? -1 : Integer.parseInt(m.group(2));
                    ev(n, "transport:register_commit handler start", idx, sh, "");
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            ev(n, "transport:register_commit handler end", idx, sh, "");
                            channel.sendResponse(response);
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            ev(n, "transport:register_commit handler end", idx, sh, "failed " + exception);
                            channel.sendResponse(exception);
                        }

                        @Override
                        public org.elasticsearch.TransportVersion getVersion() {
                            return channel.getVersion();
                        }
                    }, task);
                });
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // reporting
    // ---------------------------------------------------------------------------------------------------------------------------

    private static double rel(long t, long t0) {
        return t == 0 ? Double.NaN : (t - t0) / 1_000_000.0;
    }

    private static List<Ev> events(Predicate<Ev> p) {
        List<Ev> out = new ArrayList<>();
        for (Ev e : EVENTS) {
            if (p.test(e)) {
                out.add(e);
            }
        }
        out.sort((a, b) -> Long.compare(a.t(), b.t()));
        return out;
    }

    private static long first(Predicate<Ev> p) {
        List<Ev> l = events(p);
        return l.isEmpty() ? 0 : l.get(0).t();
    }

    private static long last(Predicate<Ev> p) {
        List<Ev> l = events(p);
        return l.isEmpty() ? 0 : l.get(l.size() - 1).t();
    }

    private static final Pattern PATH_SHARD = Pattern.compile("indices/([^/]+)/(\\d+)/");

    static int shardOfPath(String path, String uuid) {
        Matcher m = PATH_SHARD.matcher(path);
        if (m.find() && m.group(1).equals(uuid)) {
            return Integer.parseInt(m.group(2));
        }
        return -1;
    }

    /** Counts of requests started in [from, to) matching the predicate. */
    private static String window(long from, long to, Predicate<Req> p) {
        if (from == 0 || to == 0 || to < from) {
            return "| - | - | - | - |";
        }
        int list = 0, get = 0, put = 0;
        long bytes = 0;
        for (Req r : REQUESTS) {
            if (r.tStart >= from && r.tStart < to && p.test(r)) {
                switch (r.op) {
                    case "LIST" -> list++;
                    case "GET" -> {
                        get++;
                        bytes += r.bytes.get();
                    }
                    case "PUT" -> put++;
                }
            }
        }
        return String.format(Locale.ROOT, "| %d | %d | %d | %s |", list, get, put, mb(bytes));
    }

    private static String mb(long bytes) {
        return bytes < 1024 * 1024
            ? String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0)
            : String.format(Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }

    private static String row(String step, long from, long to, long t0, Predicate<Req> p) {
        if (to == Long.MAX_VALUE) {
            return String.format(Locale.ROOT, "| %s | %.1f | end | - ", step, rel(from, t0)) + window(from, to, p);
        }
        String dur = (from == 0 || to == 0) ? "-" : String.format(Locale.ROOT, "%.1f", (to - from) / 1_000_000.0);
        return String.format(Locale.ROOT, "| %s | %.1f | %.1f | %s ", step, rel(from, t0), rel(to, t0), dur) + window(from, to, p);
    }

    private void reportSearch(
        Profile p,
        int latency,
        Index index,
        long t0,
        long tAck,
        long firstPartial,
        long firstHit,
        long firstHitTook,
        long secondMs,
        int attempts,
        long hits,
        List<String> nodes,
        Map<String, String> tiers,
        String searchNode,
        String hollowNode
    ) {
        final String tag = p.name() + " L=" + latency;
        final String idx = index.getName();
        final String uuid = index.getUUID();
        log("%s ==== SEARCH ACTIVATION (replicas 0->1, cold cache, BW=%d MB/s) ====", tag, BANDWIDTH_BYTES_PER_SEC >> 20);
        log(
            "%s settings-ack=%.1f firstPartialHit=%.1f firstFullHit=%.1f (first successful query took %d ms, %d polls, %d hits on word);"
                + " second query %d ms",
            tag,
            rel(tAck, t0),
            rel(firstPartial, t0),
            rel(firstHit, t0),
            firstHitTook,
            attempts,
            hits,
            secondMs
        );

        // node-level events on the search node
        long pub = first(e -> e.node().equals(searchNode) && e.kind().equals("transport:publish_state recv") && e.t() >= t0);
        long commit = first(e -> e.node().equals(searchNode) && e.kind().equals("transport:commit_state recv") && e.t() >= pub);
        long ixB = first(e -> e.node().equals(searchNode) && e.kind().equals("beforeIndexCreated") && e.index().equals(idx));
        long ixA = first(e -> e.node().equals(searchNode) && e.kind().equals("afterIndexCreated") && e.index().equals(idx));

        IndicesService searchIndices = internalCluster().getInstance(IndicesService.class, searchNode);
        IndexService searchIs = searchIndices.indexServiceSafe(index);

        Predicate<Req> onSearch = r -> r.node.equals(searchNode);
        for (int s = 0; s < p.shards(); s++) {
            final int sh = s;
            Predicate<Ev> mine = e -> e.node().equals(searchNode) && e.index().equals(idx) && e.shard() == sh;
            long shB = first(e -> mine.test(e) && e.kind().equals("beforeIndexShardCreated"));
            long shA = first(e -> mine.test(e) && e.kind().equals("afterIndexShardCreated"));
            long recovering = first(e -> mine.test(e) && e.kind().equals("state:RECOVERING"));
            long regSend = first(
                e -> mine.test(e)
                    && e.kind().endsWith("TransportSendRecoveryCommitRegistrationAction")
                    && e.detail().contains("sending recovery commit registration")
            );
            long regRecv = first(
                e -> mine.test(e)
                    && e.kind().endsWith("TransportSendRecoveryCommitRegistrationAction")
                    && e.detail().contains("received registration response")
            );
            long regHs = first(e -> e.index().equals(idx) && e.shard() == sh && e.kind().equals("transport:register_commit handler start"));
            long regHe = first(e -> e.index().equals(idx) && e.shard() == sh && e.kind().equals("transport:register_commit handler end"));
            long postRec = first(e -> mine.test(e) && e.kind().equals("state:POST_RECOVERY"));
            long startedSent = first(e -> mine.test(e) && e.kind().equals("transport:shard_started sent"));
            long started = first(e -> mine.test(e) && e.kind().equals("state:STARTED"));
            long warmStart = first(
                e -> mine.test(e) && e.kind().endsWith("SharedBlobCacheWarmingService") && e.detail().contains(" warming, ")
            );
            long warmEnd = last(
                e -> mine.test(e) && e.kind().endsWith("SharedBlobCacheWarmingService") && e.detail().contains("completed in")
            );

            IndexShard shard = searchIs.getShard(sh);
            RecoveryState rs = shard.recoveryState();
            long recStart = timerStartNanos(rs.getTimer());
            long idxStageStart = timerStartNanos(rs.getIndex());
            long openStart = timerStartNanos(rs.getTranslog());
            long openEnd = openStart == 0 ? 0 : openStart + TimeUnit.MILLISECONDS.toNanos(rs.getTranslog().time());
            long recDone = recStart == 0 ? 0 : recStart + TimeUnit.MILLISECONDS.toNanos(rs.getTimer().time());

            Predicate<Req> shardReqs = r -> onSearch.test(r) && shardOfPath(r.path, uuid) == sh;
            log(
                "%s shard %d: search-node step table (ms from settings update; requests = search-node object-store requests"
                    + " started in the window for this shard's blobs; bytes = bytes delivered)",
                tag,
                sh
            );
            log("| step | start | end | dur | LIST | GET | PUT | bytes |");
            log("|---|---|---|---|---|---|---|---|");
            log(row("S2 update-settings sent -> publish_state received on search node", t0, pub, t0, shardReqs));
            log(
                row(
                    "S2 .. master object-store requests until commit received (cluster-state persistence)",
                    t0,
                    commit,
                    t0,
                    r -> r.node.equals(nodes.get(0))
                )
            );
            log(row("S2 publish -> commit_state received (apply starts)", pub, commit, t0, shardReqs));
            log(row("S2 commit -> beforeIndexCreated", commit, ixB, t0, shardReqs));
            log(row("S3 IndexService ctor (beforeIndexCreated -> afterIndexCreated)", ixB, ixA, t0, shardReqs));
            log(row("S3 mapping merge/parse (afterIndexCreated -> beforeIndexShardCreated)", ixA, shB, t0, shardReqs));
            log(row("S3 IndexShard ctor (before -> afterIndexShardCreated)", shB, shA, t0, shardReqs));
            log(row("S3 shard created -> RECOVERING (recovery start)", shA, recovering, t0, shardReqs));
            log(
                row(
                    "S4+S5 commit discovery (LISTs) + BCC header read (RECOVERING -> registration sent)",
                    recovering,
                    regSend,
                    t0,
                    shardReqs
                )
            );
            log(row("S6 registration round trip (sent -> response received)", regSend, regRecv, t0, shardReqs));
            log(row("S6 .. of which indexing-node handler", regHs, regHe, t0, r -> false));
            log(row("S7+S8 referenced headers + warming kick-off (response -> pre-recovery done)", regRecv, idxStageStart, t0, shardReqs));
            log(row("S8 recovery warming (start -> last completion)", warmStart, warmEnd, t0, shardReqs));
            log(row("pre-recovery done -> engine open start (INDEX/VERIFY stages)", idxStageStart, openStart, t0, shardReqs));
            log(row("S9 engine open: SearchEngine ctor / DirectoryReader open (TRANSLOG stage)", openStart, openEnd, t0, shardReqs));
            log(row("S10 finalize + post_recovery refresh (-> POST_RECOVERY)", openEnd, postRec, t0, shardReqs));
            log(row("S11 POST_RECOVERY -> shard-started RPC sent", postRec, startedSent, t0, shardReqs));
            log(row("S11 shard-started -> STARTED applied on search node (P2)", startedSent, started, t0, shardReqs));
            log(
                row(
                    "S12 POST_RECOVERY -> first full hit (query was dispatched to the INITIALIZING copy, held by the gate)",
                    postRec,
                    firstHit,
                    t0,
                    shardReqs
                )
            );
            log(
                row(
                    "info: successful query dispatched -> returned",
                    firstHit - TimeUnit.MILLISECONDS.toNanos(firstHitTook),
                    firstHit,
                    t0,
                    shardReqs
                )
            );
            log(row("TOTAL update -> first full hit", t0, firstHit, t0, shardReqs));
            log(row("TOTAL update -> STARTED", t0, started, t0, shardReqs));
            log(row("after first hit (warming tail)", firstHit, Long.MAX_VALUE, t0, shardReqs));
            log(
                "%s shard %d recoveryState: timer start=%.1f done=%.1f indexStage=%.1f translogStage(open)=%.1f+%d ms; reg handler %.1f-%.1f",
                tag,
                sh,
                rel(recStart, t0),
                rel(recDone, t0),
                rel(idxStageStart, t0),
                rel(openStart, t0),
                rs.getTranslog().time(),
                rel(regHs, t0),
                rel(regHe, t0)
            );
        }
        // per-node / per-pool totals
        for (String n : nodes) {
            log(
                "%s totals node=%s to-first-hit %s | after %s",
                tag,
                tiers.get(n),
                window(t0, firstHit, r -> r.node.equals(n)),
                window(firstHit, Long.MAX_VALUE, r -> r.node.equals(n))
            );
        }
        Map<String, long[]> byPool = new TreeMap<>();
        for (Req r : REQUESTS) {
            if (r.node.equals(searchNode)) {
                long[] v = byPool.computeIfAbsent(r.op + "@" + r.pool() + (r.tStart < firstHit ? " (<hit)" : " (>hit)"), k -> new long[2]);
                v[0]++;
                v[1] += r.bytes.get();
            }
        }
        byPool.forEach((k, v) -> log("%s search-node requests by op@pool %s: n=%d bytes=%s", tag, k, v[0], mb(v[1])));
        dumpTimeline(tag, t0, firstHit + TimeUnit.SECONDS.toNanos(5), searchNode);
    }

    private void reportWrite(
        Profile p,
        int latency,
        Index index,
        int shardId,
        long t0,
        long ack,
        long secondWriteMs,
        List<String> nodes,
        Map<String, String> tiers,
        String hollowNode,
        String searchNode
    ) {
        final String tag = p.name() + " L=" + latency;
        final String idx = index.getName();
        log(
            "%s ==== FIRST WRITE ON HOLLOW SHARD %d (unhollow) ==== ack=%.1f ms; second write (same shard) %d ms",
            tag,
            shardId,
            rel(ack, t0),
            secondWriteMs
        );
        Predicate<Ev> hs = e -> e.node().equals(hollowNode)
            && e.kind().endsWith("HollowShardsService")
            && e.index().equals(idx)
            && e.shard() == shardId;
        long blocker = first(
            e -> hs.test(e) && (e.detail().contains("adding ingestion operation") || e.detail().contains("acquiring primary permit"))
        );
        long uStart = first(e -> hs.test(e) && e.detail().contains("unhollowing shard (reason"));
        long stateRead = first(
            e -> e.node().equals(hollowNode)
                && e.kind().endsWith("HollowShardsService")
                && e.detail().contains("Updating BlobStoreCacheDirectory metadata")
                && e.t() >= uStart
        );
        long flushing = first(e -> hs.test(e) && e.detail().contains("Flushing shard"));
        long unhollowed = first(e -> hs.test(e) && e.detail().contains("unhollowed shard with gen"));
        long warmEnd = last(
            e -> e.node().equals(hollowNode) && e.kind().endsWith("SharedBlobCacheWarmingService") && e.detail().contains("completed in")
        );
        Predicate<Req> onHollow = r -> r.node.equals(hollowNode);
        log(
            "%s write step table (node %s holds the hollow shard; requests = all object-store requests on that node)",
            tag,
            tiers.get(hollowNode)
        );
        log("| step | start | end | dur | LIST | GET | PUT | bytes |");
        log("|---|---|---|---|---|---|---|---|");
        log(row("W1 client -> ingestion blocked on hollow shard (routing, coordinator, primary)", t0, blocker, t0, onHollow));
        log(row("W1 blocked -> unhollow task starts", blocker, uStart, t0, onHollow));
        log(row("W4 readIndexingShardState (LIST + BCC headers)", uStart, stateRead, t0, onHollow));
        log(row("W5+W6 pre-warm kick-off + engine reset (IndexWriter open) + refresh", stateRead, flushing, t0, onHollow));
        log(row("W7 unhollow flush (commit upload)", flushing, unhollowed, t0, onHollow));
        log(row("W9 op + translog sync -> ack", unhollowed, ack, t0, onHollow));
        log(row("TOTAL client -> ack", t0, ack, t0, onHollow));
        log(row("unhollow warming (last completion)", uStart, warmEnd, t0, onHollow));
        log(row("after ack", ack, Long.MAX_VALUE, t0, onHollow));
        for (String n : nodes) {
            log(
                "%s write totals node=%s to-ack %s | after %s",
                tag,
                tiers.get(n),
                window(t0, ack, r -> r.node.equals(n)),
                window(ack, Long.MAX_VALUE, r -> r.node.equals(n))
            );
        }
        Map<String, long[]> byKind = new TreeMap<>();
        for (Req r : REQUESTS) {
            if (r.node.equals(hollowNode) && r.tStart < ack) {
                String kind = r.path.contains("/translog")
                    ? "translog"
                    : (r.blob.startsWith("stateless_commit") ? "bcc" : r.blob.replaceAll("[0-9]", ""));
                long[] v = byKind.computeIfAbsent(r.op + ":" + kind + "@" + r.pool(), k -> new long[3]);
                v[0]++;
                v[1] += r.bytes.get();
                v[2] = Math.max(v[2], r.tEnd - r.tStart);
            }
        }
        byKind.forEach(
            (k, v) -> log("%s hollow-node requests before ack %s: n=%d bytes=%s maxDur=%d ms", tag, k, v[0], mb(v[1]), ms(v[2]))
        );
        dumpTimeline(tag + " write", t0, ack + TimeUnit.SECONDS.toNanos(2), hollowNode);
    }

    private void dumpTimeline(String tag, long t0, long until, String focusNode) {
        List<Ev> evs = events(e -> e.t() >= t0 && e.t() <= until);
        for (Ev e : evs) {
            log(
                "%s TL %8.1f ev  %-8s %s [%s][%d] %s",
                tag,
                rel(e.t(), t0),
                e.node(),
                e.kind(),
                e.index(),
                e.shard(),
                e.detail().replace('\n', ' ')
            );
        }
        List<Req> rs = new ArrayList<>(REQUESTS);
        rs.sort((a, b) -> Long.compare(a.tStart, b.tStart));
        for (Req r : rs) {
            if (r.tStart >= t0 && r.tStart <= until) {
                log(
                    "%s TL %8.1f req %-8s %s %s/%s req=%d got=%d firstByte=+%.1f end=+%.1f pool=%s",
                    tag,
                    rel(r.tStart, t0),
                    r.node,
                    r.op,
                    r.path,
                    r.blob,
                    r.requested,
                    r.bytes.get(),
                    (r.tFirstByte - r.tStart) / 1e6,
                    r.tEnd == 0 ? Double.NaN : (r.tEnd - r.tStart) / 1e6,
                    r.pool()
                );
            }
        }
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

    private void logBlobInventory(Profile p, String indexName, Index index) throws Exception {
        var objectStore = getObjectStoreService(internalCluster().getMasterName());
        for (int s = 0; s < p.shards(); s++) {
            var container = objectStore.getProjectBlobStore(new org.elasticsearch.index.shard.ShardId(index, s))
                .blobContainer(objectStore.shardBasePath(new org.elasticsearch.index.shard.ShardId(index, s)));
            Map<String, Long> sizes = new TreeMap<>();
            long total = 0;
            for (var term : container.children(OperationPurpose.INDICES).entrySet()) {
                for (var b : term.getValue().listBlobs(OperationPurpose.INDICES).entrySet()) {
                    sizes.put(term.getKey() + "/" + b.getKey(), b.getValue().length());
                    total += b.getValue().length();
                }
            }
            log("%s shard %d blobs: n=%d total=%s %s", p.name(), s, sizes.size(), mb(total), sizes);
        }
        var segs = indicesAdmin().prepareSegments(indexName).get();
        segs.getIndices().get(indexName).forEach(iss -> {
            for (var shardSegs : iss.shards()) {
                long deleted = shardSegs.getSegments().stream().mapToLong(seg -> seg.getDeletedDocs()).sum();
                log(
                    "%s shard %d (%s) segments=%d deletedDocs=%d",
                    p.name(),
                    iss.shardId().id(),
                    shardSegs.getShardRouting().role(),
                    shardSegs.getSegments().size(),
                    deleted
                );
            }
        });
    }

    private void quiesce(long quietMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            long lastStart = 0;
            boolean inFlight = false;
            for (Req r : REQUESTS) {
                lastStart = Math.max(lastStart, r.tStart);
                if (r.tEnd == 0 && r.op.equals("GET") == false) {
                    inFlight = true;
                }
                if (r.op.equals("GET") && r.tEnd == 0 && System.nanoTime() - r.tStart < TimeUnit.SECONDS.toNanos(10)) {
                    inFlight = true;
                }
            }
            if (inFlight == false && System.nanoTime() - lastStart > TimeUnit.MILLISECONDS.toNanos(quietMs)) {
                return;
            }
            Thread.sleep(50);
        }
    }

    private static long countMatch(String node, String indexName, String word) {
        var resp = client(node).prepareSearch(indexName)
            .setQuery(QueryBuilders.matchQuery("text", word))
            .setSize(10)
            .setTrackTotalHits(true)
            .get();
        try {
            return resp.getHits().getTotalHits().value();
        } finally {
            resp.decRef();
        }
    }

    private static long countAll(String node, String indexName) {
        var resp = client(node).prepareSearch(indexName).setSize(0).setTrackTotalHits(true).get();
        try {
            return resp.getHits().getTotalHits().value();
        } finally {
            resp.decRef();
        }
    }

    private XContentBuilder mapping(Profile p) throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder().startObject().startObject("properties");
        b.startObject("text").field("type", "text").endObject();
        b.startObject("kw").field("type", "keyword").endObject();
        b.startObject("num").field("type", "long").endObject();
        b.startObject("ts").field("type", "date").endObject();
        for (int i = 0; i < p.wideFields(); i++) {
            String type = switch (i % 10) {
                case 0, 1, 2, 3 -> "keyword";
                case 4, 5, 6 -> "long";
                case 7, 8 -> "text";
                default -> "date";
            };
            b.startObject("f" + i).field("type", type).endObject();
        }
        return b.endObject().endObject();
    }

    private Map<String, Object> doc(Profile p, Random rnd, int id) {
        Map<String, Object> d = new HashMap<>();
        StringBuilder text = new StringBuilder();
        for (int w = 0; w < p.wordsPerDoc(); w++) {
            if (w > 0) {
                text.append(' ');
            }
            text.append(vocabulary[rnd.nextInt(VOCABULARY)]);
        }
        d.put("text", text.toString());
        d.put("kw", vocabulary[rnd.nextInt(VOCABULARY)]);
        d.put("num", rnd.nextLong());
        d.put("ts", 1_700_000_000_000L + id);
        for (int k = 0; k < Math.min(20, p.wideFields()); k++) {
            int f = rnd.nextInt(p.wideFields());
            Object v = switch (f % 10) {
                case 0, 1, 2, 3 -> vocabulary[rnd.nextInt(VOCABULARY)];
                case 4, 5, 6 -> rnd.nextLong();
                case 7, 8 -> vocabulary[rnd.nextInt(VOCABULARY)] + " " + vocabulary[rnd.nextInt(VOCABULARY)];
                default -> 1_700_000_000_000L + rnd.nextInt(1_000_000);
            };
            d.put("f" + f, v);
        }
        return d;
    }

    private void bulk(String indexName, List<Integer> ids, Profile p, Random rnd) {
        final int batch = 2000;
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

    private static long ms(long nanos) {
        return TimeValue.nsecToMSec(nanos);
    }

    private static void log(String format, Object... args) {
        LOG.info("S0| " + (args.length == 0 ? format : String.format(Locale.ROOT, format, args)));
    }
}
