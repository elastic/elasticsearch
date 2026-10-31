/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cheapshard;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.action.CheapShardUnparkOnWriteFilter;
import org.elasticsearch.xpack.stateless.allocation.CheapShardParkedAllocationDecider;
import org.elasticsearch.xpack.stateless.commits.CommitPointer;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.FastSearchActivationSettings;
import org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Cheap shards, phase 3 ("both tiers to zero"): an open index whose indexing copy is <em>parked</em> (unassigned, kept so by
 * {@link CheapShardParkedAllocationDecider}) and whose search copies are 0 has no {@code IndexService} on any node; it comes back by
 * clearing {@code index.cheap_shard.parked} (primary recovers from the object store with term + 1). See
 * {@code notes/phase3-both-tiers-zero.md}.
 * <p>
 * Parking does not reach "nothing on any node": the former primary's node keeps the shard directory under
 * {@code indices/&lt;uuid&gt;} (a few small files; {@code IndicesStore} deletes shard data only once all copies are started, which a
 * parked primary never is), and the data nodes keep the index's shared-cache regions until they are evicted (demoted, not freed; the
 * tests force-evict the search node's). The {@code residue} helper logs both; nothing asserts them to be zero.
 */
public class CheapShardParkIT extends AbstractStatelessPluginIntegTestCase {

    private static final org.apache.logging.log4j.Logger LOG = org.apache.logging.log4j.LogManager.getLogger(CheapShardParkIT.class);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(FastSearchActivationSettings.COMMIT_POINTER_ENABLED_SETTING.getKey(), true);
    }

    /** Data nodes only; pinned like ActivationLatencyIT: randomized region/range sizes (down to a few hundred KB) multiply the GETs. */
    static final Settings CACHE_SETTINGS = Settings.builder()
        .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "16mb")
        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), "16mb")
        .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "512mb")
        .build();

    final Map<String, ActivationLatencyIT.LatencyStore> stores = new HashMap<>();

    void setLatency(long millis) {
        stores.values().forEach(s -> s.configure(millis, 0L));
    }

    @org.junit.After
    public void stopRecording() {
        ActivationLatencyIT.recording = false;
        ActivationLatencyIT.REQUESTS.clear();
    }

    record Fixture(String master, List<String> indexNodes, String searchNode, String indexName, Index index, long docs) {
        ShardId shardId() {
            return new ShardId(index, 0);
        }

        List<String> dataNodes() {
            final List<String> nodes = new ArrayList<>(indexNodes);
            nodes.add(searchNode);
            return nodes;
        }
    }

    /** master, two index nodes, one search node; one index (1 shard, 0 replicas) with a few flushed commits and its pointer. */
    Fixture setUpIndex() throws Exception {
        final String master = startMasterOnlyNode();
        final List<String> indexNodes = List.of(startIndexNode(CACHE_SETTINGS), startIndexNode(CACHE_SETTINGS));
        final String searchNode = startSearchNode(CACHE_SETTINGS);
        ensureStableCluster(4);
        // object store latency/accounting of the step 2b harness (latency 0 until a measurement configures it)
        for (String node : List.of(master, indexNodes.get(0), indexNodes.get(1), searchNode)) {
            final var store = new ActivationLatencyIT.LatencyStore(node);
            stores.put(node, store);
            setNodeRepositoryStrategy(node, store);
        }
        final String indexName = "parked-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        long docs = 0;
        for (int r = 0; r < 3; r++) {
            final int n = randomIntBetween(100, 300);
            indexDocs(indexName, n);
            docs += n;
            flush(indexName);
        }
        final Fixture f = new Fixture(master, indexNodes, searchNode, indexName, resolveIndex(indexName), docs);
        awaitPointerOfLatestUpload(f);
        return f;
    }

    static long primaryTerm(Fixture f) {
        return clusterState().metadata().getProject().index(f.indexName).primaryTerm(0);
    }

    static ClusterState clusterState() {
        return internalCluster().clusterService(internalCluster().getMasterName()).state();
    }

    static ShardRouting primary(Fixture f) {
        return clusterState().routingTable().shardRoutingTable(f.shardId()).primaryShard();
    }

    void awaitPointerOfLatestUpload(Fixture f) throws Exception {
        final String node = clusterState().nodes().get(primary(f).currentNodeId()).getName();
        final PrimaryTermAndGeneration latest = internalCluster().getInstance(StatelessCommitService.class, node)
            .getLatestUploadedBcc(f.shardId())
            .primaryTermAndGeneration();
        assertBusy(() -> {
            final CommitPointer pointer = CommitPointer.read(
                getObjectStoreService(node).getProjectBlobContainer(f.shardId()),
                primaryTerm(f)
            );
            assertThat(pointer, notNullValue());
            assertThat(pointer.batchedCompoundCommitTermAndGeneration(), equalTo(latest));
        });
    }

    /**
     * Park: 0 search copies, parked=true, then fail the started primary once (reroute cancel with allow_primary); the decider keeps it
     * unassigned from then on. A routing change afterwards lets every node apply the state (and IndicesStore run).
     */
    void park(Fixture f) throws Exception {
        setReplicaCount(0, f.indexName);
        flush(f.indexName);
        awaitPointerOfLatestUpload(f);
        updateIndexSettings(Settings.builder().put(CheapShardParkedAllocationDecider.PARKED_SETTING.getKey(), true), f.indexName);
        final String primaryNode = clusterState().nodes().get(primary(f).currentNodeId()).getName();
        ClusterRerouteUtils.reroute(client(), new CancelAllocationCommand(f.indexName, 0, primaryNode, true));
        for (int i = 0; i < 3; i++) {
            ClusterRerouteUtils.reroute(client());
        }
        triggerRoutingChange();
        assertBusy(() -> {
            for (String node : f.dataNodes()) {
                assertThat(node, internalCluster().getInstance(IndicesService.class, node).indexService(f.index), nullValue());
            }
        });
        evictSearchCache(f);
    }

    void unpark(Fixture f) {
        updateIndexSettings(Settings.builder().put(CheapShardParkedAllocationDecider.PARKED_SETTING.getKey(), false), f.indexName);
    }

    void assertPrimaryParked(Fixture f) {
        final ShardRouting primary = primary(f);
        assertTrue(primary.toString(), primary.unassigned());
        assertThat(primary.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.REROUTE_CANCELLED));
        assertThat(primary.unassignedInfo().lastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.DECIDERS_NO));
    }

    void evictSearchCache(Fixture f) {
        internalCluster().getInstance(PluginsService.class, f.searchNode)
            .filterPlugins(StatelessPlugin.class)
            .findFirst()
            .orElseThrow()
            .getStatelessSharedBlobCacheService()
            .forceEvict(key -> key.shardId().getIndex().equals(f.index));
    }

    void triggerRoutingChange() throws Exception {
        final String other = "routing-poke-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createIndex(other, indexSettings(1, 0).build());
        ensureGreen(other);
        assertAcked(indicesAdmin().prepareDelete(other));
        final long version = clusterState().version();
        assertBusy(() -> {
            for (ClusterService cs : internalCluster().getInstances(ClusterService.class)) {
                assertTrue(cs.state().version() >= version);
            }
        });
    }

    /** Per node: files/bytes under indices/&lt;uuid&gt;, IndexService presence, shared-cache regions of the index. */
    String residue(Fixture f) throws IOException {
        final StringBuilder sb = new StringBuilder();
        for (String node : f.dataNodes()) {
            long files = 0;
            long bytes = 0;
            final List<String> listing = new ArrayList<>();
            for (Path dataPath : internalCluster().getInstance(NodeEnvironment.class, node).nodeDataPaths()) {
                final Path dir = dataPath.resolve("indices").resolve(f.index.getUUID());
                if (Files.exists(dir) == false) {
                    continue;
                }
                try (Stream<Path> s = Files.walk(dir)) {
                    for (Path p : s.toList()) {
                        if (Files.isRegularFile(p)) {
                            files++;
                            bytes += Files.size(p);
                            listing.add(dir.relativize(p).toString());
                        }
                    }
                }
            }
            final long regions = internalCluster().getInstance(PluginsService.class, node)
                .filterPlugins(StatelessPlugin.class)
                .findFirst()
                .orElseThrow()
                .getStatelessSharedBlobCacheService()
                .countCachedRegions(k -> k.shardId().getIndex().equals(f.index));
            sb.append(
                String.format(
                    Locale.ROOT,
                    "%n  node=%s indexService=%s files=%d bytes=%d cacheRegions=%d listing=%s",
                    node,
                    internalCluster().getInstance(IndicesService.class, node).indexService(f.index) != null,
                    files,
                    bytes,
                    regions,
                    listing
                )
            );
        }
        return sb.toString();
    }

    static void log(String format, Object... args) {
        LOG.info("PARK| " + String.format(Locale.ROOT, format, args));
    }

    // ---- C1: park and unpark by settings update ------------------------------------------------------------------------------------

    /**
     * The decider keeps a parked primary unassigned through reroutes and unrelated routing changes; no data node holds the index; search
     * fails (no copy); clearing the setting brings the primary back with term + 1 and all documents.
     */
    public void testParkKeepsPrimaryUnassigned() throws Exception {
        final Fixture f = setUpIndex();
        final long term = primaryTerm(f);
        park(f);
        assertPrimaryParked(f);
        // failing an active primary bumps the term (IndexMetadataUpdater#shardFailed); nothing is ever assigned at term + 1
        assertThat(primaryTerm(f), equalTo(term + 1));
        log("parked residue:%s", residue(f));
        expectThrows(SearchPhaseExecutionException.class, () -> prepareSearch(f.indexName).get().decRef());

        // still parked after more reroutes
        ClusterRerouteUtils.reroute(client());
        triggerRoutingChange();
        assertPrimaryParked(f);

        unpark(f);
        assertBusy(() -> assertTrue(primary(f).started()));
        assertThat(primaryTerm(f), equalTo(term + 2));
        setReplicaCount(1, f.indexName);
        ensureGreen(f.indexName);
        assertHitCount(prepareSearch(f.indexName).setTrackTotalHits(true), f.docs);
    }

    // ---- C2: search activation while the indexing shard is parked -----------------------------------------------------------------

    /** Cluster settings of the parked search path: step 2b pointer activation + unpromotable copies without a primary. */
    static void enableParkedSearch() {
        updateClusterSettings(
            Settings.builder()
                .put(FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING.getKey(), true)
                .put(FastSearchActivationSettings.FAST_ACTIVATION_DISCOVERY_SETTING.getKey(), "pointer")
                .put(ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING.getKey(), true)
        );
    }

    static ShardRouting searchCopy(Fixture f) {
        final var copies = clusterState().routingTable().shardRoutingTable(f.shardId()).assignedUnpromotableShards();
        return copies.isEmpty() ? null : copies.get(0);
    }

    /** Sorted ids of all documents, searched on the search node (the only one holding a searchable copy). */
    String allIds(Fixture f) {
        final List<String> ids = new ArrayList<>();
        final var resp = client(f.searchNode).prepareSearch(f.indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setTrackTotalHits(true)
            .setSize(10_000)
            .get();
        try {
            assertNoFailures(resp);
            for (SearchHit hit : resp.getHits().getHits()) {
                ids.add(hit.getId());
            }
            Collections.sort(ids);
            return resp.getHits().getTotalHits().value() + ":" + ids;
        } finally {
            resp.decRef();
        }
    }

    static long count(String node, String op, String blobPrefix) {
        return ActivationLatencyIT.REQUESTS.stream()
            .filter(r -> r.node.equals(node) && (op == null || r.op.equals(op)) && (blobPrefix == null || r.blob.startsWith(blobPrefix)))
            .count();
    }

    static long bytes(String node) {
        return ActivationLatencyIT.REQUESTS.stream().filter(r -> r.node.equals(node)).mapToLong(r -> r.bytes.get()).sum();
    }

    /**
     * With the indexing shard parked (unassigned) the search copy is allocated, opens from the pointer of the parked term (1 GET, 0
     * LIST) and returns the same documents as before the park; the primary stays unassigned and no index node holds the index.
     */
    public void testSearchActivatesFromPointerWhilePrimaryParked() throws Exception {
        final Fixture f = setUpIndex();
        setReplicaCount(1, f.indexName);
        ensureGreen(f.indexName);
        final String expected = allIds(f);
        park(f);
        enableParkedSearch();

        ActivationLatencyIT.REQUESTS.clear();
        ActivationLatencyIT.recording = true;
        setReplicaCount(1, f.indexName);
        assertBusy(() -> assertThat(String.valueOf(searchCopy(f)), searchCopy(f) != null && searchCopy(f).started(), equalTo(true)));
        ActivationLatencyIT.recording = false;
        assertThat(allIds(f), equalTo(expected));
        assertPrimaryParked(f);
        assertThat("no object store discovery", count(f.searchNode, "LIST", null), equalTo(0L));
        assertThat("one pointer GET", count(f.searchNode, "GET", CommitPointer.BLOB_NAME_PREFIX), equalTo(1L));
        for (String indexNode : f.indexNodes) {
            assertThat(internalCluster().getInstance(IndicesService.class, indexNode).indexService(f.index), nullValue());
        }
        // leave the index green: test cleanup waits ~60 s on a RED index
        unpark(f);
        ensureGreen(f.indexName);
    }

    // ---- C3: deferred registration, live search copy across unpark ----------------------------------------------------------------

    /**
     * A search copy activated while parked registers with the indexing shard only once it is active again (once), keeps running across
     * the primary term bump of the unpark, and sees documents written after the unpark.
     */
    public void testUnparkDeliversNewCommitsToLiveSearchCopy() throws Exception {
        final Fixture f = setUpIndex();
        park(f);
        enableParkedSearch();
        final AtomicInteger registrations = new AtomicInteger();
        for (String indexNode : f.indexNodes) {
            MockTransportService.getInstance(indexNode)
                .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                    registrations.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                });
        }
        setReplicaCount(1, f.indexName);
        assertBusy(() -> assertThat(searchCopy(f) != null && searchCopy(f).started(), equalTo(true)));
        final String allocationId = searchCopy(f).allocationId().getId();
        assertHitCount(client(f.searchNode).prepareSearch(f.indexName).setTrackTotalHits(true), f.docs);
        assertThat("no registration while parked", registrations.get(), equalTo(0));

        final long term = primaryTerm(f);
        unpark(f);
        assertBusy(() -> assertTrue(primary(f).started()));
        assertThat(primaryTerm(f), equalTo(term + 1));
        assertBusy(() -> assertThat("deferred registration sent once the primary is active", registrations.get(), equalTo(1)));

        indexDocs(f.indexName, 7);
        refresh(f.indexName);
        assertBusy(() -> assertHitCount(client(f.searchNode).prepareSearch(f.indexName).setTrackTotalHits(true), f.docs + 7));
        assertThat("search copy was not restarted", searchCopy(f).allocationId().getId(), equalTo(allocationId));
        assertThat(registrations.get(), equalTo(1));
    }

    // ---- C4: unpark on write ------------------------------------------------------------------------------------------------------

    /**
     * With the write filter on, a plain bulk to a parked index (no test-side settings update) is acked: the filter clears the parked
     * setting, the primary recovers with term + 1, and the live search copy (activated while parked) sees the document.
     */
    public void testWriteUnparksIndexingShard() throws Exception {
        final Fixture f = setUpIndex();
        park(f);
        enableParkedSearch();
        updateClusterSettings(Settings.builder().put(CheapShardUnparkOnWriteFilter.ENABLED_SETTING.getKey(), true));
        final boolean withSearchCopy = randomBoolean();
        if (withSearchCopy) {
            setReplicaCount(1, f.indexName);
            assertBusy(() -> assertThat(searchCopy(f) != null && searchCopy(f).started(), equalTo(true)));
        }
        final long term = primaryTerm(f);
        final long t0 = System.nanoTime();
        final var bulk = client(randomFrom(f.dataNodes())).prepareBulk()
            .add(client().prepareIndex(f.indexName).setId("written-while-parked").setSource("field", "v"))
            .get(TimeValue.timeValueSeconds(30));
        log("write-unpark via filter (search copy %s): ack in %d ms", withSearchCopy, ms(System.nanoTime() - t0));
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        // the coordinating node acted on its applied state; the master's may lag behind by the STARTED publication
        assertBusy(() -> assertTrue(primary(f).started()));
        assertThat(primaryTerm(f), equalTo(term + 1));
        assertFalse(
            CheapShardParkedAllocationDecider.PARKED_SETTING.get(clusterState().metadata().getProject().index(f.indexName).getSettings())
        );
        if (withSearchCopy == false) {
            setReplicaCount(1, f.indexName);
            ensureGreen(f.indexName);
        }
        refresh(f.indexName);
        assertBusy(() -> assertHitCount(client(f.searchNode).prepareSearch(f.indexName).setTrackTotalHits(true), f.docs + 1));
    }

    /**
     * An {@code _update} (here an upsert) to a parked index unparks it too: unlike index/delete it is not a bulk until it has reached the
     * primary, so the filter must act on the update action itself.
     */
    public void testUpdateUnparksIndexingShard() throws Exception {
        final Fixture f = setUpIndex();
        park(f);
        updateClusterSettings(Settings.builder().put(CheapShardUnparkOnWriteFilter.ENABLED_SETTING.getKey(), true));
        final long term = primaryTerm(f);
        final var update = client(randomFrom(f.dataNodes())).prepareUpdate(f.indexName, "updated-while-parked")
            .setDoc("field", "v")
            .setDocAsUpsert(true)
            .get(TimeValue.timeValueSeconds(30));
        assertThat(update.getId(), equalTo("updated-while-parked"));
        assertBusy(() -> assertTrue(primary(f).started()));
        assertThat(primaryTerm(f), equalTo(term + 1));
        assertFalse(
            CheapShardParkedAllocationDecider.PARKED_SETTING.get(clusterState().metadata().getProject().index(f.indexName).getSettings())
        );
        setReplicaCount(1, f.indexName);
        ensureGreen(f.indexName);
        refresh(f.indexName);
        assertHitCount(prepareSearch(f.indexName).setTrackTotalHits(true), f.docs + 1);
    }

    // ---- C5: measurements --------------------------------------------------------------------------------------------------------

    /**
     * From parked (no primary, 0 search copies, no IndexService/IndexShard on any node; the shard directory remains on the former primary's
     * node and the search node's cache regions are force-evicted, see the class javadoc): search activation end-to-end and
     * node-local, then a write that unparks the indexing shard (bulk sent first, waits in ReroutePhase, then parked=false), at
     * L = 30 and 100 ms object store first-byte latency (no jitter, unlimited bandwidth). Prints {@code PARK|} lines.
     */
    public void testMeasureParkedActivationAndWriteUnpark() throws Exception {
        final Fixture f = setUpIndex();
        enableParkedSearch();
        final int reps = Integer.getInteger("tests.park.reps", 3);
        long docs = f.docs;
        for (long latency : new long[] { 30, 100 }) {
            for (int rep = 0; rep < reps; rep++) {
                setLatency(0);
                park(f);
                assertPrimaryParked(f);
                if (rep == 0) {
                    log("L=%d parked residue (term=%d):%s", latency, primaryTerm(f), residue(f));
                }
                setLatency(latency);

                // -- search activation: replicas 0 -> 1 until the first full hit
                final AtomicLong initializingSeenAt = new AtomicLong();
                final ClusterStateListener seen = event -> {
                    final var copy = event.state().routingTable().shardRoutingTable(f.shardId()).assignedUnpromotableShards();
                    if (copy.isEmpty() == false && initializingSeenAt.get() == 0L) {
                        initializingSeenAt.compareAndSet(0L, System.nanoTime());
                    }
                };
                internalCluster().clusterService(f.searchNode).addListener(seen);
                ActivationLatencyIT.REQUESTS.clear();
                ActivationLatencyIT.recording = true;
                final long t0 = System.nanoTime();
                setReplicaCount(1, f.indexName);
                final long tAck = System.nanoTime();
                long tHit;
                while (true) {
                    try {
                        final var resp = client(f.searchNode).prepareSearch(f.indexName).setTrackTotalHits(true).setSize(0).get();
                        try {
                            if (resp.getFailedShards() == 0 && resp.getHits().getTotalHits().value() == docs) {
                                tHit = System.nanoTime();
                                break;
                            }
                        } finally {
                            resp.decRef();
                        }
                    } catch (Exception e) {
                        // not started yet
                    }
                    if (System.nanoTime() - t0 > TimeUnit.SECONDS.toNanos(30)) {
                        throw new AssertionError("search copy did not activate");
                    }
                }
                ActivationLatencyIT.recording = false;
                internalCluster().clusterService(f.searchNode).removeListener(seen);
                log(
                    "L=%d rep=%d search-activation e2e=%d ms (settings ack %d ms) node-local=%d ms | search node: GET=%d pointerGET=%d LIST=%d"
                        + " bytes=%d | index nodes requests=%d | primary unassigned=%s",
                    latency,
                    rep,
                    ms(tHit - t0),
                    ms(tAck - t0),
                    ms(tHit - initializingSeenAt.get()),
                    count(f.searchNode, "GET", null),
                    count(f.searchNode, "GET", CommitPointer.BLOB_NAME_PREFIX),
                    count(f.searchNode, "LIST", null),
                    bytes(f.searchNode),
                    f.indexNodes.stream().mapToLong(n -> count(n, null, null)).sum(),
                    primary(f).unassigned()
                );

                // -- write unpark: the bulk waits for the primary, the unpark is the settings update (what a write filter would do)
                ActivationLatencyIT.REQUESTS.clear();
                ActivationLatencyIT.recording = true;
                final String id = "unpark-" + latency + "-" + rep;
                final long w0 = System.nanoTime();
                final var bulk = client().prepareBulk().add(client().prepareIndex(f.indexName).setId(id).setSource("field", "v")).execute();
                unpark(f);
                final long wSettings = System.nanoTime();
                final var bulkResponse = bulk.actionGet(TimeValue.timeValueSeconds(60));
                final long wAck = System.nanoTime();
                ActivationLatencyIT.recording = false;
                assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                docs++;
                final String primaryNode = clusterState().nodes().get(primary(f).currentNodeId()).getName();
                // -- freshness on the live search copy
                final long r0 = System.nanoTime();
                refresh(f.indexName);
                final long finalDocs = docs;
                assertBusy(() -> assertHitCount(client(f.searchNode).prepareSearch(f.indexName).setTrackTotalHits(true), finalDocs));
                final long r1 = System.nanoTime();
                log(
                    "L=%d rep=%d write-unpark ack=%d ms (settings update %d ms) | primary node %s: GET=%d LIST=%d PUT=%d bytes=%d"
                        + " | term=%d | refresh+visible on search copy=%d ms",
                    latency,
                    rep,
                    ms(wAck - w0),
                    ms(wSettings - w0),
                    primaryNode,
                    count(primaryNode, "GET", null),
                    count(primaryNode, "LIST", null),
                    count(primaryNode, "PUT", null),
                    bytes(primaryNode),
                    primaryTerm(f),
                    ms(r1 - r0)
                );
            }
        }
        setLatency(0);
    }

    static long ms(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }
}
