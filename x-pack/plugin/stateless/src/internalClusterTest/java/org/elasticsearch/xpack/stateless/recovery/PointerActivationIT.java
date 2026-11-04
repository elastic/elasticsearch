/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.CommitPointer;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Step 2b: a search shard activates from the {@link CommitPointer} (one object store GET, no LIST, no header reads) while the indexing
 * shard cannot serve it (registration never answered, answered with no commit, or failing; reads from the indexing node rejected), returns
 * the same hits as the default recovery, and falls back to object store discovery when the pointer is missing, corrupt or stale.
 */
public class PointerActivationIT extends AbstractStatelessPluginIntegTestCase {

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

    /**
     * The tests assert the object store reads of an activation, and the open bytes of the pointer are written in the shared cache. The
     * random cache of the test base (down to a single region, or no cache at all) evicts them as they are written, so the reader
     * refetches them from the object store and the read counts depend on the random cache size, not on the pointer. Only the nodes with
     * a cache go through here: the setting is not permitted on the master only node.
     */
    @Override
    protected Settings.Builder settingsForRoles(DiscoveryNodeRole... roles) {
        return super.settingsForRoles(roles).put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(128))
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(8));
    }

    /** Counts the LISTs and the reads per blob name of a node. */
    private static class CountingStrategy extends StatelessMockRepositoryStrategy {
        final AtomicInteger lists = new AtomicInteger();
        final Queue<String> reads = new ConcurrentLinkedQueue<>();

        @Override
        public Map<String, BlobContainer> blobContainerChildren(
            CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            lists.incrementAndGet();
            return super.blobContainerChildren(originalSupplier, purpose);
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobs(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            lists.incrementAndGet();
            return super.blobContainerListBlobs(originalSupplier, purpose);
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobsByPrefix(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobNamePrefix
        ) throws IOException {
            lists.incrementAndGet();
            return super.blobContainerListBlobsByPrefix(originalSupplier, purpose, blobNamePrefix);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            reads.add(blobName);
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            reads.add(blobName);
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
        }

        long pointerReads() {
            return reads.stream().filter(n -> n.startsWith(CommitPointer.BLOB_NAME_PREFIX)).count();
        }

        void reset() {
            lists.set(0);
            reads.clear();
        }
    }

    private record Fixture(String indexNode, String searchNode, CountingStrategy searchStore, String indexName, Index index) {}

    /** An index with several flushed (uploaded) commits carrying soft deletes, and no search copy. */
    private Fixture setUpIndex() throws Exception {
        return setUpIndex(Settings.EMPTY);
    }

    private Fixture setUpIndex(Settings indexNodeSettings) throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode(indexNodeSettings);
        final CountingStrategy searchStore = new CountingStrategy();
        final String searchNode = startSearchNode(searchStore);
        ensureStableCluster(3);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        final int rounds = randomIntBetween(2, 5);
        for (int r = 0; r < rounds; r++) {
            indexDocs(indexName, randomIntBetween(50, 300), () -> Integer.toString(randomIntBetween(0, 600)));
            flush(indexName);
        }
        final Fixture f = new Fixture(indexNode, searchNode, searchStore, indexName, resolveIndex(indexName));
        awaitPointerOfLatestUpload(f);
        return f;
    }

    private ShardId shardId(Fixture f) {
        return new ShardId(f.index, 0);
    }

    private long primaryTerm(Fixture f) {
        return client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(f.indexName)
            .primaryTerm(0);
    }

    private BlobContainer shardContainer(Fixture f) {
        return getObjectStoreService(f.indexNode).getProjectBlobContainer(shardId(f));
    }

    private void awaitPointerOfLatestUpload(Fixture f) throws Exception {
        final PrimaryTermAndGeneration latest = internalCluster().getInstance(StatelessCommitService.class, f.indexNode)
            .getLatestUploadedBcc(shardId(f))
            .primaryTermAndGeneration();
        assertBusy(() -> {
            final CommitPointer pointer = CommitPointer.read(shardContainer(f), primaryTerm(f));
            assertThat(pointer, notNullValue());
            assertThat(pointer.batchedCompoundCommitTermAndGeneration(), equalTo(latest));
            assertThat(pointer.validate(shardId(f), primaryTerm(f)), nullValue());
        });
    }

    private static void setActivation(boolean pointer) {
        updateClusterSettings(
            Settings.builder()
                .put(FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING.getKey(), pointer)
                .put(FastSearchActivationSettings.FAST_ACTIVATION_DISCOVERY_SETTING.getKey(), pointer ? "pointer" : "registration")
                .put(FastSearchActivationSettings.FAST_ACTIVATION_RANGE_SIZE_SETTING.getKey(), randomFrom("64kb", "1mb", "16mb"))
                .put(FastSearchActivationSettings.FAST_ACTIVATION_HEADER_RANGE_SIZE_SETTING.getKey(), randomFrom("4kb", "64kb", "1mb"))
                .put(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey(), randomFrom("0b", "16kb", "1gb"))
        );
    }

    private String activateAndQuery(Fixture f) throws Exception {
        f.searchStore.reset();
        setReplicaCount(1, f.indexName);
        ensureGreen(f.indexName);
        return queries(f);
    }

    private void deactivate(Fixture f) throws Exception {
        setReplicaCount(0, f.indexName);
        assertBusy(() -> assertThat(internalCluster().getInstance(IndicesService.class, f.searchNode).indexService(f.index), nullValue()));
        internalCluster().getInstance(PluginsService.class, f.searchNode)
            .filterPlugins(StatelessPlugin.class)
            .findFirst()
            .orElseThrow()
            .getStatelessSharedBlobCacheService()
            .forceEvict(key -> key.shardId().getIndex().equals(f.index));
    }

    private static String queries(Fixture f) {
        StringBuilder sb = new StringBuilder();
        for (var query : new org.elasticsearch.index.query.QueryBuilder[] {
            QueryBuilders.matchAllQuery(),
            QueryBuilders.rangeQuery("field.keyword").gte("m"),
            QueryBuilders.existsQuery("field") }) {
            SearchResponse resp = client(f.searchNode).prepareSearch(f.indexName)
                .setQuery(query)
                .setTrackTotalHits(true)
                .setSize(10_000)
                .addSort("field.keyword", SortOrder.ASC)
                .get();
            try {
                assertNoFailures(resp);
                sb.append(resp.getHits().getTotalHits().value()).append(':');
                List<String> hits = new ArrayList<>();
                for (SearchHit hit : resp.getHits().getHits()) {
                    hits.add(hit.getId() + '=' + hit.getSourceAsMap().get("field"));
                }
                Collections.sort(hits);
                sb.append(hits).append('|');
            } finally {
                resp.decRef();
            }
        }
        return sb.toString();
    }

    /**
     * The indexing shard cannot serve the activation: registrations are never answered (as if its node were unreachable), answered
     * with no commit (an indexing shard without engine / {@code NoOpEngine}) or fail, and reads from the indexing node are rejected.
     * The search shard still starts, from 1 pointer GET and 0 LIST, with the same hits as the default recovery.
     */
    public void testActivatesFromPointerWithoutIndexingShard() throws Exception {
        final Fixture f = setUpIndex();
        setActivation(false);
        final String expected = activateAndQuery(f);
        assertThat(f.searchStore.lists.get(), greaterThan(0));
        deactivate(f);

        setActivation(true);
        final var behaviour = randomFrom("hang", "empty", "fail");
        final AtomicInteger registrations = new AtomicInteger();
        final AtomicInteger chunkReads = new AtomicInteger();
        final Queue<TransportChannel> held = new ConcurrentLinkedQueue<>();
        final var indexTransport = MockTransportService.getInstance(f.indexNode);
        indexTransport.addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
            registrations.incrementAndGet();
            switch (behaviour) {
                case "hang" -> held.add(channel);
                case "empty" -> channel.sendResponse(RegisterCommitResponse.EMPTY);
                case "fail" -> channel.sendResponse(new ElasticsearchException("simulated: no indexing engine"));
                default -> throw new AssertionError(behaviour);
            }
        });
        indexTransport.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                chunkReads.incrementAndGet();
                channel.sendResponse(new ElasticsearchException("simulated: no indexing engine"));
            }
        );
        try {
            final String actual = activateAndQuery(f);
            assertThat(behaviour, actual, equalTo(expected));
            assertThat("no object store discovery", f.searchStore.lists.get(), equalTo(0));
            assertThat("one pointer GET", f.searchStore.pointerReads(), equalTo(1L));
            assertThat("no read from the indexing node", chunkReads.get(), equalTo(0));
            // registration is attempted in the background, and the shard started without its answer
            assertBusy(() -> assertThat(registrations.get(), greaterThanOrEqualTo(1)));
        } finally {
            indexTransport.clearAllRules();
            for (TransportChannel channel : held) {
                channel.sendResponse(new ElasticsearchException("released"));
            }
            setActivation(false);
        }
    }

    /**
     * Phase 4: the pointer carries the bytes that opening the reader reads ({@code commit_pointer.open_bytes_max_size}). With no indexing
     * engine and no whole-blob warming, the search shard starts after exactly one object store read (the pointer) and returns the same
     * hits as the default recovery; ignoring the open bytes ({@code use_pointer_open_bytes: false}) warms headers from the store instead.
     */
    public void testActivatesFromPointerOpenBytesWithoutOtherReads() throws Exception {
        final Fixture f = setUpIndex(
            Settings.builder().put(FastSearchActivationSettings.COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING.getKey(), "4mb").build()
        );
        final CommitPointer pointer = CommitPointer.read(shardContainer(f), primaryTerm(f));
        assertThat(pointer.openRanges().isEmpty(), equalTo(false));
        setActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);

        final Queue<TransportChannel> held = new ConcurrentLinkedQueue<>();
        final var indexTransport = MockTransportService.getInstance(f.indexNode);
        indexTransport.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> held.add(channel)
        );
        try {
            for (boolean useOpenBytes : new boolean[] { true, false }) {
                setActivation(true);
                updateClusterSettings(
                    Settings.builder()
                        .put(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey(), "0b")
                        .put(FastSearchActivationSettings.FAST_ACTIVATION_USE_POINTER_OPEN_BYTES_SETTING.getKey(), useOpenBytes)
                );
                f.searchStore.reset();
                setReplicaCount(1, f.indexName);
                ensureGreen(f.indexName);
                final List<String> reads = List.copyOf(f.searchStore.reads);
                assertThat("no object store discovery", f.searchStore.lists.get(), equalTo(0));
                if (useOpenBytes) {
                    assertThat("only the pointer is read to open the reader: " + reads, reads.size(), equalTo(1));
                    assertThat(f.searchStore.pointerReads(), equalTo(1L));
                } else {
                    assertThat("headers are warmed from the store: " + reads, reads.size(), greaterThan(1));
                }
                assertThat(queries(f), equalTo(expected));
                deactivate(f);
            }
        } finally {
            indexTransport.clearAllRules();
            for (TransportChannel channel : held) {
                channel.sendResponse(new ElasticsearchException("released"));
            }
            updateClusterSettings(
                Settings.builder()
                    .putNull(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey())
                    .putNull(FastSearchActivationSettings.FAST_ACTIVATION_USE_POINTER_OPEN_BYTES_SETTING.getKey())
            );
            setActivation(false);
        }
    }

    /**
     * Pointer missing, corrupt, of another term than the cluster state's, or naming another shard: the search shard falls back to
     * object store discovery and returns the same hits.
     */
    public void testFallsBackOnMissingCorruptOrStalePointer() throws Exception {
        final Fixture f = setUpIndex();
        setActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);
        // stop publishing so that the broken pointer is not overwritten
        updateClusterSettings(Settings.builder().put(FastSearchActivationSettings.COMMIT_POINTER_ENABLED_SETTING.getKey(), false));

        final long term = primaryTerm(f);
        final BlobContainer container = shardContainer(f);
        final CommitPointer good = CommitPointer.read(container, term);
        assertThat(good, notNullValue());
        final var broken = randomFrom("missing", "corrupt", "truncated", "newer-term", "other-shard");
        switch (broken) {
            case "missing" -> container.deleteBlobsIgnoringIfNotExists(
                OperationPurpose.INDICES,
                List.of(CommitPointer.blobName(term)).iterator()
            );
            case "corrupt" -> {
                byte[] bytes = good.toBytes().toBytesRef().bytes.clone();
                bytes[randomIntBetween(0, bytes.length - 1)] ^= 0x5A;
                container.writeBlobAtomic(OperationPurpose.INDICES, CommitPointer.blobName(term), new BytesArray(bytes), false);
            }
            case "truncated" -> container.writeBlobAtomic(
                OperationPurpose.INDICES,
                CommitPointer.blobName(term),
                good.toBytes().slice(0, randomIntBetween(0, good.toBytes().length() - 1)),
                false
            );
            // written by (or claiming to be) a primary of a newer term than the one the search shard knows
            case "newer-term" -> container.writeBlobAtomic(
                OperationPurpose.INDICES,
                CommitPointer.blobName(term),
                new CommitPointer(
                    term + 1,
                    good.writerNodeId(),
                    good.batchedCompoundCommitTermAndGeneration(),
                    good.compoundCommit(),
                    good.replicatedFileRanges()
                ).toBytes(),
                false
            );
            case "other-shard" -> container.writeBlobAtomic(
                OperationPurpose.INDICES,
                CommitPointer.blobName(term),
                new CommitPointer(
                    term,
                    good.writerNodeId(),
                    good.batchedCompoundCommitTermAndGeneration(),
                    new org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit(
                        new ShardId(new Index("other", "other-uuid"), 0),
                        good.compoundCommit().primaryTermAndGeneration(),
                        good.compoundCommit().translogRecoveryStartFile(),
                        good.compoundCommit().nodeEphemeralId(),
                        good.compoundCommit().commitFiles(),
                        good.compoundCommit().sizeInBytes(),
                        good.compoundCommit().internalFiles(),
                        good.compoundCommit().headerSizeInBytes(),
                        good.compoundCommit().internalFilesReplicatedRanges(),
                        good.compoundCommit().extraContent(),
                        good.compoundCommit().hollow(),
                        good.compoundCommit().timestampFieldValueRange()
                    ),
                    good.replicatedFileRanges()
                ).toBytes(),
                false
            );
            default -> throw new AssertionError(broken);
        }

        setActivation(true);
        try {
            assertThat(broken, activateAndQuery(f), equalTo(expected));
            assertThat(broken + ": pointer read once", f.searchStore.pointerReads(), equalTo(1L));
            assertThat(broken + ": fallback discovers the commit in the object store", f.searchStore.lists.get(), greaterThan(0));
        } finally {
            setActivation(false);
        }
    }

    /**
     * The pointer names the latest uploaded commit; a newer refreshed (not uploaded) commit of a resident indexing shard reaches the
     * pointer-activated shard through the background registration.
     */
    public void testBackgroundRegistrationDeliversNewerCommit() throws Exception {
        final Fixture f = setUpIndex();
        indexDocs(f.indexName, randomIntBetween(1, 50), () -> "unflushed-" + randomUUID());
        refresh(f.indexName);
        setActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);

        setActivation(true);
        try {
            f.searchStore.reset();
            setReplicaCount(1, f.indexName);
            ensureGreen(f.indexName);
            assertThat(f.searchStore.lists.get(), equalTo(0));
            assertBusy(() -> assertThat(queries(f), equalTo(expected)));
        } finally {
            setActivation(false);
        }
    }

    /** Distinct blobs referenced by the pointed commit, as (term, blob name). */
    private static List<BlobFile> blobsOf(CommitPointer pointer) {
        return pointer.compoundCommit().commitFiles().values().stream().map(BlobLocation::blobFile).distinct().toList();
    }

    private boolean blobExists(Fixture f, BlobFile blob) throws IOException {
        return getObjectStoreService(f.indexNode).getProjectBlobContainer(shardId(f), blob.primaryTerm())
            .blobExists(OperationPurpose.INDICES, blob.blobName());
    }

    /**
     * Retention race (phase 3 gap 2), as it happens: the search shard reads pointer P1, then (before it registers) a newer commit that
     * references none of P1's blobs is uploaded and the indexing shard releases P1's commit. Stopping the publisher keeps P1 in place,
     * which stands for a search shard that read it just before. Returns P1.
     */
    private CommitPointer releasePointedCommit(Fixture f) throws Exception {
        updateClusterSettings(Settings.builder().put(FastSearchActivationSettings.COMMIT_POINTER_ENABLED_SETTING.getKey(), false));
        final CommitPointer stale = CommitPointer.read(shardContainer(f), primaryTerm(f));
        assertThat(stale, notNullValue());
        indexDocs(f.indexName, 1, () -> "force-merged-" + randomUUID());
        assertNoFailures(client().admin().indices().prepareForceMerge(f.indexName).setMaxNumSegments(1).setFlush(true).get());
        indexDocs(f.indexName, 1, () -> "after-merge-" + randomUUID());
        flush(f.indexName);
        return stale;
    }

    /**
     * Phase 3 gap 2, without a deletion delay: the blobs of the pointed commit are deleted before the search shard opens it. Warming
     * fails on the missing blob, and the shard falls back to discovery and serves the latest commit.
     */
    public void testFallsBackWhenPointedCommitWasDeleted() throws Exception {
        final Fixture f = setUpIndex();
        final CommitPointer stale = releasePointedCommit(f);
        setActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);
        // warming touches every blob of the commit, so one deleted blob is enough to make it fail
        assertBusy(() -> {
            boolean anyDeleted = false;
            for (BlobFile blob : blobsOf(stale)) {
                anyDeleted |= blobExists(f, blob) == false;
            }
            assertTrue("a blob of the released commit " + blobsOf(stale) + " was deleted", anyDeleted);
        });

        setActivation(true);
        try {
            assertThat(activateAndQuery(f), equalTo(expected));
            assertThat("pointer read once", f.searchStore.pointerReads(), equalTo(1L));
            assertThat("fallback discovers the commit in the object store", f.searchStore.lists.get(), greaterThan(0));
        } finally {
            setActivation(false);
        }
    }

    /**
     * Phase 3 gap 2, with the existing {@link ObjectStoreService#OBJECT_STORE_FILE_DELETION_DELAY} on the index node: the released
     * commit's blobs are kept for the delay, so the search shard opens the pointed commit without falling back (0 LIST), and the
     * background registration then moves it to the latest commit.
     */
    public void testDeletionDelayKeepsPointedCommitReadable() throws Exception {
        final Fixture f = setUpIndex(Settings.builder().put(ObjectStoreService.OBJECT_STORE_FILE_DELETION_DELAY.getKey(), "1h").build());
        final CommitPointer stale = releasePointedCommit(f);
        // the indexing shard released the pointed commit and would have deleted its blobs by now without the delay
        final var objectStoreService = getObjectStoreService(f.indexNode);
        final var staleBcc = stale.batchedCompoundCommitTermAndGeneration();
        assertBusy(
            () -> assertTrue(
                objectStoreService.getCommitBlobsDeletionsInProgress(shardId(f))
                    .stream()
                    .anyMatch(c -> c.primaryTermAndGeneration().equals(staleBcc))
            )
        );
        setActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);
        for (BlobFile blob : blobsOf(stale)) {
            assertTrue("released blob " + blob + " kept", blobExists(f, blob));
        }

        setActivation(true);
        try {
            f.searchStore.reset();
            setReplicaCount(1, f.indexName);
            ensureGreen(f.indexName);
            assertThat("opened from the pointer, no fallback", f.searchStore.lists.get(), equalTo(0));
            assertThat(f.searchStore.pointerReads(), equalTo(1L));
            assertBusy(() -> assertThat(queries(f), equalTo(expected)));
        } finally {
            setActivation(false);
        }
    }

    /**
     * Phase 3 gap 3: a blob of the commit a pointer-activated shard serves goes missing after activation (deleted under it, see gap 2).
     * The query that hits it fails, and the shard is failed (instead of failing every query on that range until it moves) and recovers;
     * once the blob is back (here: restored; in reality: recovery picks the then-current pointer or discovers the latest commit) it
     * serves the same hits again.
     */
    public void testMissingBlobAfterPointerActivationFailsShard() throws Exception {
        final Fixture f = setUpIndex();
        setActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);

        setActivation(true);
        try {
            assertThat(activateAndQuery(f), equalTo(expected));
            assertThat(f.searchStore.lists.get(), equalTo(0));
            final String allocationId = findSearchShard(f.indexName).routingEntry().allocationId().getId();

            final CommitPointer pointer = CommitPointer.read(shardContainer(f), primaryTerm(f));
            final BlobFile blob = randomFrom(blobsOf(pointer));
            final BlobContainer termContainer = getObjectStoreService(f.indexNode).getProjectBlobContainer(shardId(f), blob.primaryTerm());
            final byte[] bytes;
            try (InputStream in = termContainer.readBlob(OperationPurpose.INDICES, blob.blobName())) {
                bytes = in.readAllBytes();
            }
            termContainer.deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, List.of(blob.blobName()).iterator());
            evictSearchCache(f);

            // every query reads all segments (match_all, all hits with source), so it needs the deleted blob
            Throwable failure = null;
            try {
                queries(f);
            } catch (Throwable t) {
                failure = t;
            }
            assertNotNull("query on a deleted blob fails", failure);
            termContainer.writeBlob(OperationPurpose.INDICES, blob.blobName(), new BytesArray(bytes), false);

            assertBusy(() -> {
                final var routing = findSearchShardRouting(f);
                assertNotNull(routing);
                assertThat("search shard was failed and reallocated", routing.allocationId().getId(), not(equalTo(allocationId)));
            });
            ClusterRerouteUtils.rerouteRetryFailed(client());
            ensureGreen(f.indexName);
            assertBusy(() -> assertThat(queries(f), equalTo(expected)));
        } finally {
            setActivation(false);
        }
    }

    private ShardRouting findSearchShardRouting(Fixture f) {
        return client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(f.indexName)
            .shard(0)
            .assignedUnpromotableShards()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private void evictSearchCache(Fixture f) {
        internalCluster().getInstance(PluginsService.class, f.searchNode)
            .filterPlugins(StatelessPlugin.class)
            .findFirst()
            .orElseThrow()
            .getStatelessSharedBlobCacheService()
            .forceEvict(key -> key.shardId().getIndex().equals(f.index));
    }
}
