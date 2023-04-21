/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicator;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicatorReader;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.PluginsService;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessIT extends AbstractStatelessIntegTestCase {

    public void testCompoundCommitHasNodeEphemeralId() throws Exception {
        startMasterOnlyNode();

        String indexNodeName = startIndexNodes(1).get(0);
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);

        indexDocuments(indexName);
        // Force a flush if necessary to make sure we have at least one compound commit
        flush(indexName);

        assertObjectStoreConsistentWithIndexShards();

        Index index = resolveIndex(indexName);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeName);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, indexNodeName);
        var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        String commitFile = StatelessCompoundCommit.NAME + Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();
        assertThat("" + commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile))
        );
        assertThat(
            "Expected that the compound commit has the ephemeral Id of the indexing node",
            commit.nodeEphemeralId(),
            equalTo(clusterService.localNode().getEphemeralId())
        );
    }

    public void testClusterCanFormWithStatelessEnabled() {
        startMasterOnlyNode();

        final int numIndexNodes = randomIntBetween(1, 5);
        startIndexNodes(numIndexNodes);
        ensureStableCluster(numIndexNodes + 1);

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(Stateless.class).stream())
            .toList();
        assertThat(plugins.size(), greaterThan(0));
    }

    public void testUploadToObjectStore() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        indexDocuments(indexName);
    }

    public void testTranslogIsSyncedToObjectStoreDuringIndexing() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = 1;
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        final Map<Index, Integer> indices = resolveIndices();
        Optional<Map.Entry<Index, Integer>> index = indices.entrySet()
            .stream()
            .filter(e -> indexName.equals(e.getKey().getName()))
            .findFirst();
        assertTrue(index.isPresent());

        Map.Entry<Index, Integer> entry = index.get();
        DiscoveryNode indexNode = findIndexNode(entry.getKey(), 0);
        final ShardId shardId = new ShardId(entry.getKey(), 0);

        indexDocs(indexName, 1);

        // Check that the translog on the object store contains the correct sequence numbers and number of operations
        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode.getName());
        var reader = new TranslogReplicatorReader(indexObjectStoreService.getTranslogBlobContainer(), shardId);
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long totalOps = 0;
        Translog.Operation next = reader.next();
        while (next != null) {
            maxSeqNo = SequenceNumbers.max(maxSeqNo, next.seqNo());
            totalOps++;
            next = reader.next();
        }
        assertThat(maxSeqNo, equalTo(0L));
        assertThat(totalOps, equalTo(1L));
    }

    public void testAllTranslogOperationsAreWrittenToObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        assertReplicatedTranslogConsistentWithShards();
    }

    private static void assertReplicatedTranslogConsistentWithShards() throws Exception {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                DiscoveryNode indexNode = findIndexNode(entry.getKey(), shardId);
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                final ShardId objShardId = new ShardId(entry.getKey(), shardId);

                // Check that the translog on the object store contains the correct sequence numbers and number of operations
                var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode.getName());
                var reader = new TranslogReplicatorReader(indexObjectStoreService.getTranslogBlobContainer(), objShardId);
                long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                long totalOps = 0;
                Translog.Operation next = reader.next();
                while (next != null) {
                    maxSeqNo = SequenceNumbers.max(maxSeqNo, next.seqNo());
                    totalOps++;
                    next = reader.next();
                }
                assertThat(maxSeqNo, equalTo(indexShard.seqNoStats().getMaxSeqNo()));
                assertThat(totalOps, equalTo(indexShard.seqNoStats().getMaxSeqNo() + 1));
            }
        }
    }

    public void testDownloadNewCommitsFromObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 2);
        startIndexNodes(numberOfShards);
        startSearchNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        // Index more documents
        indexDocuments(indexName);
        // Force a flush if necessary since it is possible that indexDocuments(indexName) only refreshed
        flush(indexName);

        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }

        // Index more documents
        indexDocuments(indexName);
        // Force a flush if necessary since it is possible that indexDocuments(indexName) only refreshed
        flush(indexName);

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }
    }

    public void testDownloadNewReplicasFromObjectStore() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        for (int i = 0; i < 3; i++) {
            indexDocuments(indexName);
            flush(indexName);
            assertObjectStoreConsistentWithIndexShards();
        }

        startSearchNodes(numberOfShards);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        assertObjectStoreConsistentWithSearchShards();
    }

    public void testCreatesSearchShardsOfClosedIndex() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        indexDocuments(indexName);
        flush(indexName);
        assertObjectStoreConsistentWithIndexShards();

        assertAcked(client().admin().indices().prepareClose(indexName));

        startSearchNodes(numberOfShards);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        // TODO assertObjectStoreConsistentWithSearchShards(); doesn't work yet because closing the index incremented the primary term
    }

    public void testSetsRecyclableBigArraysInTranslogReplicator() throws Exception {
        startMasterAndIndexNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertBusy(() -> {
            var bigArrays = internalCluster().getInstance(TranslogReplicator.class).bigArrays();
            assertNotNull(bigArrays);
            assertNotNull(bigArrays.breakerService());
        });
    }

    public void testIndicesSegments() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 3);
        startIndexNodes(numberOfShards);
        final int numberOfReplicas = randomIntBetween(0, 3);
        startSearchNodes(numberOfReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(0, 5);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }
        if (iters > 0 || randomBoolean()) {
            refresh(indexName);
        }

        var indicesSegments = client().admin().indices().prepareSegments(indexName).get();
        assertThat(indicesSegments.getSuccessfulShards(), equalTo(numberOfShards + numberOfShards * numberOfReplicas));
        assertThat(indicesSegments.getShardFailures().length, equalTo(0));
        assertThat(indicesSegments.getIndices().size(), equalTo(1));

        var indices = indicesSegments.getIndices().get(indexName);
        assertThat(indices, notNullValue());
        assertThat(indices.getShards().size(), equalTo(numberOfShards));

        var index = resolveIndex(indexName);
        for (int shard = 0; shard < numberOfShards; shard++) {
            var shardSegments = indices.getShards().get(shard);
            assertThat(shardSegments, notNullValue());
            assertThat(shardSegments.shards(), notNullValue());
            assertThat(shardSegments.shards().length, equalTo(1 + numberOfReplicas));
            var shardId = new ShardId(index, shard);
            assertThat(shardSegments.shardId(), equalTo(shardId));

            var indexShard = Arrays.stream(shardSegments.shards())
                .filter(segments -> segments.getShardRouting().isPromotableToPrimary())
                .findAny()
                .orElseThrow(() -> new AssertionError("no index shard found for " + shardId));
            assertThat(indexShard, notNullValue());

            var searchShards = Arrays.stream(shardSegments.shards()).filter(segments -> segments.getShardRouting().isSearchable()).toList();
            assertThat(searchShards.size(), equalTo(numberOfReplicas));

            for (var searchShard : searchShards) {
                assertThat(searchShard.getNumberOfSearch(), equalTo(indexShard.getNumberOfSearch()));
                assertThat(searchShard.getNumberOfCommitted(), equalTo(indexShard.getNumberOfCommitted()));
                assertThat(searchShard.getSegments().size(), equalTo(indexShard.getSegments().size()));

                for (var indexShardSegment : indexShard.getSegments()) {
                    var searchShardSegment = searchShard.getSegments()
                        .stream()
                        .filter(segment -> segment.getName().equals(indexShardSegment.getName()))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("search shard has no corresponding segment " + indexShardSegment.getName()));
                    assertThat(searchShardSegment.getGeneration(), equalTo(indexShardSegment.getGeneration()));
                    assertThat(searchShardSegment.isCommitted(), equalTo(indexShardSegment.isCommitted()));
                    assertThat(searchShardSegment.getNumDocs(), equalTo(indexShardSegment.getNumDocs()));
                    assertThat(searchShardSegment.getDeletedDocs(), equalTo(indexShardSegment.getDeletedDocs()));
                    assertThat(searchShardSegment.getSize(), equalTo(indexShardSegment.getSize()));
                    assertThat(searchShardSegment.getVersion(), equalTo(indexShardSegment.getVersion()));
                    assertThat(searchShardSegment.getMergeId(), equalTo(indexShardSegment.getMergeId()));
                    assertThat(searchShardSegment.getAttributes(), equalTo(indexShardSegment.getAttributes()));
                    assertThat(searchShardSegment.getSegmentSort(), equalTo(indexShardSegment.getSegmentSort()));
                }
            }
        }
    }

    private static void indexDocuments(String indexName) {
        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush(indexName).setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh(indexName).get();
                case 2 -> client().admin().indices().prepareForceMerge(indexName).get();
            }
            assertObjectStoreConsistentWithIndexShards();
        }
    }

    private static void assertObjectStoreConsistentWithIndexShards() {
        assertObjectStoreConsistentWithShards(DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
    }

    private static void assertObjectStoreConsistentWithSearchShards() {
        assertObjectStoreConsistentWithShards(DiscoveryNodeRole.SEARCH_ROLE, ShardRouting.Role.SEARCH_ONLY);
    }

    private static void assertObjectStoreConsistentWithShards(DiscoveryNodeRole nodeRole, ShardRouting.Role shardRole) {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                assertThatObjectStoreIsConsistentWithLastCommit(findShard(entry.getKey(), shardId, nodeRole, shardRole));
            }
        }
    }

    // TODO: Remove This method once we stop uploading the old segments file style
    private static void assertThatObjectStoreIsConsistentWithLastCommitOld(final IndexShard indexShard) {
        final Store store = indexShard.store();
        store.incRef();
        try {
            var blobContainer = internalCluster().getDataNodeInstance(ObjectStoreService.class)
                .getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());

            // can take some time for files to be uploaded to the object store
            assertBusy(() -> {
                var localFiles = segmentInfos.files(false);
                var remoteFiles = blobContainer.listBlobs().keySet();
                assertThat(
                    "Expected that all local files " + localFiles + " exist in remote " + remoteFiles,
                    remoteFiles,
                    hasItems(localFiles.toArray(String[]::new))
                );
                for (String file : segmentInfos.files(false)) {
                    assertThat("" + file, blobContainer.blobExists(file), is(true));
                    try (
                        IndexInput input = store.directory().openInput(file, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        InputStream remote = blobContainer.readBlob(file)
                    ) {
                        assertEquals("File [" + file + "] in object store has a different content than local file ", local, remote);
                    }
                }
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            store.decRef();
        }
    }

    private static void assertThatObjectStoreIsConsistentWithLastCommit(final IndexShard indexShard) {
        // TODO: Remove This call once we stop uploading the old segments file style
        assertThatObjectStoreIsConsistentWithLastCommitOld(indexShard);

        final Store store = indexShard.store();
        store.incRef();
        try {
            ObjectStoreService objectStoreService = internalCluster().getDataNodeInstance(ObjectStoreService.class);
            var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());

            // can take some time for files to be uploaded to the object store
            assertBusy(() -> {
                String commitFile = StatelessCompoundCommit.NAME + segmentInfos.getGeneration();
                assertThat("" + commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
                StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
                    new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile))
                );
                var localFiles = segmentInfos.files(true);
                var expectedBlobFile = localFiles.stream().map(s -> commit.commitFiles().get(s).blobName()).collect(Collectors.toSet());
                var remoteFiles = blobContainerForCommit.listBlobs().keySet();
                assertThat(
                    "Expected that all local files " + localFiles + " exist in remote " + remoteFiles,
                    remoteFiles,
                    hasItems(expectedBlobFile.toArray(String[]::new))
                );
                for (String localFile : segmentInfos.files(true)) {
                    BlobLocation blobLocation = commit.commitFiles().get(localFile);
                    final BlobContainer blobContainerForFile = objectStoreService.getBlobContainer(
                        indexShard.shardId(),
                        blobLocation.primaryTerm()
                    );
                    assertThat("" + localFile, blobContainerForFile.blobExists(blobLocation.blobName()), is(true));
                    try (
                        IndexInput input = store.directory().openInput(localFile, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        InputStream remote = blobContainerForFile.readBlob(
                            blobLocation.blobName(),
                            blobLocation.offset(),
                            blobLocation.length()
                        );
                    ) {
                        assertEquals("File [" + blobLocation + "] in object store has a different content than local file ", local, remote);
                    }
                }
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            store.decRef();
        }
    }

    private static void assertThatSearchShardIsConsistentWithLastCommit(final IndexShard indexShard, final IndexShard searchShard) {
        final Store indexStore = indexShard.store();
        final Store searchStore = searchShard.store();
        indexStore.incRef();
        searchStore.incRef();
        try {
            ObjectStoreService objectStoreService = internalCluster().getDataNodeInstance(ObjectStoreService.class);
            var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(indexStore.directory());

            String commitFile = StatelessCompoundCommit.NAME + segmentInfos.getGeneration();
            assertBusy(() -> assertThat("" + commitFile, blobContainerForCommit.blobExists(commitFile), is(true)));
            StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
                new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile))
            );

            for (String localFile : segmentInfos.files(false)) {
                var blobPath = commit.commitFiles().get(localFile);
                BlobContainer blobContainer = objectStoreService.getBlobContainer(indexShard.shardId(), blobPath.primaryTerm());
                var blobFile = blobPath.blobName();
                // can take some time for files to be uploaded to the object store
                assertBusy(() -> {
                    assertThat("" + blobFile, blobContainer.blobExists(blobFile), is(true));

                    try (
                        IndexInput input = indexStore.directory().openInput(localFile, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        IndexInput searchInput = searchStore.directory().openInput(localFile, IOContext.READONCE);
                        InputStream searchInputStream = new InputStreamIndexInput(searchInput, searchInput.length());
                    ) {
                        assertEquals(
                            "File [" + blobFile + "] on search shard has a different content than local file ",
                            local,
                            searchInputStream
                        );
                    }
                });
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            indexStore.decRef();
            searchStore.decRef();
        }
    }

    private static void assertEquals(String message, InputStream expected, InputStream actual) throws IOException {
        // adapted from Files.mismatch()
        final int BUFFER_SIZE = 8192;
        byte[] buffer1 = new byte[BUFFER_SIZE];
        byte[] buffer2 = new byte[BUFFER_SIZE];
        try (
            InputStream expectedStream = new BufferedInputStream(expected, BUFFER_SIZE);
            InputStream actualStream = new BufferedInputStream(actual, BUFFER_SIZE)
        ) {
            long totalRead = 0;
            while (true) {
                int nRead1 = expectedStream.readNBytes(buffer1, 0, BUFFER_SIZE);
                int nRead2 = actualStream.readNBytes(buffer2, 0, BUFFER_SIZE);

                int i = Arrays.mismatch(buffer1, 0, nRead1, buffer2, 0, nRead2);
                assertThat(message + "(position: " + (totalRead + i) + ')', i, equalTo(-1));
                if (nRead1 < BUFFER_SIZE) {
                    // we've reached the end of the files, but found no mismatch
                    break;
                }
                totalRead += nRead1;
            }
        }
    }

    private static DiscoveryNode findIndexNode(Index index, int shardId) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.INDEX_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    if (shardOrNull != null && shardOrNull.isActive()) {
                        assertTrue(shardOrNull.routingEntry().primary());
                        return indicesService.clusterService().localNode();
                    }
                }
            }
        }
        throw new AssertionError("Cannot finding indexing node for: " + shardId);
    }

    private static IndexShard findIndexShard(Index index, int shardId) {
        return findShard(index, shardId, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
    }

    private static IndexShard findSearchShard(Index index, int shardId) {
        return findShard(index, shardId, DiscoveryNodeRole.SEARCH_ROLE, ShardRouting.Role.SEARCH_ONLY);
    }

    private static IndexShard findShard(Index index, int shardId, DiscoveryNodeRole nodeRole, ShardRouting.Role shardRole) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), nodeRole)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shard = indexService.getShardOrNull(shardId);
                    if (shard != null && shard.isActive()) {
                        assertThat("Unexpected shard role", shard.routingEntry().role(), equalTo(shardRole));
                        return shard;
                    }
                }
            }
        }
        throw new AssertionError(
            "IndexShard instance not found for shard " + new ShardId(index, shardId) + " on nodes with [" + nodeRole.roleName() + "] role"
        );
    }

    private static Map<Index, Integer> resolveIndices() {
        return client().admin()
            .indices()
            .prepareGetIndex()
            .get()
            .getSettings()
            .values()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    settings -> new Index(
                        settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
                        settings.get(IndexMetadata.SETTING_INDEX_UUID)
                    ),
                    settings -> settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)
                )
            );
    }
}
