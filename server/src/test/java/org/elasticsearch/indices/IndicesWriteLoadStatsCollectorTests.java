/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

// public class IndicesWriteLoadStatsCollectorTests extends IndexShardTestCase {
// private LongSupplier timeSupplier = () -> 0L;
//
// @Override
// protected ThreadPool setUpThreadPool() {
// return new TestThreadPool(getClass().getName(), threadPoolSettings()) {
// @Override
// public long rawRelativeTimeInNanos() {
// return timeSupplier.getAsLong();
// }
// };
// }
//
// public void testNotGranularEnoughClock() throws Exception {
// try (var shardRef = createShard(new FakeClock(TimeValue.timeValueMillis(100)))) {
// final var shard = shardRef.shard();
// indexDocs(shard, 100);
//
// final var firstTotalIndexingTimeReading = shard.getTotalIndexingTimeInNanos();
// assertThat(firstTotalIndexingTimeReading, is(greaterThan(0L)));
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(shard));
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(shardsProvider::get, stoppedClock());
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
//
// indexDocs(shard, 10);
//
// assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(firstTotalIndexingTimeReading)));
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
//
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getShardLoadDistributions();
//
// assertThat(shardLoadDistributions, hasSize(1));
//
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
// assertThat(shardWriteLoadDistribution.shardId(), is(equalTo(shard.shardId())));
//
// final var indexingLoadDistribution = shardWriteLoadDistribution.indexingLoadDistribution();
// assertThat(indexingLoadDistribution.p50(), is(equalTo(0.0)));
// assertThat(indexingLoadDistribution.p90(), is(equalTo(0.0)));
// assertThat(indexingLoadDistribution.max(), is(equalTo(0.0)));
// }
// }
//
// public void testIndexingLoadTracking() throws Exception {
// final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
// final var samplingFrequency = TimeValue.timeValueSeconds(1);
// try (var shardRef = createShard(fakeClock)) {
// final var shard = shardRef.shard();
// final var index = shard.shardId().getIndex();
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(shard));
// final var clusterService = mock(ClusterService.class);
// final var clusterState = ClusterState.builder(new ClusterName("cluster"))
// .metadata(
// Metadata.builder()
// .put(getDataStream(index))
// .put(
// IndexMetadata.builder(index.getName())
// .settings(settings(Version.CURRENT))
// .numberOfShards(1)
// .numberOfReplicas(0)
// .build(),
// false
// )
// .build()
// )
// .build();
// when(clusterService.state()).thenReturn(clusterState);
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(
// clusterService,
// shardsProvider::get,
// fakeClock(samplingFrequency)
// );
//
// int numberOfProcessors = randomIntBetween(2, 64);
// int maxCPUsUsed = 0;
// long previousTotalIndexingTimeSample = 0;
// for (int i = 0; i < 60; i++) {
// if (randomBoolean()) {
// // Simulate some long indexing operations taking a few CPUS
// fakeClock.setTickValue(samplingFrequency);
// int numDocs = randomIntBetween(1, numberOfProcessors);
// indexDocs(shard, numDocs);
// maxCPUsUsed = Math.max(maxCPUsUsed, numDocs);
// } else {
// // Otherwise, simulate a few quick indexing ops
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(shard, randomIntBetween(1, 20));
// }
//
// assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(previousTotalIndexingTimeSample)));
// previousTotalIndexingTimeSample = shard.getTotalIndexingTimeInNanos();
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// {
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getWriteLoadDistributionAndReset();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
// assertThat(shardWriteLoadDistribution.indexingMax(), is(closeTo(maxCPUsUsed, 0.5)));
// assertThat(shardWriteLoadDistribution.indexingP90(), is(lessThanOrEqualTo(shardWriteLoadDistribution.indexingMax())));
// assertThat(shardWriteLoadDistribution.indexingP50(), is(lessThanOrEqualTo(shardWriteLoadDistribution.indexingP90())));
// }
//
// {
// // We didn't have any readings after the previous reset
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getWriteLoadDistributionAndReset();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
// assertThat(shardWriteLoadDistribution.indexingMax(), is(equalTo(0.0)));
// assertThat(shardWriteLoadDistribution.indexingP90(), is(equalTo(0.0)));
// assertThat(shardWriteLoadDistribution.indexingP50(), is(equalTo(0.0)));
// }
// }
// }
//
// public void testRefreshLoadTracking() throws Exception {
// final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
// final var samplingFrequency = TimeValue.timeValueSeconds(1);
// try (var shardRef = createShard(fakeClock, NoMergePolicy.INSTANCE)) {
// final var shard = shardRef.shard();
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(shard));
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(
// shardsProvider::get,
// fakeClock(samplingFrequency)
// );
//
// long previousTotalRefreshTimeSample = 0;
// for (int i = 0; i < 60; i++) {
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(shard, randomIntBetween(1, 20));
//
// fakeClock.setTickValue(samplingFrequency);
// shard.refresh("test");
//
// assertThat(shard.getTotalRefreshTimeInNanos(), is(greaterThan(previousTotalRefreshTimeSample)));
// previousTotalRefreshTimeSample = shard.getTotalRefreshTimeInNanos();
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getShardLoadDistributions();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
//
// assertThat(shardWriteLoadDistribution.indexingMax(), is(greaterThan(0.0)));
//
// assertThat(shardWriteLoadDistribution.refreshMax(), is(closeTo(1.0, 0.5)));
// assertThat(shardWriteLoadDistribution.refreshP90(), is(lessThanOrEqualTo(shardWriteLoadDistribution.refreshMax())));
// assertThat(shardWriteLoadDistribution.refreshP50(), is(lessThanOrEqualTo(shardWriteLoadDistribution.refreshP90())));
// }
// }
//
// public void testMergeLoadTracking() throws Exception {
// final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
// final var samplingFrequency = TimeValue.timeValueSeconds(1);
// final var mergePolicy = new SwappableMergePolicy(NoMergePolicy.INSTANCE);
// try (var shardRef = createShard(fakeClock, mergePolicy)) {
// final var shard = shardRef.shard();
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(shard));
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(
// shardsProvider::get,
// fakeClock(samplingFrequency)
// );
//
// for (int i = 0; i < 60; i++) {
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(shard, randomIntBetween(1, 20));
// shard.refresh("test");
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// mergePolicy.swapDelegate(new TieredMergePolicy());
//
// fakeClock.setTickValue(TimeValue.timeValueSeconds(120));
// shard.forceMerge(new ForceMergeRequest().maxNumSegments(1));
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
//
// assertThat(shard.getTotalMergeTimeInMillis(), is(equalTo(TimeValue.timeValueSeconds(120).millis())));
//
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getShardLoadDistributions();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
//
// assertThat(shardWriteLoadDistribution.indexingMax(), is(greaterThan(0.0)));
//
// assertThat(shardWriteLoadDistribution.mergesMax(), is(closeTo(120.0, 0.5)));
// }
// }
//
// public void testDeleteTime() throws Exception {
// final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
// final var samplingFrequency = TimeValue.timeValueSeconds(1);
// try (var shardRef = createShard(fakeClock)) {
// final var shard = shardRef.shard();
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(shard));
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(
// shardsProvider::get,
// fakeClock(samplingFrequency)
// );
//
// for (int i = 0; i < 60; i++) {
// // We want to ensure that we are only measuring delete time
// fakeClock.setTickValue(TimeValue.ZERO);
// final int numDocs = randomIntBetween(1, 20);
// final var docIds = indexDocs(shard, numDocs);
//
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// deleteDocs(shard, docIds);
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// assertThat(shard.getTotalDeleteTimeInNanos(), is(greaterThan(0L)));
//
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getShardLoadDistributions();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
//
// assertThat(shardWriteLoadDistribution.indexingMax(), is(greaterThan(0.0)));
// }
// }
//
// public void testShardLoadDistributionInfoIsClearedAfterDeletion() throws Exception {
// final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
// final var samplingFrequency = TimeValue.timeValueSeconds(1);
// try (var shardRef = createShard(fakeClock)) {
// final var shard = shardRef.shard();
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(shard));
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(
// shardsProvider::get,
// fakeClock(samplingFrequency)
// );
//
// for (int i = 0; i < 60; i++) {
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(shard, randomIntBetween(1, 20));
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getShardLoadDistributions();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
// assertThat(shardWriteLoadDistribution.shardId(), is(equalTo(shard.shardId())));
//
// indicesWriteLoadStatsCollector.afterIndexShardDeleted(shard.shardId(), shard.indexSettings().getSettings());
//
// assertThat(indicesWriteLoadStatsCollector.getShardLoadDistributions(), is(empty()));
// }
// }
//
// public void testRolledOverDataStreamIndicesAreRemovedAfterCollection() throws Exception {
// final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
// final var samplingFrequency = TimeValue.timeValueSeconds(1);
//
// try (var firstDataStreamShardRef = createShard(fakeClock); var rolledOverDataStreamShardRef = createShard(fakeClock)) {
//
// final var firstDataStreamShard = firstDataStreamShardRef.shard();
// final var firstDataStreamIndex = firstDataStreamShard.shardId().getIndex();
//
// final var rolledOverShard = rolledOverDataStreamShardRef.shard();
// final var rolledOverIndex = rolledOverShard.shardId().getIndex();
//
// final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(firstDataStreamShard));
// final var clusterService = mock(ClusterService.class);
// final var clusterState = ClusterState.builder(new ClusterName("cluster"))
// .metadata(
// Metadata.builder()
// .put(getDataStream(firstDataStreamIndex))
// .put(
// IndexMetadata.builder(firstDataStreamIndex.getName())
// .settings(settings(Version.CURRENT))
// .numberOfShards(1)
// .numberOfReplicas(0)
// .build(),
// false
// )
// .build()
// )
// .build();
// when(clusterService.state()).thenReturn(clusterState);
//
// final var indicesWriteLoadStatsCollector = new TestIndicesWriteLoadStatsCollector(
// clusterService,
// shardsProvider::get,
// fakeClock(samplingFrequency)
// ) {
//// @Override
//// protected Iterable<IndexShard> getShardsForIndex(Index index) {
//// if (firstDataStreamShard.shardId().getIndex().equals(index)) {
//// return List.of(firstDataStreamShard);
//// }
//// return Collections.emptyList();
//// }
// };
//
// for (int i = 0; i < 60; i++) {
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(firstDataStreamShard, randomIntBetween(1, 20));
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// final var shardLoadDistributions = indicesWriteLoadStatsCollector.getWriteLoadDistributionAndReset();
//
// assertThat(shardLoadDistributions, hasSize(1));
// final var shardWriteLoadDistribution = shardLoadDistributions.get(0);
// assertThat(shardWriteLoadDistribution.shardId(), is(equalTo(firstDataStreamShard.shardId())));
//
// // take some new samples before rolling over
// for (int i = 0; i < 10; i++) {
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(firstDataStreamShard, randomIntBetween(1, 20));
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// final var clusterStateWithRolledOverDataStream = ClusterState.builder(clusterState)
// .metadata(
// Metadata.builder()
// .put(getDataStream(firstDataStreamIndex, rolledOverIndex))
// .put(
// IndexMetadata.builder(rolledOverIndex.getName())
// .settings(settings(Version.CURRENT))
// .numberOfShards(1)
// .numberOfReplicas(0)
// .build(),
// false
// )
// .build()
// )
// .build();
// when(clusterService.state()).thenReturn(clusterStateWithRolledOverDataStream);
// shardsProvider.set(List.of(rolledOverShard));
//
// for (int i = 0; i < 10; i++) {
// fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
// indexDocs(rolledOverShard, randomIntBetween(1, 20));
//
// indicesWriteLoadStatsCollector.collectWriteLoadStats();
// }
//
// final var writeLoadDistributionAfterRollOver = indicesWriteLoadStatsCollector.getWriteLoadDistributionAndReset();
// assertThat(writeLoadDistributionAfterRollOver, hasSize(2));
// assertThat(
// writeLoadDistributionAfterRollOver.stream().anyMatch(s -> s.shardId().getIndex().equals(firstDataStreamIndex)),
// is(true)
// );
// assertThat(writeLoadDistributionAfterRollOver.stream().anyMatch(s -> s.shardId().getIndex().equals(rolledOverIndex)), is(true));
//
// final var writeLoadAfterRolloverCleanup = indicesWriteLoadStatsCollector.getShardLoadDistributions();
// assertThat(writeLoadAfterRolloverCleanup, hasSize(1));
// final var shardWriteLoadDistributionAfterRollover = writeLoadAfterRolloverCleanup.get(0);
// assertThat(shardWriteLoadDistributionAfterRollover.shardId(), is(equalTo(rolledOverShard.shardId())));
// }
// }
//
// private DataStream getDataStream(Index... indices) {
// return new DataStream(
// "datastream",
// Arrays.stream(indices).toList(),
// 1,
// Collections.emptyMap(),
// false,
// false,
// false,
// false,
// IndexMode.STANDARD
// );
// }
//
// record ShardRef(IndexShard shard) implements AutoCloseable {
// @Override
// public void close() throws IOException {
// IOUtils.close(shard.store(), () -> shard.close("test", false));
// }
// }
//
// private ShardRef createShard(LongSupplier relativeTimeClock) throws Exception {
// return createShard(relativeTimeClock, new TieredMergePolicy());
// }
//
// private ShardRef createShard(LongSupplier relativeTimeClock, MergePolicy mergePolicy) throws Exception {
// final var shardId = new ShardId(new Index(randomAlphaOfLength(10), "_na_"), 0);
// final var shard = newShard(shardId, true, Settings.EMPTY, config -> {
// final EngineConfig configWithMergePolicy = new EngineConfig(
// config.getShardId(),
// config.getThreadPool(),
// config.getIndexSettings(),
// config.getWarmer(),
// config.getStore(),
// mergePolicy,
// config.getAnalyzer(),
// config.getSimilarity(),
// new CodecService(null),
// config.getEventListener(),
// config.getQueryCache(),
// config.getQueryCachingPolicy(),
// config.getTranslogConfig(),
// config.getFlushMergesAfter(),
// config.getExternalRefreshListener(),
// config.getInternalRefreshListener(),
// config.getIndexSort(),
// config.getCircuitBreakerService(),
// config.getGlobalCheckpointSupplier(),
// config.retentionLeasesSupplier(),
// config.getPrimaryTermSupplier(),
// IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
// config.getLeafSorter()
// );
// return new InternalEngine(configWithMergePolicy);
// });
// recoverShardFromStore(shard);
// setUpRelativeTimeClock(relativeTimeClock);
// return new ShardRef(shard);
// }
//
// private List<String> indexDocs(IndexShard shard, int numDocs) throws Exception {
// final List<String> docIds = new ArrayList<>();
// for (int i = 0; i < numDocs; i++) {
// final var id = UUIDs.randomBase64UUID();
// final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
// Versions.MATCH_ANY,
// VersionType.INTERNAL,
// new SourceToParse(id, new BytesArray("{}"), XContentType.JSON),
// SequenceNumbers.UNASSIGNED_SEQ_NO,
// 0,
// IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
// false
// );
// assertThat(result.getResultType(), is(equalTo(Engine.Result.Type.SUCCESS)));
// // assertThat(result.getTook(), is(greaterThan(0L)));
// docIds.add(id);
// }
// return docIds;
// }
//
// private void deleteDocs(IndexShard shard, List<String> docIds) throws Exception {
// for (String docId : docIds) {
// final var result = shard.applyDeleteOperationOnPrimary(
// 1,
// docId,
// VersionType.INTERNAL,
// UNASSIGNED_SEQ_NO,
// UNASSIGNED_PRIMARY_TERM
// );
// assertThat(result.getResultType(), is(equalTo(Engine.Result.Type.SUCCESS)));
// assertThat(result.getTook(), is(greaterThan(0L)));
// }
// }
//
// /**
// * Creates a fake clock that advances the given {@param tickTime} in every
// * call to the clock
// */
// private FakeClock fakeClock(TimeValue tickTime) {
// return new FakeClock(tickTime);
// }
//
// private LongSupplier stoppedClock() {
// var clock = new AtomicLong(randomIntBetween(0, 100000));
// return clock::get;
// }
//
// private void setUpRelativeTimeClock(LongSupplier clock) {
// timeSupplier = clock;
// }
//
// static class FakeClock implements LongSupplier {
// private final AtomicLong clock = new AtomicLong();
// private TimeValue tickTime;
//
// FakeClock(TimeValue tickValue) {
// this.tickTime = tickValue;
// }
//
// @Override
// public long getAsLong() {
// return clock.getAndAdd(tickTime.nanos());
// }
//
// void setTickValue(TimeValue tickTime) {
// this.tickTime = tickTime;
// }
// }
//
// private static final class SwappableMergePolicy extends MergePolicy {
// private volatile MergePolicy delegate;
//
// SwappableMergePolicy(MergePolicy delegate) {
// this.delegate = delegate;
// }
//
// void swapDelegate(MergePolicy delegate) {
// this.delegate = delegate;
// }
//
// @Override
// public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
// throws IOException {
// return delegate.findMerges(mergeTrigger, segmentInfos, mergeContext);
// }
//
// @Override
// public MergeSpecification findForcedMerges(
// SegmentInfos segmentInfos,
// int maxSegmentCount,
// Map<SegmentCommitInfo, Boolean> segmentsToMerge,
// MergeContext mergeContext
// ) throws IOException {
// return delegate.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
// }
//
// @Override
// public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
// return delegate.findForcedDeletesMerges(segmentInfos, mergeContext);
// }
//
// @Override
// public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
// throws IOException {
// return delegate.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
// }
//
// @Override
// public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, MergeContext mergeContext) throws IOException {
// return delegate.useCompoundFile(infos, mergedInfo, mergeContext);
// }
//
// @Override
// public long size(SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
// return Long.MAX_VALUE;
// }
//
// @Override
// public double getNoCFSRatio() {
// return delegate.getNoCFSRatio();
// }
//
// @Override
// public void setNoCFSRatio(double noCFSRatio) {
// delegate.setNoCFSRatio(noCFSRatio);
// }
//
// @Override
// public double getMaxCFSSegmentSizeMB() {
// return delegate.getMaxCFSSegmentSizeMB();
// }
//
// @Override
// public void setMaxCFSSegmentSizeMB(double v) {
// delegate.setMaxCFSSegmentSizeMB(v);
// }
//
// @Override
// public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
// return delegate.keepFullyDeletedSegment(readerIOSupplier);
// }
//
// @Override
// public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) throws IOException {
// return delegate.numDeletesToMerge(info, delCount, readerSupplier);
// }
// }
//
// private static class TestIndicesWriteLoadStatsCollector extends IndicesWriteLoadStatsCollector {
// private final Supplier<List<IndexShard>> shardsSupplier;
//
// TestIndicesWriteLoadStatsCollector(Supplier<List<IndexShard>> shardsSupplier, LongSupplier relativeTimeInNanosSupplier) {
// this(mock(ClusterService.class), shardsSupplier, relativeTimeInNanosSupplier);
// }
//
// TestIndicesWriteLoadStatsCollector(
// ClusterService clusterService,
// Supplier<List<IndexShard>> shardsSupplier,
// LongSupplier relativeTimeInNanosSupplier
// ) {
// super(clusterService, relativeTimeInNanosSupplier);
// this.shardsSupplier = shardsSupplier;
// }
//
// List<IndexShard> getDataStreamsWriteIndicesShards() {
// return shardsSupplier.get();
// }
//
// @Override
// String getParentDataStreamName(String indexName) {
// return "data_stream";
// }
// }
//
// }
