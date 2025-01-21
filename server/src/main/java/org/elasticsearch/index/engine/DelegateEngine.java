/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An engine that delegates all operations to another engine.
 */
public class DelegateEngine extends Engine {

    protected final Engine delegateEngine;

    @SuppressWarnings("this-escape")
    public DelegateEngine(EngineConfig engineConfig, Engine delegateEngine) {
        super(engineConfig, Objects.requireNonNull(delegateEngine.ensureOpenRefs));
        this.delegateEngine = delegateEngine;
    }

    public Engine getDelegateEngine() {
        return delegateEngine;
    }

    public static Engine unwrap(Engine engine) {
        if (engine instanceof DelegateEngine delegateEngine) {
            return delegateEngine.getDelegateEngine();
        }
        return engine;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return delegateEngine.getLastSyncedGlobalCheckpoint();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return delegateEngine.getIndexBufferRAMBytesUsed();
    }

    @Override
    public List<Segment> segments() {
        return delegateEngine.segments();
    }

    @Override
    public List<Segment> segments(boolean includeVectorFormatsInfo) {
        return delegateEngine.segments(includeVectorFormatsInfo);
    }

    @Override
    public boolean refreshNeeded() {
        return delegateEngine.refreshNeeded();
    }

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        return delegateEngine.refresh(source);
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        delegateEngine.flushHoldingLock(force, waitIfOngoing, listener);
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {
        delegateEngine.trimUnreferencedTranslogFiles();
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return delegateEngine.shouldRollTranslogGeneration();
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        delegateEngine.rollTranslogGeneration();
    }

    @Override
    public SegmentInfos getLastCommittedSegmentInfos() {
        return delegateEngine.getLastCommittedSegmentInfos();
    }

    @Override
    public String getHistoryUUID() {
        return delegateEngine.getHistoryUUID();
    }

    @Override
    public long getWritingBytes() {
        return delegateEngine.getWritingBytes();
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return delegateEngine.completionStats(fieldNamePatterns);
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return delegateEngine.getIndexThrottleTimeInMillis();
    }

    @Override
    public boolean isThrottled() {
        return delegateEngine.isThrottled();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        delegateEngine.trimOperationsFromTranslog(belowTerm, aboveSeqNo);
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        return delegateEngine.index(index);
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        return delegateEngine.delete(delete);
    }

    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        return delegateEngine.noOp(noOp);
    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return delegateEngine.get(get, mappingLookup, documentParser, searcherWrapper);
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return delegateEngine.getReferenceManager(scope);
    }

    @Override
    public boolean allowSearchIdleOptimization() {
        return delegateEngine.allowSearchIdleOptimization();
    }

    @Override
    public void externalRefresh(String source, ActionListener<RefreshResult> listener) {
        delegateEngine.externalRefresh(source, listener);
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        delegateEngine.maybeRefresh(source, listener);
    }

    @Override
    public void writeIndexingBuffer() throws IOException {
        delegateEngine.writeIndexingBuffer();
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return delegateEngine.shouldPeriodicallyFlush();
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        delegateEngine.asyncEnsureTranslogSynced(location, listener);
    }

    @Override
    public void asyncEnsureGlobalCheckpointSynced(long globalCheckpoint, Consumer<Exception> listener) {
        delegateEngine.asyncEnsureGlobalCheckpointSynced(globalCheckpoint, listener);
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return delegateEngine.isTranslogSyncNeeded();
    }

    @Override
    public void syncTranslog() throws IOException {
        delegateEngine.syncTranslog();
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return delegateEngine.acquireHistoryRetentionLock();
    }

    @Override
    public int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException {
        return delegateEngine.countChanges(source, fromSeqNo, toSeqNo);
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        long maxChunkSize
    ) throws IOException {
        return delegateEngine.newChangesSnapshot(source, fromSeqNo, toSeqNo, requiredFullRange, singleConsumer, accessStats, maxChunkSize);
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return delegateEngine.hasCompleteOperationHistory(reason, startingSeqNo);
    }

    @Override
    public long getMinRetainedSeqNo() {
        return delegateEngine.getMinRetainedSeqNo();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return delegateEngine.getTranslogStats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return delegateEngine.getTranslogLastWriteLocation();
    }

    @Override
    public long getMaxSeqNo() {
        return delegateEngine.getMaxSeqNo();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return delegateEngine.getProcessedLocalCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return delegateEngine.getPersistedLocalCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return delegateEngine.getSeqNoStats(globalCheckpoint);
    }

    @Override
    public void flushAndClose() throws IOException {
        delegateEngine.flushAndClose();
    }

    @Override
    public void activateThrottling() {
        delegateEngine.activateThrottling();
    }

    @Override
    public void deactivateThrottling() {
        delegateEngine.deactivateThrottling();
    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return delegateEngine.restoreLocalHistoryFromTranslog(translogRecoveryRunner);
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return delegateEngine.fillSeqNoGaps(primaryTerm);
    }

    @Override
    public void recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo, ActionListener<Void> listener) {
        delegateEngine.recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo, listener);
    }

    @Override
    public void skipTranslogRecovery() {
        delegateEngine.skipTranslogRecovery();
    }

    @Override
    public void maybePruneDeletes() {
        delegateEngine.maybePruneDeletes();
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        delegateEngine.updateMaxUnsafeAutoIdTimestamp(newTimestamp);
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return delegateEngine.getMaxSeqNoOfUpdatesOrDeletes();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        delegateEngine.advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOnPrimary);
    }

    @Override
    public ShardLongFieldRange getRawFieldRange(String field) throws IOException {
        return delegateEngine.getRawFieldRange(field);
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) throws EngineException,
        IOException {
        delegateEngine.forceMerge(flush, maxNumSegments, onlyExpungeDeletes, forceMergeUUID);
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return delegateEngine.acquireLastIndexCommit(flushFirst);
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return delegateEngine.acquireSafeIndexCommit();
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return delegateEngine.getSafeCommitInfo();
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        delegateEngine.closeNoLock(reason, closedLatch);
    }

    @Override
    public void close() throws IOException {
        delegateEngine.close();
    }

    @Override
    protected boolean isClosed() {
        return delegateEngine.isClosed();
    }

    @Override
    public void failEngine(String reason, Exception failure) {
        delegateEngine.failEngine(reason, failure);
    }

    @Override
    public void addFlushListener(Translog.Location location, ActionListener<Long> listener) {
        delegateEngine.addFlushListener(location, listener);
    }

    @Override
    public void addPrimaryTermAndGenerationListener(long minPrimaryTerm, long minGeneration, ActionListener<Long> listener) {
        delegateEngine.addPrimaryTermAndGenerationListener(minPrimaryTerm, minGeneration, listener);
    }

    @Override
    boolean assertSearcherIsWarmedUp(String source, SearcherScope scope) {
        return delegateEngine.assertSearcherIsWarmedUp(source, scope);
    }

    @Override
    protected Exception getFailedEngine() {
        return delegateEngine.getFailedEngine();
    }

    @Override
    public GetResult getFromTranslog(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return delegateEngine.getFromTranslog(get, mappingLookup, documentParser, searcherWrapper);
    }

    @Override
    public long getLastWriteNanos() {
        return delegateEngine.getLastWriteNanos();
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return delegateEngine.getMaxSeenAutoIdTimestamp();
    }

    @Override
    public MergeStats getMergeStats() {
        return delegateEngine.getMergeStats();
    }

    @Override
    public long getTotalFlushTimeExcludingWaitingOnLockInMillis() {
        return delegateEngine.getTotalFlushTimeExcludingWaitingOnLockInMillis();
    }

    @Override
    protected boolean maybeFailEngine(String source, Exception e) {
        return delegateEngine.maybeFailEngine(source, e);
    }

    @Override
    public void onSettingsChanged() {
        delegateEngine.onSettingsChanged();
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        return delegateEngine.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
    }

    @Override
    public SparseVectorStats sparseVectorStats(MappingLookup mappingLookup) {
        return delegateEngine.sparseVectorStats(mappingLookup);
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        delegateEngine.verifyEngineBeforeIndexClosing();
    }

    @Override
    protected void writerSegmentStats(SegmentsStats stats) {
        delegateEngine.writerSegmentStats(stats);
    }

    @Override
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        return delegateEngine.acquireSearcherSupplier(wrapper, scope);
    }

    @Override
    public DenseVectorStats denseVectorStats(MappingLookup mappingLookup) {
        return delegateEngine.denseVectorStats(mappingLookup);
    }

    @Override
    public DocsStats docStats() {
        return delegateEngine.docStats();
    }

    @Override
    protected void fillSegmentStats(SegmentReader segmentReader, boolean includeSegmentFileSizes, SegmentsStats stats) {
        delegateEngine.fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        return delegateEngine.acquireSearcher(source, scope);
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope, Function<Searcher, Searcher> wrapper) throws EngineException {
        return delegateEngine.acquireSearcher(source, scope, wrapper);
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        delegateEngine.flush(force, waitIfOngoing);
    }
}
