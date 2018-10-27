/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An engine implementation for following shards.
 */
public final class FollowingEngine extends InternalEngine {

    private final CounterMetric numOfOptimizedIndexing = new CounterMetric();

    /**
     * Construct a new following engine with the specified engine configuration.
     *
     * @param engineConfig the engine configuration
     */
    FollowingEngine(final EngineConfig engineConfig) {
        super(validateEngineConfig(engineConfig));
    }

    private static EngineConfig validateEngineConfig(final EngineConfig engineConfig) {
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(engineConfig.getIndexSettings().getSettings()) == false) {
            throw new IllegalArgumentException("a following engine can not be constructed for a non-following index");
        }
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled() == false) {
            throw new IllegalArgumentException("a following engine requires soft deletes to be enabled");
        }
        return engineConfig;
    }

    private void preFlight(final Operation operation) {
        /*
         * We assert here so that this goes uncaught in unit tests and fails nodes in standalone tests (we want a harsh failure so that we
         * do not have a situation where a shard fails and is recovered elsewhere and a test subsequently passes). We throw an exception so
         * that we also prevent issues in production code.
         */
        assert operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
        if (operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalStateException("a following engine does not accept operations without an assigned sequence number");
        }
        assert (operation.origin() == Operation.Origin.PRIMARY) == (operation.versionType() == VersionType.EXTERNAL) :
            "invalid version_type in a following engine; version_type=" + operation.versionType() + "origin=" + operation.origin();
    }

    @Override
    protected InternalEngine.IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        preFlight(index);
        // NOTES: refer Engine#getMaxSeqNoOfUpdatesOrDeletes for the explanation of the optimization using sequence numbers.
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        assert maxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "max_seq_no_of_updates is not initialized";
        if (hasBeenProcessedBefore(index)) {
            if (index.origin() == Operation.Origin.PRIMARY) {
                /*
                 * The existing operation in this engine was probably assigned the term of the previous primary shard which is different
                 * from the term of the current operation. If the current operation arrives on replicas before the previous operation,
                 * then the Lucene content between the primary and replicas are not identical (primary terms are different). We can safely
                 * skip the existing operations below the global checkpoint, however must replicate the ones above the global checkpoint
                 * but with the previous primary term (not the current term of the operation) in order to guarantee the consistency
                 * between the primary and replicas (see TransportBulkShardOperationsAction#shardOperationOnPrimary).
                 */
                final AlreadyProcessedFollowingEngineException error = new AlreadyProcessedFollowingEngineException(
                    shardId, index.seqNo(), lookupPrimaryTerm(index.seqNo()));
                return IndexingStrategy.skipDueToVersionConflict(error, false, index.version(), index.primaryTerm());
            } else {
                return IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            }
        } else if (maxSeqNoOfUpdatesOrDeletes <= getLocalCheckpoint()) {
            assert maxSeqNoOfUpdatesOrDeletes < index.seqNo() : "seq_no[" + index.seqNo() + "] <= msu[" + maxSeqNoOfUpdatesOrDeletes + "]";
            numOfOptimizedIndexing.inc();
            return InternalEngine.IndexingStrategy.optimizedAppendOnly(index.seqNo(), index.version());

        } else {
            return planIndexingAsNonPrimary(index);
        }
    }

    @Override
    protected InternalEngine.DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        preFlight(delete);
        if (delete.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(delete)) {
            // See the comment in #indexingStrategyForOperation for the explanation why we can safely skip this operation.
            final AlreadyProcessedFollowingEngineException error = new AlreadyProcessedFollowingEngineException(
                shardId, delete.seqNo(), lookupPrimaryTerm(delete.seqNo()));
            return DeletionStrategy.skipDueToVersionConflict(error, delete.version(), delete.primaryTerm(), false);
        } else {
            return planDeletionAsNonPrimary(delete);
        }
    }

    @Override
    protected Optional<Exception> preFlightCheckForNoOp(NoOp noOp) throws IOException {
        if (noOp.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(noOp)) {
            // See the comment in #indexingStrategyForOperation for the explanation why we can safely skip this operation.
            final OptionalLong existingTerm = lookupPrimaryTerm(noOp.seqNo());
            return Optional.of(new AlreadyProcessedFollowingEngineException(shardId, noOp.seqNo(), existingTerm));
        } else {
            return super.preFlightCheckForNoOp(noOp);
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // a noop implementation, because follow shard does not own the history but the leader shard does.
        return 0;
    }

    @Override
    protected boolean assertPrimaryIncomingSequenceNumber(final Operation.Origin origin, final long seqNo) {
        // sequence number should be set when operation origin is primary
        assert seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO : "primary operations on a following index must have an assigned sequence number";
        return true;
    }

    @Override
    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        return true;
    }

    @Override
    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert index.version() == 1 && index.versionType() == VersionType.EXTERNAL
                : "version [" + index.version() + "], type [" + index.versionType() + "]";
        return true;
    }

    private OptionalLong lookupPrimaryTerm(final long seqNo) throws IOException {
        refreshIfNeeded("lookup_primary_term", seqNo);
        try (Searcher engineSearcher = acquireSearcher("lookup_primary_term", SearcherScope.INTERNAL)) {
            // We have to acquire a searcher before execute this check to ensure that the requesting seq_no is always found in the else
            // branch. If the operation is at most the global checkpoint, we should not look up its term as we may have merged away the
            // operation. Moreover, we won't need to replicate this operation to replicas since it was processed on every copies already.
            if (seqNo <= engineConfig.getGlobalCheckpointSupplier().getAsLong()) {
                return OptionalLong.empty();
            } else {
                final DirectoryReader reader = Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader());
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                final Query query = new BooleanQuery.Builder()
                    .add(LongPoint.newExactQuery(SeqNoFieldMapper.NAME, seqNo), BooleanClause.Occur.FILTER)
                    // excludes the non-root nested documents which don't have primary_term.
                    .add(new DocValuesFieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME), BooleanClause.Occur.FILTER)
                    .build();
                final TopDocs topDocs = searcher.search(query, 1);
                if (topDocs.scoreDocs.length == 1) {
                    final int docId = topDocs.scoreDocs[0].doc;
                    final LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(docId, reader.leaves()));
                    final NumericDocValues primaryTermDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                    if (primaryTermDV != null && primaryTermDV.advanceExact(docId - leaf.docBase)) {
                        assert primaryTermDV.longValue() > 0 : "invalid term [" + primaryTermDV.longValue() + "]";
                        return OptionalLong.of(primaryTermDV.longValue());
                    }
                }
                assert false : "seq_no[" + seqNo + "] does not have primary_term, total_hits=[" + topDocs.totalHits + "]";
                throw new IllegalStateException("seq_no[" + seqNo + "] does not have primary_term (total_hits=" + topDocs.totalHits + ")");
            }
        } catch (IOException e) {
            try {
                maybeFailEngine("lookup_primary_term", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    /**
     * Returns the number of indexing operations that have been optimized (bypass version lookup) using sequence numbers in this engine.
     * This metric is not persisted, and started from 0 when the engine is opened.
     */
    public long getNumberOfOptimizedIndexing() {
        return numOfOptimizedIndexing.count();
    }
}
