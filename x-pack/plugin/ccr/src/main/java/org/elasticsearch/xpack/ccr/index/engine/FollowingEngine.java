/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An engine implementation for following shards.
 */
public class FollowingEngine extends InternalEngine {


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
        assert FollowingEngineAssertions.preFlight(operation);
        if (operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new ElasticsearchStatusException("a following engine does not accept operations without an assigned sequence number",
                RestStatus.FORBIDDEN);
        }
    }

    @Override
    protected InternalEngine.IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        preFlight(index);
        if (index.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(index)) {
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
            return IndexingStrategy.skipDueToVersionConflict(error, false, index.version());
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
            return DeletionStrategy.skipDueToVersionConflict(error, delete.version(), false);
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
    protected long generateSeqNoForOperationOnPrimary(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        assert operation.seqNo() >= 0 : "ops should have an assigned seq no. but was: " + operation.seqNo();
        markSeqNoAsSeen(operation.seqNo()); // even though we're not generating a sequence number, we mark it as seen
        return operation.seqNo();
    }

    @Override
    protected void advanceMaxSeqNoOfDeleteOnPrimary(long seqNo) {
        if (Assertions.ENABLED) {
            final long localCheckpoint = getProcessedLocalCheckpoint();
            final long maxSeqNoOfUpdates = getMaxSeqNoOfUpdatesOrDeletes();
            assert localCheckpoint < maxSeqNoOfUpdates || maxSeqNoOfUpdates >= seqNo :
                "maxSeqNoOfUpdates is not advanced local_checkpoint=" + localCheckpoint + " msu=" + maxSeqNoOfUpdates + " seq_no=" + seqNo;
        }

        super.advanceMaxSeqNoOfDeleteOnPrimary(seqNo);
    }

    @Override
    protected void advanceMaxSeqNoOfUpdateOnPrimary(long seqNo) {
        // In some scenarios it is possible to advance maxSeqNoOfUpdatesOrDeletes over the leader
        // maxSeqNoOfUpdatesOrDeletes, since in this engine (effectively it is a replica) we don't check if the previous version
        // was a delete and it's possible to consider it as an update, advancing the max sequence number over the leader
        // maxSeqNoOfUpdatesOrDeletes.
        // The goal of this marker it's just an optimization and it won't affect the correctness or durability of the indexed documents.

        // See FollowingEngineTests#testConcurrentUpdateOperationsWithDeletesCanAdvanceMaxSeqNoOfUpdates or #72527 for more details.
        super.advanceMaxSeqNoOfUpdateOnPrimary(seqNo);
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // a noop implementation, because follow shard does not own the history but the leader shard does.
        return 0;
    }

    @Override
    protected boolean assertPrimaryIncomingSequenceNumber(final Operation.Origin origin, final long seqNo) {
        assert FollowingEngineAssertions.assertPrimaryIncomingSequenceNumber(origin, seqNo);
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
        // Don't need to look up term for operations before the global checkpoint for they were processed on every copies already.
        if (seqNo <= engineConfig.getGlobalCheckpointSupplier().getAsLong()) {
            return OptionalLong.empty();
        }
        refreshIfNeeded("lookup_primary_term", seqNo);
        try (Searcher engineSearcher = acquireSearcher("lookup_primary_term", SearcherScope.INTERNAL)) {
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
            if (seqNo <= engineConfig.getGlobalCheckpointSupplier().getAsLong()) {
                return OptionalLong.empty(); // we have merged away the looking up operation.
            } else {
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

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        // the value of the global checkpoint is not verified when the following engine is closed,
        // allowing it to be closed even in the case where all operations have not been fetched and
        // processed from the leader and the operations history has gaps. This way the following
        // engine can be closed and reopened in order to bootstrap the follower index again.
    }
}
