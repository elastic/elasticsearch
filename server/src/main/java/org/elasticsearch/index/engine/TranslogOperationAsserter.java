/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.Objects;

/**
 *
 * A utility class to assert that translog operations with the same sequence number
 * in the same generation are either identical or equivalent when synthetic sources are used.
 */
public abstract class TranslogOperationAsserter {
    public static final TranslogOperationAsserter DEFAULT = new TranslogOperationAsserter() {
    };

    private TranslogOperationAsserter() {

    }

    public static TranslogOperationAsserter withEngineConfig(EngineConfig engineConfig) {
        return new TranslogOperationAsserter() {
            @Override
            boolean sameIndexOperation(Translog.Index o1, Translog.Index o2) throws IOException {
                if (super.sameIndexOperation(o1, o2)) {
                    return true;
                }
                if (engineConfig.getIndexSettings().isRecoverySourceSyntheticEnabled()) {
                    return super.sameIndexOperation(synthesizeSource(engineConfig, o1), o2)
                        || super.sameIndexOperation(o1, synthesizeSource(engineConfig, o2));
                }
                return false;
            }
        };
    }

    static Translog.Index synthesizeSource(EngineConfig engineConfig, Translog.Index op) throws IOException {
        final ShardId shardId = engineConfig.getShardId();
        MappingLookup mappingLookup = engineConfig.getMapperService().mappingLookup();
        DocumentParser documentParser = engineConfig.getMapperService().documentParser();
        try (var reader = new TranslogDirectoryReader(shardId, op, mappingLookup, documentParser, engineConfig, () -> {})) {
            final Engine.Searcher searcher = new Engine.Searcher(
                "assert_translog",
                reader,
                engineConfig.getSimilarity(),
                engineConfig.getQueryCache(),
                engineConfig.getQueryCachingPolicy(),
                () -> {}
            );
            try (
                LuceneSyntheticSourceChangesSnapshot snapshot = new LuceneSyntheticSourceChangesSnapshot(
                    mappingLookup,
                    searcher,
                    LuceneSyntheticSourceChangesSnapshot.DEFAULT_BATCH_SIZE,
                    Integer.MAX_VALUE,
                    op.seqNo(),
                    op.seqNo(),
                    true,
                    false,
                    engineConfig.getIndexSettings().getIndexVersionCreated()
                )
            ) {
                final Translog.Operation normalized = snapshot.next();
                assert normalized != null : "expected one operation; got zero";
                return (Translog.Index) normalized;
            }
        }
    }

    boolean sameIndexOperation(Translog.Index o1, Translog.Index o2) throws IOException {
        // TODO: We haven't had timestamp for Index operations in Lucene yet, we need to loosen this check without timestamp.
        return Objects.equals(o1.id(), o2.id())
            && Objects.equals(o1.source(), o2.source())
            && Objects.equals(o1.routing(), o2.routing())
            && o1.primaryTerm() == o2.primaryTerm()
            && o1.seqNo() == o2.seqNo()
            && o1.version() == o2.version();
    }

    public boolean assertSameOperations(
        long seqNo,
        long generation,
        Translog.Operation newOp,
        Translog.Operation prvOp,
        Exception prevFailure
    ) throws IOException {
        final boolean sameOp;
        if (newOp instanceof final Translog.Index o2 && prvOp instanceof final Translog.Index o1) {
            sameOp = sameIndexOperation(o1, o2);
        } else if (newOp instanceof final Translog.Delete o1 && prvOp instanceof final Translog.Delete o2) {
            sameOp = Objects.equals(o1.id(), o2.id())
                && o1.primaryTerm() == o2.primaryTerm()
                && o1.seqNo() == o2.seqNo()
                && o1.version() == o2.version();
        } else {
            sameOp = false;
        }
        if (sameOp == false) {
            throw new AssertionError(
                "seqNo ["
                    + seqNo
                    + "] was processed twice in generation ["
                    + generation
                    + "], with different data. "
                    + "prvOp ["
                    + prvOp
                    + "], newOp ["
                    + newOp
                    + "]",
                prevFailure
            );
        }
        return true;
    }
}
