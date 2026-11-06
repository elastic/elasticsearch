/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests how {@link InternalEngine} applies {@link Engine.Truncate} on a primary and a replica, and what it leaves behind for peers.
 */
public class InternalEngineTruncateTests extends EngineTestCase {

    private Engine.TruncateResult truncateOnPrimary(InternalEngine target) throws IOException {
        return target.truncate(
            new Engine.Truncate(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm.get(), Engine.Operation.Origin.PRIMARY, System.nanoTime())
        );
    }

    private Engine.TruncateResult truncateOnReplica(InternalEngine target, long seqNo) throws IOException {
        return target.truncate(new Engine.Truncate(seqNo, primaryTerm.get(), Engine.Operation.Origin.REPLICA, System.nanoTime()));
    }

    private void indexOnPrimary(String id) throws IOException {
        final ParsedDocument doc = createParsedDoc(id, null);
        assertThat(engine.index(indexForDoc(doc)).getFailure(), nullValue());
    }

    /**
     * Everything indexed before a truncate disappears, what is indexed afterwards is kept, and the sequence numbers keep counting
     * instead of starting over: the truncate itself consumes one.
     */
    public void testTruncateOnPrimaryDiscardsDocumentsAndKeepsCounting() throws IOException {
        final int docsBefore = between(1, 20);
        for (int i = 0; i < docsBefore; i++) {
            indexOnPrimary("before-" + i);
        }
        engine.refresh("test");
        assertVisibleCount(engine, docsBefore);

        final Engine.TruncateResult result = truncateOnPrimary(engine);
        assertThat(result.getFailure(), nullValue());
        assertThat("the truncate takes the next sequence number", result.getSeqNo(), equalTo((long) docsBefore));
        engine.refresh("test");
        assertVisibleCount(engine, 0);
        assertThat(engine.getProcessedLocalCheckpoint(), equalTo(result.getSeqNo()));

        final int docsAfter = between(1, 20);
        for (int i = 0; i < docsAfter; i++) {
            indexOnPrimary("after-" + i);
        }
        engine.refresh("test");
        assertVisibleCount(engine, docsAfter);
        assertThat(engine.getProcessedLocalCheckpoint(), equalTo(result.getSeqNo() + docsAfter));
    }

    /**
     * A document id that existed before the truncate is a brand new document afterwards, rather than an update of the old one.
     */
    public void testIdReusedAfterTruncateIsCreatedAgain() throws IOException {
        indexOnPrimary("a");
        final Engine.IndexResult second = engine.index(indexForDoc(createParsedDoc("a", null)));
        assertThat(second.isCreated(), equalTo(false));
        assertThat(second.getVersion(), equalTo(2L));

        truncateOnPrimary(engine);

        final Engine.IndexResult afterTruncate = engine.index(indexForDoc(createParsedDoc("a", null)));
        assertThat(afterTruncate.isCreated(), equalTo(true));
        assertThat(afterTruncate.getVersion(), equalTo(1L));
    }

    /**
     * Operations that the primary wrote to its translog, including the truncate, replay on a replica to the same state.
     */
    public void testTruncateReplicatesToReplica() throws IOException {
        final int docsBefore = between(1, 10);
        for (int i = 0; i < docsBefore; i++) {
            indexOnPrimary("before-" + i);
        }
        final Engine.TruncateResult result = truncateOnPrimary(engine);
        indexOnPrimary("after");
        engine.syncTranslog();

        try (Translog.Snapshot snapshot = EngineTestCase.getTranslog(engine).newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                applyOperation(replicaEngine, translogHandler.convertToEngineOp(op, Engine.Operation.Origin.REPLICA));
            }
        }
        replicaEngine.refresh("test");
        assertVisibleCount(replicaEngine, 1);
        assertThat(replicaEngine.getProcessedLocalCheckpoint(), equalTo(result.getSeqNo() + 1));
    }

    /**
     * A copy that is recovering can be told about the truncate before it has seen the older operations that the truncate
     * superseded (they arrive late from a history read before the truncate happened). Those must not bring documents back.
     */
    public void testOperationsBelowTruncateThatArriveLateAreNotApplied() throws IOException {
        final long truncateSeqNo = 5;
        truncateOnReplica(replicaEngine, truncateSeqNo);

        for (long seqNo = 0; seqNo < truncateSeqNo; seqNo++) {
            final ParsedDocument doc = createParsedDoc("late-" + seqNo, null);
            final Engine.IndexResult result = replicaEngine.index(replicaIndexForDoc(doc, 1, seqNo, false));
            assertThat(result.getFailure(), nullValue());
        }
        final ParsedDocument after = createParsedDoc("after", null);
        assertThat(replicaEngine.index(replicaIndexForDoc(after, 1, truncateSeqNo + 1, false)).getFailure(), nullValue());

        replicaEngine.refresh("test");
        assertVisibleCount(replicaEngine, 1);
        assertThat(replicaEngine.getProcessedLocalCheckpoint(), equalTo(truncateSeqNo + 1));
    }

    /**
     * Applying the same truncate twice (a recovery can resend what a copy already applied) must not drop documents indexed in between.
     */
    public void testTruncateAppliedTwiceKeepsLaterDocuments() throws IOException {
        truncateOnReplica(replicaEngine, 0);
        final ParsedDocument doc = createParsedDoc("kept", null);
        assertThat(replicaEngine.index(replicaIndexForDoc(doc, 1, 1, false)).getFailure(), nullValue());

        truncateOnReplica(replicaEngine, 0);

        replicaEngine.refresh("test");
        assertVisibleCount(replicaEngine, 1);
    }

    /**
     * A peer that asks for history from before the truncate is given the truncate itself followed by what came after it, because
     * the older operations no longer exist and the truncate stands in for them.
     */
    public void testHistoryServedToPeersStartsAtTruncate() throws IOException {
        final int docsBefore = between(1, 10);
        for (int i = 0; i < docsBefore; i++) {
            indexOnPrimary("before-" + i);
        }
        final Engine.TruncateResult result = truncateOnPrimary(engine);
        final int docsAfter = between(1, 10);
        for (int i = 0; i < docsAfter; i++) {
            indexOnPrimary("after-" + i);
        }
        engine.refresh("test");

        final List<Translog.Operation> history = new ArrayList<>();
        try (
            Translog.Snapshot snapshot = engine.newChangesSnapshot(
                "test",
                0,
                engine.getProcessedLocalCheckpoint(),
                true,
                true,
                true,
                Long.MAX_VALUE
            )
        ) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                history.add(op);
            }
        }
        assertThat(history.size(), equalTo(1 + docsAfter));
        assertThat(history.getFirst(), instanceOf(Translog.Truncate.class));
        assertThat(history.getFirst().seqNo(), equalTo(result.getSeqNo()));
        for (int i = 1; i < history.size(); i++) {
            assertThat(history.get(i), instanceOf(Translog.Index.class));
            assertThat(history.get(i).seqNo(), equalTo(result.getSeqNo() + i));
        }
        assertThat("history entirely below the truncate is empty", engine.countChanges("test", 0, result.getSeqNo() - 1), equalTo(0));
    }

    /**
     * The truncate must survive a restart, whether or not it was flushed: replayed from the translog in the second case and read
     * from the commit in the first.
     */
    public void testTruncateSurvivesRestart() throws IOException {
        final int docsBefore = between(1, 10);
        for (int i = 0; i < docsBefore; i++) {
            indexOnPrimary("before-" + i);
        }
        final Engine.TruncateResult result = truncateOnPrimary(engine);
        indexOnPrimary("after");
        if (randomBoolean()) {
            engine.flush();
        }
        engine.syncTranslog();
        engine.close();

        // createEngine recovers from the translog
        engine = createEngine(store, primaryTranslogDir);
        engine.refresh("test");
        assertVisibleCount(engine, 1);

        // a late operation from below the truncate is still ignored after the restart
        final ParsedDocument late = createParsedDoc("late", null);
        assertThat(engine.index(replicaIndexForDoc(late, 1, result.getSeqNo() - 1, false)).getFailure(), nullValue());
        engine.refresh("test");
        assertVisibleCount(engine, 1);
    }
}
