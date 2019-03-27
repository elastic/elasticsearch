/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class SoftDeletesChangesSnapshot implements Translog.Snapshot {
    static final int DEFAULT_LIVE_READER_BATCH_SIZE = 1024;
    static final int DEFAULT_COMMITTED_READER_BATCH_SIZE = 10;

    private final SoftDeletesPolicy softDeletesPolicy;
    private final MapperService mapperService;
    private final CheckedFunction<IndexCommit, Engine.Searcher, IOException> openIndexCommit;
    private final int committedReaderBatchSize;
    private long targetSeqNo;
    private final long fromSeqNo;
    private final long toSeqNo;
    private boolean requireFullRange;
    private boolean hasUnloadedReader = true;
    private final List<SoftDeletesChangesReader> readers;

    SoftDeletesChangesSnapshot(MapperService mapperService, SoftDeletesPolicy softDeletesPolicy,
                               Engine.Searcher liveSearcher, CheckedFunction<IndexCommit, Engine.Searcher, IOException> openIndexCommit,
                               long fromSeqNo, long toSeqNo, boolean requireFullRange,
                               int liveReaderBatchSize, int committedReaderBatchSize) throws IOException {
        this.mapperService = mapperService;
        this.softDeletesPolicy = softDeletesPolicy;
        this.openIndexCommit = openIndexCommit;
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.targetSeqNo = fromSeqNo;
        this.requireFullRange = requireFullRange;
        this.committedReaderBatchSize = committedReaderBatchSize;
        this.readers = new ArrayList<>();
        this.readers.add(new SoftDeletesChangesReader(liveSearcher, mapperService, liveReaderBatchSize, fromSeqNo, toSeqNo));
    }

    @Override
    public int totalOperations() {
        return readers.stream().mapToInt(SoftDeletesChangesReader::totalOperations).sum();
    }

    @Override
    public int skippedOperations() {
        return readers.stream().mapToInt(SoftDeletesChangesReader::skippedOperations).sum();
    }

    @Override
    public Translog.Operation next() throws IOException {
        if (targetSeqNo > toSeqNo) {
            return null;
        }
        Translog.Operation op = null;
        for (SoftDeletesChangesReader reader : readers) {
            op = minOperation(op, reader.readOperation(targetSeqNo));
            if (op != null && op.seqNo() == targetSeqNo) {
                break;
            }
        }
        while ((op == null || op.seqNo() != targetSeqNo) && hasUnloadedReader) {
            final SoftDeletesChangesReader prevReader = loadPreviousCommittedReader(targetSeqNo);
            if (prevReader != null) {
                readers.add(prevReader);
                op = minOperation(op, prevReader.readOperation(targetSeqNo));
            } else {
                hasUnloadedReader = false;
            }
        }
        if (requireFullRange) {
            rangeCheck(op);
        }
        if (op != null) {
            assert op.seqNo() >= targetSeqNo : "targetSeqNo[" + targetSeqNo + "] op[" + op + "]";
            targetSeqNo = op.seqNo() + 1;
        }
        return op;
    }

    private void rangeCheck(Translog.Operation op) {
        if (op == null) {
            if (targetSeqNo <= toSeqNo) {
                throw new MissingHistoryOperationsException("Not all operations between from_seqno [" + fromSeqNo + "] " +
                    "and to_seqno [" + toSeqNo + "] found; prematurely terminated last operation [" + (targetSeqNo - 1) + "]");
            }
        } else {
            if (op.seqNo() != targetSeqNo) {
                throw new MissingHistoryOperationsException("Not all operations between from_seqno [" + fromSeqNo + "] " +
                    "and to_seqno [" + toSeqNo + "] found; expected seqno [" + targetSeqNo + "]; found [" + op + "]");
            }
        }
    }

    private Translog.Operation minOperation(Translog.Operation o1, Translog.Operation o2) {
        if (o1 == null) {
            return o2;
        }
        if (o2 != null && o1.seqNo() > o2.seqNo()) {
            return o2;
        }
        return o1;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(readers);
    }

    private SoftDeletesChangesReader loadPreviousCommittedReader(long targetSeqNo) throws IOException {
        final IndexCommit lastLoadedCommit = readers.get(readers.size() - 1).directoryReader().getIndexCommit();
        final List<Engine.IndexCommitRef> commitRefs = softDeletesPolicy.acquireRetainingCommits();
        try (Closeable ignored = () -> IOUtils.close(commitRefs)) {
            for (int i = commitRefs.size() - 1; i >= 0; i--) { // traverse from most-recent to least recent
                final IndexCommit commit = commitRefs.get(i).getIndexCommit();
                if (Long.parseLong(commit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)) < targetSeqNo) {
                    break;
                }
                if (commit.compareTo(lastLoadedCommit) < 0 || (commit.compareTo(lastLoadedCommit) == 0 && readers.size() == 1)) {
                    Engine.Searcher searcher = openIndexCommit.apply(commit);
                    try {
                        final SoftDeletesChangesReader reader = new SoftDeletesChangesReader(
                            searcher, mapperService, committedReaderBatchSize, targetSeqNo, toSeqNo);
                        searcher = null;
                        return reader;
                    } finally {
                        IOUtils.close(searcher);
                    }
                }
            }
        }
        return null;
    }
}
