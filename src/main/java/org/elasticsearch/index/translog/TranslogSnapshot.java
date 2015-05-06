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

package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TranslogSnapshot implements Translog.Snapshot {

    private final List<ChannelSnapshot> orderedTranslogs;
    private final ESLogger logger;
    private final ByteBuffer cacheBuffer;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final int estimatedTotalOperations;
    private int currentTranslog;

    /**
     * Create a snapshot of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     */
    public TranslogSnapshot(List<ChannelSnapshot> orderedTranslogs, ESLogger logger) {
        this.orderedTranslogs = orderedTranslogs;
        this.logger = logger;
        int ops = 0;
        for (ChannelSnapshot translog : orderedTranslogs) {

            final int tops = translog.estimatedTotalOperations();
            if (tops < 0) {
                ops = ChannelReader.UNKNOWN_OP_COUNT;
                break;
            }
            ops += tops;
        }
        estimatedTotalOperations = ops;
        cacheBuffer = ByteBuffer.allocate(1024);
        currentTranslog = 0;
    }


    @Override
    public int estimatedTotalOperations() {
        return estimatedTotalOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        ensureOpen();
        for (; currentTranslog < orderedTranslogs.size(); currentTranslog++) {
            final ChannelSnapshot current = orderedTranslogs.get(currentTranslog);
            Translog.Operation op = null;
            try {
                op = current.next(cacheBuffer);
            } catch (TruncatedTranslogException e) {
                // file is empty or header has been half-written and should be ignored
                logger.trace("ignoring truncation exception, the translog [{}] is either empty or half-written", e, current.translogId());
            }
            if (op != null) {
                return op;
            }
        }
        return null;
    }

    protected void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("snapshot already closed");
        }
    }

    @Override
    public void close() throws ElasticsearchException {
        if (closed.compareAndSet(false, true)) {
            try {
                IOUtils.close(orderedTranslogs);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close channel snapshots", e);
            } finally {
                orderedTranslogs.clear();
            }
        }
    }
}
