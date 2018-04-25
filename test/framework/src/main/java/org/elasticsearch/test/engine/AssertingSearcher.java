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

package org.elasticsearch.test.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A searcher that asserts the IndexReader's refcount on close
 */
class AssertingSearcher extends Engine.Searcher {
    private final Engine.Searcher wrappedSearcher;
    private final ShardId shardId;
    private RuntimeException firstReleaseStack;
    private final Object lock = new Object();
    private final int initialRefCount;
    private final Logger logger;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    AssertingSearcher(IndexSearcher indexSearcher, final Engine.Searcher wrappedSearcher,
                             ShardId shardId,
                             Logger logger) {
        super(wrappedSearcher.source(), indexSearcher);
        // we only use the given index searcher here instead of the IS of the wrapped searcher. the IS might be a wrapped searcher
        // with a wrapped reader.
        this.wrappedSearcher = wrappedSearcher;
        this.logger = logger;
        this.shardId = shardId;
        initialRefCount = wrappedSearcher.reader().getRefCount();
        assert initialRefCount > 0 : "IndexReader#getRefCount() was [" + initialRefCount + "] expected a value > [0] - reader is already closed";
    }

    @Override
    public String source() {
        return wrappedSearcher.source();
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed.compareAndSet(false, true)) {
                firstReleaseStack = new RuntimeException();
                final int refCount = wrappedSearcher.reader().getRefCount();
                // this assert seems to be paranoid but given LUCENE-5362 we better add some assertions here to make sure we catch any potential
                // problems.
                assert refCount > 0 : "IndexReader#getRefCount() was [" + refCount + "] expected a value > [0] - reader is already closed. Initial refCount was: [" + initialRefCount + "]";
                try {
                    wrappedSearcher.close();
                } catch (RuntimeException ex) {
                    logger.debug("Failed to release searcher", ex);
                    throw ex;
                }
            } else {
                AssertionError error = new AssertionError("Released Searcher more than once, source [" + wrappedSearcher.source() + "]");
                error.initCause(firstReleaseStack);
                throw error;
            }
        }
    }

    public ShardId shardId() {
        return shardId;
    }

    public boolean isOpen() {
        return closed.get() == false;
    }
}
