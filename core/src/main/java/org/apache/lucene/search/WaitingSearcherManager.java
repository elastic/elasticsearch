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

package org.apache.lucene.search;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;

import java.io.Closeable;
import java.io.IOException;

/**
 * Adds ability to wait completion of searches via acquired {@link IndexSearcher}.
 *
 * @author ikuznetsov
 */
public class WaitingSearcherManager implements Closeable {

    private static final int POLL_TIME_IN_MILLIS = 100;

    public final SearcherManager manager;

    public WaitingSearcherManager(DirectoryReader reader, SearcherFactory searcherFactory) throws IOException {
        this.manager = new SearcherManager(reader, searcherFactory);
    }

    /**
     * Blocks until all searches will be completed.
     * Returns immediately if there is no searches.
     *
     * @param logger for debug logging of wait loop.
     * @return time in millis spent to wait completion.
     * @throws InterruptedException if this thread has been interrupted.
     */
    public long awaitSearchCompletion(ESLogger logger) throws InterruptedException {
        if (manager.current == null ||
            manager.current.getIndexReader().getRefCount() <= 2) {
            return 0;
        }

        logger.debug("waiting search completion (refs: {})", manager.current.getIndexReader());
        long time = System.currentTimeMillis();
        long counter = 0;
        while (true) {
            if (manager.current.getIndexReader().getRefCount() <= 2) {
                long millis = System.nanoTime() - time;
                logger.debug("search waiting completed with [{}] with polls [{}] (refs: {})", TimeValue.timeValueMillis(millis), counter, manager.current.getIndexReader());
                return millis;
            }
            counter++;
            Thread.sleep(POLL_TIME_IN_MILLIS);
        }
    }

    public boolean hasActiveSearches() {
        return manager.current.getIndexReader().getRefCount() > 2;
    }

    public boolean isSearcherCurrent() throws IOException {
        return manager.isSearcherCurrent();
    }

    public IndexSearcher acquire() throws IOException {
        return manager.acquire();
    }

    public void release(IndexSearcher searcher) throws IOException {
        manager.release(searcher);
    }

    @Override
    public void close() throws IOException {
        manager.close();
    }

    public void maybeRefreshBlocking() throws IOException {
        manager.maybeRefreshBlocking();
    }
}
