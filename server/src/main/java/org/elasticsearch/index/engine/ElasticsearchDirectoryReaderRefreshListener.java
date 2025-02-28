/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ElasticsearchDirectoryReaderRefreshListener {

    private final ReferenceManager<ElasticsearchDirectoryReader> readerManager;
    private final List<ReferenceManager.RefreshListener> listeners;

    public ElasticsearchDirectoryReaderRefreshListener(
        ReferenceManager<ElasticsearchDirectoryReader> readerManager,
        List<ReferenceManager.RefreshListener> listeners
    ) {
        this.readerManager = Objects.requireNonNull(readerManager);
        this.listeners = List.copyOf(listeners);
        this.readerManager.addListener(new InternalRefreshListener());
    }

    private class InternalRefreshListener implements ReferenceManager.RefreshListener {

        @Override
        public void beforeRefresh() throws IOException {
            for (var listener : listeners) {
                listener.beforeRefresh();
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            var reader = readerManager.acquire();
            try {
                for (var listener : listeners) {
                    if (listener instanceof ReaderAwareRefreshListener l) {
                        l.afterRefresh(didRefresh, reader);
                    } else {
                        listener.afterRefresh(didRefresh);
                    }
                }
            } finally {
                readerManager.release(reader);
            }
        }
    }
}
