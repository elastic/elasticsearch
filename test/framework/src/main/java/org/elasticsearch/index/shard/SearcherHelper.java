/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.search.stats.ShardFieldUsageTracker;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.mockito.Mockito.mock;

public class SearcherHelper {

    public static Engine.Searcher wrapSearcher(Engine.Searcher engineSearcher,
                                               CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper) {
        try {
            return IndexShard.wrapSearcher(engineSearcher, mock(ShardFieldUsageTracker.FieldUsageStatsTrackingSession.class),
                readerWrapper);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
