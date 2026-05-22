/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.Engine;

import java.util.Map;
import java.util.Set;

public class IndexEngineTestUtils {

    private IndexEngineTestUtils() {}

    public static Map<DirectoryReader, Set<PrimaryTermAndGeneration>> getOpenReaders(IndexEngine indexEngine) {
        return indexEngine.getOpenReaders();
    }

    public static long getLatestCommittedGeneration(DirectoryReader directoryReader) {
        return IndexEngine.getLatestCommittedGeneration(directoryReader);
    }

    public static RefreshManager getRefreshManager(IndexEngine indexEngine) {
        return indexEngine.getRefreshManager();
    }

    public static void flushHollow(IndexEngine indexEngine, ActionListener<Engine.FlushResult> listener) {
        indexEngine.flushHollow(listener);
    }

    public static void awaitClose(IndexEngine engine) {
        engine.awaitClose();
    }

    public static void awaitClose(HollowIndexEngine engine) {
        engine.awaitClose();
    }
}
