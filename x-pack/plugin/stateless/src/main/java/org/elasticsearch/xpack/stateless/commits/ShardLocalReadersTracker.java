/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tracks all open {@link DirectoryReader} instances at the shard level to ensure their
 * underlying commit blobs are not deleted while still in use.
 * <p>
 * Readers are tied to the {@link org.elasticsearch.index.store.Store} rather than a particular
 * engine instance, so they survive across {@link org.elasticsearch.index.shard.IndexShard#resetEngine} resets.
 * This allows to close and recreate engines without losing or prematurely deleting
 * the blobs still referenced by existing readers.
 */
public class ShardLocalReadersTracker {
    // The values of this map are sets of BCCs referenced by the reader. This map is guarded by the openReaders monitor.
    private final Map<DirectoryReader, Set<PrimaryTermAndGeneration>> openReaders = new HashMap<>();

    private final IndexEngineLocalReaderListener localReaderListener;

    public ShardLocalReadersTracker(IndexEngineLocalReaderListener localReaderListener) {
        this.localReaderListener = localReaderListener;
    }

    public void trackOpenReader(DirectoryReader directoryReader, Set<PrimaryTermAndGeneration> referencedBCCsForCommit) {
        synchronized (openReaders) {
            openReaders.put(directoryReader, referencedBCCsForCommit);
        }
    }

    public void onLocalReaderClosed(DirectoryReader reader) {
        Set<PrimaryTermAndGeneration> bccDependencies;
        Set<PrimaryTermAndGeneration> remainingReferencedBCCs;
        // CHM iterators are weakly consistent, meaning that we're not guaranteed to see new insertions while we compute
        // the set of remainingReferencedBCCs, that's why we use a regular HashMap with synchronized.
        synchronized (openReaders) {
            bccDependencies = openReaders.remove(reader);
            assert bccDependencies != null : openReaders + " -> " + reader;
            assert bccDependencies.isEmpty() == false;
            remainingReferencedBCCs = openReaders.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        }

        long bccHoldingCommit = bccDependencies.stream().max(PrimaryTermAndGeneration::compareTo).get().generation();
        localReaderListener.onLocalReaderClosed(bccHoldingCommit, remainingReferencedBCCs);
    }

    public Map<DirectoryReader, Set<PrimaryTermAndGeneration>> getOpenReaders() {
        synchronized (openReaders) {
            return Map.copyOf(openReaders);
        }
    }
}
