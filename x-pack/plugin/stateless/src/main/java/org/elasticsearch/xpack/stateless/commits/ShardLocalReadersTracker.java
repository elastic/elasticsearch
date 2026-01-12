/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.apache.lucene.index.DirectoryReader;

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
