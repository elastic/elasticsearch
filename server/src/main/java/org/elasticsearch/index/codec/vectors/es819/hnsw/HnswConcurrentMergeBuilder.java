/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es819.hnsw;

import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.hnsw.HnswGraphBuilder.HNSW_COMPONENT;

/**
 * A graph builder that manages multiple workers, it only supports adding the whole graph all at
 * once. It will spawn a thread for each worker and the workers will pick the work in batches.
 */
public class HnswConcurrentMergeBuilder implements HnswBuilder {

    private static final int DEFAULT_BATCH_SIZE = 2048; // number of vectors the worker handles sequentially at one batch

    private final TaskExecutor taskExecutor;
    private final ConcurrentMergeWorker[] workers;
    private final HnswLock hnswLock;
    private InfoStream infoStream = InfoStream.getDefault();
    private boolean frozen;

    public HnswConcurrentMergeBuilder(
        TaskExecutor taskExecutor,
        int numWorker,
        RandomVectorScorerSupplier scorerSupplier,
        int beamWidth,
        OnHeapHnswGraph hnsw,
        BitSet initializedNodes
    ) throws IOException {
        this.taskExecutor = taskExecutor;
        AtomicInteger workProgress = new AtomicInteger(0);
        workers = new ConcurrentMergeWorker[numWorker];
        hnswLock = new HnswLock();
        for (int i = 0; i < numWorker; i++) {
            workers[i] = new ConcurrentMergeWorker(
                scorerSupplier.copy(),
                beamWidth,
                HnswGraphBuilder.randSeed,
                hnsw,
                hnswLock,
                initializedNodes,
                workProgress
            );
        }
    }

    @Override
    public OnHeapHnswGraph build(int maxOrd) throws IOException {
        if (frozen) {
            throw new IllegalStateException("graph has already been built");
        }
        if (infoStream.isEnabled(HNSW_COMPONENT)) {
            infoStream.message(HNSW_COMPONENT, "build graph from " + maxOrd + " vectors, with " + workers.length + " workers");
        }
        List<Callable<Void>> futures = new ArrayList<>();
        for (int i = 0; i < workers.length; i++) {
            int finalI = i;
            futures.add(() -> {
                workers[finalI].run(maxOrd);
                return null;
            });
        }
        taskExecutor.invokeAll(futures);
        return getCompletedGraph();
    }

    @Override
    public void addGraphNode(int node) throws IOException {
        throw new UnsupportedOperationException("This builder is for merge only");
    }

    @Override
    public void setInfoStream(InfoStream infoStream) {
        this.infoStream = infoStream;
        for (HnswBuilder worker : workers) {
            worker.setInfoStream(infoStream);
        }
    }

    @Override
    public OnHeapHnswGraph getCompletedGraph() throws IOException {
        if (frozen == false) {
            // should already have been called in build(), but just in case
            finish();
            frozen = true;
        }
        return getGraph();
    }

    private void finish() throws IOException {
        workers[0].finish();
    }

    @Override
    public OnHeapHnswGraph getGraph() {
        return workers[0].getGraph();
    }

    /* test only for now */
    void setBatchSize(int newSize) {
        for (ConcurrentMergeWorker worker : workers) {
            worker.batchSize = newSize;
        }
    }

    private static final class ConcurrentMergeWorker extends HnswGraphBuilder {

        /**
         * A common AtomicInteger shared among all workers, used for tracking what's the next vector to
         * be added to the graph.
         */
        private final AtomicInteger workProgress;

        private final BitSet initializedNodes;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private final UpdateableRandomVectorScorer scorer;

        private ConcurrentMergeWorker(
            RandomVectorScorerSupplier scorerSupplier,
            int beamWidth,
            long seed,
            OnHeapHnswGraph hnsw,
            HnswLock hnswLock,
            BitSet initializedNodes,
            AtomicInteger workProgress
        ) throws IOException {
            super(
                scorerSupplier,
                beamWidth,
                seed,
                hnsw,
                hnswLock,
                new MergeSearcher(new NeighborQueue(beamWidth, true), hnswLock, new FixedBitSet(hnsw.maxNodeId() + 1))
            );
            this.workProgress = workProgress;
            this.initializedNodes = initializedNodes;
            this.scorer = scorerSupplier.scorer();
        }

        /**
         * This method first try to "reserve" part of work by calling {@link #getStartPos(int)} and then
         * calling {@link #addVectors(int, int)} to actually add the nodes to the graph. By doing this
         * we are able to dynamically allocate the work to multiple workers and try to make all of them
         * finishing around the same time.
         */
        private void run(int maxOrd) throws IOException {
            int start = getStartPos(maxOrd);
            int end;
            while (start != -1) {
                end = Math.min(maxOrd, start + batchSize);
                addVectors(start, end);
                start = getStartPos(maxOrd);
            }
        }

        /** Reserve the work by atomically increment the {@link #workProgress} */
        private int getStartPos(int maxOrd) {
            int start = workProgress.getAndAdd(batchSize);
            if (start < maxOrd) {
                return start;
            } else {
                return -1;
            }
        }

        @Override
        public void addGraphNode(int node, UpdateableRandomVectorScorer scorer) throws IOException {
            if (initializedNodes != null && initializedNodes.get(node)) {
                return;
            }
            super.addGraphNode(node, scorer);
        }

        @Override
        public void addGraphNode(int node) throws IOException {
            if (initializedNodes != null && initializedNodes.get(node)) {
                return;
            }
            scorer.setScoringOrdinal(node);
            addGraphNode(node, scorer);
        }
    }

    /**
     * This searcher will obtain the lock and make a copy of neighborArray when seeking the graph such
     * that concurrent modification of the graph will not impact the search
     */
    private static class MergeSearcher extends HnswGraphSearcher {
        private final HnswLock hnswLock;
        private int[] nodeBuffer;
        private int upto;
        private int size;

        private MergeSearcher(NeighborQueue candidates, HnswLock hnswLock, BitSet visited) {
            super(candidates, visited);
            this.hnswLock = hnswLock;
        }

        @Override
        void graphSeek(HnswGraph graph, int level, int targetNode) {
            Lock lock = hnswLock.read(level, targetNode);
            try {
                NeighborArray neighborArray = ((OnHeapHnswGraph) graph).getNeighbors(level, targetNode);
                if (nodeBuffer == null || nodeBuffer.length < neighborArray.size()) {
                    nodeBuffer = new int[neighborArray.size()];
                }
                size = neighborArray.size();
                System.arraycopy(neighborArray.nodes(), 0, nodeBuffer, 0, size);
            } finally {
                lock.unlock();
            }
            upto = -1;
        }

        @Override
        int graphNextNeighbor(HnswGraph graph) {
            if (++upto < size) {
                return nodeBuffer[upto];
            }
            return NO_MORE_DOCS;
        }
    }
}
