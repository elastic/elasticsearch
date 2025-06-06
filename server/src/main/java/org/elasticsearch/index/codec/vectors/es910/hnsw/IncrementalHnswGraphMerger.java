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
package org.elasticsearch.index.codec.vectors.es910.hnsw;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * This merges multiple graphs in a single thread in incremental fashion.
 */
public class IncrementalHnswGraphMerger implements HnswGraphMerger {

    protected final FieldInfo fieldInfo;
    protected final RandomVectorScorerSupplier scorerSupplier;
    protected final int M;
    protected final int beamWidth;

    protected List<GraphReader> graphReaders = new ArrayList<>();
    private int numReaders = 0;

    /** Represents a vector reader that contains graph info. */
    protected record GraphReader(KnnVectorsReader reader, MergeState.DocMap initDocMap, int graphSize) {}

    /**
     * @param fieldInfo FieldInfo for the field being merged
     */
    public IncrementalHnswGraphMerger(FieldInfo fieldInfo, RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth) {
        this.fieldInfo = fieldInfo;
        this.scorerSupplier = scorerSupplier;
        this.M = M;
        this.beamWidth = beamWidth;
    }

    /**
     * Adds a reader to the graph merger if it meets the following criteria: 1. does not contain any
     * deleted docs 2. is a HnswGraphProvider
     */
    @Override
    public IncrementalHnswGraphMerger addReader(KnnVectorsReader reader, MergeState.DocMap docMap, Bits liveDocs) throws IOException {
        numReaders++;
        if (hasDeletes(liveDocs) || (reader instanceof HnswGraphProvider == false)) {
            return this;
        }
        HnswGraph graph = ((HnswGraphProvider) reader).getGraph(fieldInfo.name);
        if (graph == null || graph.size() == 0) {
            return this;
        }

        int candidateVectorCount = 0;
        switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> {
                ByteVectorValues byteVectorValues = reader.getByteVectorValues(fieldInfo.name);
                if (byteVectorValues == null) {
                    return this;
                }
                candidateVectorCount = byteVectorValues.size();
            }
            case FLOAT32 -> {
                FloatVectorValues vectorValues = reader.getFloatVectorValues(fieldInfo.name);
                if (vectorValues == null) {
                    return this;
                }
                candidateVectorCount = vectorValues.size();
            }
        }
        graphReaders.add(new GraphReader(reader, docMap, candidateVectorCount));
        return this;
    }

    /**
     * Builds a new HnswGraphBuilder
     *
     * @param mergedVectorValues vector values in the merged segment
     * @param maxOrd max num of vectors that will be merged into the graph
     * @return HnswGraphBuilder
     * @throws IOException If an error occurs while reading from the merge state
     */
    protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd) throws IOException {
        if (graphReaders.size() == 0) {
            return HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, maxOrd);
        }
        graphReaders.sort(Comparator.comparingInt(GraphReader::graphSize).reversed());

        final BitSet initializedNodes = graphReaders.size() == numReaders ? null : new FixedBitSet(maxOrd);
        int[][] ordMaps = getNewOrdMapping(mergedVectorValues, initializedNodes);
        HnswGraph[] graphs = new HnswGraph[graphReaders.size()];
        for (int i = 0; i < graphReaders.size(); i++) {
            HnswGraph graph = ((HnswGraphProvider) graphReaders.get(i).reader).getGraph(fieldInfo.name);
            if (graph.size() == 0) {
                throw new IllegalStateException("Graph should not be empty");
            }
            graphs[i] = graph;
        }

        return MergingHnswGraphBuilder.fromGraphs(
            scorerSupplier,
            beamWidth,
            HnswGraphBuilder.randSeed,
            graphs,
            ordMaps,
            maxOrd,
            initializedNodes
        );
    }

    protected final int[][] getNewOrdMapping(KnnVectorValues mergedVectorValues, BitSet initializedNodes) throws IOException {
        final int numGraphs = graphReaders.size();
        IntIntHashMap[] newDocIdToOldOrdinals = new IntIntHashMap[numGraphs];
        final int[][] oldToNewOrdinalMap = new int[numGraphs][];
        for (int i = 0; i < numGraphs; i++) {
            KnnVectorValues.DocIndexIterator vectorsIter = null;
            switch (fieldInfo.getVectorEncoding()) {
                case BYTE -> vectorsIter = graphReaders.get(i).reader.getByteVectorValues(fieldInfo.name).iterator();
                case FLOAT32 -> vectorsIter = graphReaders.get(i).reader.getFloatVectorValues(fieldInfo.name).iterator();
            }
            newDocIdToOldOrdinals[i] = new IntIntHashMap(graphReaders.get(i).graphSize);
            MergeState.DocMap docMap = graphReaders.get(i).initDocMap();
            for (int docId = vectorsIter.nextDoc(); docId != NO_MORE_DOCS; docId = vectorsIter.nextDoc()) {
                int newDocId = docMap.get(docId);
                newDocIdToOldOrdinals[i].put(newDocId, vectorsIter.index());
            }
            oldToNewOrdinalMap[i] = new int[graphReaders.get(i).graphSize];
        }

        KnnVectorValues.DocIndexIterator mergedVectorIterator = mergedVectorValues.iterator();
        for (int docId = mergedVectorIterator.nextDoc(); docId < NO_MORE_DOCS; docId = mergedVectorIterator.nextDoc()) {
            int newOrd = mergedVectorIterator.index();
            for (int i = 0; i < numGraphs; i++) {
                int oldOrd = newDocIdToOldOrdinals[i].getOrDefault(docId, -1);
                if (oldOrd != -1) {
                    oldToNewOrdinalMap[i][oldOrd] = newOrd;
                    if (initializedNodes != null) {
                        initializedNodes.set(newOrd);
                    }
                    break;
                }
            }
        }
        return oldToNewOrdinalMap;
    }

    @Override
    public OnHeapHnswGraph merge(KnnVectorValues mergedVectorValues, InfoStream infoStream, int maxOrd) throws IOException {
        HnswBuilder builder = createBuilder(mergedVectorValues, maxOrd);
        builder.setInfoStream(infoStream);
        return builder.build(maxOrd);
    }

    private static boolean hasDeletes(Bits liveDocs) {
        if (liveDocs == null) {
            return false;
        }

        for (int i = 0; i < liveDocs.length(); i++) {
            if (liveDocs.get(i) == false) {
                return true;
            }
        }
        return false;
    }
}
