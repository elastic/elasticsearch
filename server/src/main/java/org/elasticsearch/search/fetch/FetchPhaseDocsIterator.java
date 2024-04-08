/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;
import java.util.Arrays;

/**
 * Given a set of doc ids and an index reader, sorts the docs by id, splits the sorted
 * docs by leaf reader, and iterates through them calling abstract methods
 * {@link #setNextReader(LeafReaderContext, int[])} for each new leaf reader and
 * {@link #nextDoc(int)} for each document; then collects the resulting {@link SearchHit}s
 * into an array and returns them in the order of the original doc ids.
 */
abstract class FetchPhaseDocsIterator {

    /**
     * Called when a new leaf reader is reached
     * @param ctx           the leaf reader for this set of doc ids
     * @param docsInLeaf    the reader-specific docids to be fetched in this leaf reader
     */
    protected abstract void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) throws IOException;

    /**
     * Called for each document within a leaf reader
     * @param doc   the global doc id
     * @return a {@link SearchHit} for the document
     */
    protected abstract SearchHit nextDoc(int doc) throws IOException;

    /**
     * Iterate over a set of docsIds within a particular shard and index reader
     */
    public final SearchHit[] iterate(SearchShardTarget shardTarget, IndexReader indexReader, int[] docIds) {
        SearchHit[] searchHits = new SearchHit[docIds.length];
        DocIdToIndex[] docs = new DocIdToIndex[docIds.length];
        for (int index = 0; index < docIds.length; index++) {
            docs[index] = new DocIdToIndex(docIds[index], index);
        }
        // make sure that we iterate in doc id order
        Arrays.sort(docs);
        int currentDoc = docs[0].docId;
        try {
            int leafOrd = ReaderUtil.subIndex(docs[0].docId, indexReader.leaves());
            LeafReaderContext ctx = indexReader.leaves().get(leafOrd);
            int endReaderIdx = endReaderIdx(ctx, 0, docs);
            int[] docsInLeaf = docIdsInLeaf(0, endReaderIdx, docs, ctx.docBase);
            setNextReader(ctx, docsInLeaf);
            for (int i = 0; i < docs.length; i++) {
                if (i >= endReaderIdx) {
                    leafOrd = ReaderUtil.subIndex(docs[i].docId, indexReader.leaves());
                    ctx = indexReader.leaves().get(leafOrd);
                    endReaderIdx = endReaderIdx(ctx, i, docs);
                    docsInLeaf = docIdsInLeaf(i, endReaderIdx, docs, ctx.docBase);
                    setNextReader(ctx, docsInLeaf);
                }
                currentDoc = docs[i].docId;
                searchHits[docs[i].index] = nextDoc(docs[i].docId);
            }
        } catch (Exception e) {
            for (SearchHit searchHit : searchHits) {
                if (searchHit != null) {
                    searchHit.decRef();
                }
            }
            throw new FetchPhaseExecutionException(shardTarget, "Error running fetch phase for doc [" + currentDoc + "]", e);
        }
        return searchHits;
    }

    private static int endReaderIdx(LeafReaderContext currentReaderContext, int index, DocIdToIndex[] docs) {
        int firstInNextReader = currentReaderContext.docBase + currentReaderContext.reader().maxDoc();
        int i = index + 1;
        while (i < docs.length) {
            if (docs[i].docId >= firstInNextReader) {
                return i;
            }
            i++;
        }
        return i;
    }

    private static int[] docIdsInLeaf(int index, int endReaderIdx, DocIdToIndex[] docs, int docBase) {
        int[] result = new int[endReaderIdx - index];
        int d = 0;
        for (int i = index; i < endReaderIdx; i++) {
            assert docs[i].docId >= docBase;
            result[d++] = docs[i].docId - docBase;
        }
        return result;
    }

    private static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }
}
