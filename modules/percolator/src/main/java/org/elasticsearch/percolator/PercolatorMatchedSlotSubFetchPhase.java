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
package org.elasticsearch.percolator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.percolator.PercolatorHighlightSubFetchPhase.locatePercolatorQuery;

/**
 * Adds a special field to the a percolator query hit to indicate which documents matched with the percolator query.
 * This is useful when multiple documents are being percolated in a single request.
 */
final class PercolatorMatchedSlotSubFetchPhase implements FetchSubPhase {

    static final String FIELD_NAME_PREFIX = "_percolator_document_slot";

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        innerHitsExecute(context.query(), context.searcher(), hits);
    }

    static void innerHitsExecute(Query mainQuery,
                                 IndexSearcher indexSearcher,
                                 SearchHit[] hits) throws IOException {
        List<PercolateQuery> percolateQueries = locatePercolatorQuery(mainQuery);
        if (percolateQueries.isEmpty()) {
            return;
        }

        boolean singlePercolateQuery = percolateQueries.size() == 1;
        for (PercolateQuery percolateQuery : percolateQueries) {
            String fieldName = singlePercolateQuery ? FIELD_NAME_PREFIX : FIELD_NAME_PREFIX + "_" + percolateQuery.getName();
            IndexSearcher percolatorIndexSearcher = percolateQuery.getPercolatorIndexSearcher();
            Query nonNestedFilter = percolatorIndexSearcher.rewrite(Queries.newNonNestedFilter());
            Weight weight = percolatorIndexSearcher.createWeight(nonNestedFilter, ScoreMode.COMPLETE_NO_SCORES, 1f);
            Scorer s = weight.scorer(percolatorIndexSearcher.getIndexReader().leaves().get(0));
            int memoryIndexMaxDoc = percolatorIndexSearcher.getIndexReader().maxDoc();
            BitSet rootDocs = BitSet.of(s.iterator(), memoryIndexMaxDoc);
            int[] rootDocsBySlot = null;
            boolean hasNestedDocs = rootDocs.cardinality() != percolatorIndexSearcher.getIndexReader().numDocs();
            if (hasNestedDocs) {
                rootDocsBySlot = buildRootDocsSlots(rootDocs);
            }

            PercolateQuery.QueryStore queryStore = percolateQuery.getQueryStore();
            List<LeafReaderContext> ctxs = indexSearcher.getIndexReader().leaves();
            for (SearchHit hit : hits) {
                LeafReaderContext ctx = ctxs.get(ReaderUtil.subIndex(hit.docId(), ctxs));
                int segmentDocId = hit.docId() - ctx.docBase;
                Query query = queryStore.getQueries(ctx).apply(segmentDocId);
                if (query == null) {
                    // This is not a document with a percolator field.
                    continue;
                }
                if (hasNestedDocs) {
                    // Ensures that we filter out nested documents
                    query = new BooleanQuery.Builder()
                        .add(query, BooleanClause.Occur.MUST)
                        .add(nonNestedFilter, BooleanClause.Occur.FILTER)
                        .build();
                }

                TopDocs topDocs = percolatorIndexSearcher.search(query, memoryIndexMaxDoc, new Sort(SortField.FIELD_DOC));
                if (topDocs.totalHits.value == 0) {
                    // This hit didn't match with a percolate query,
                    // likely to happen when percolating multiple documents
                    continue;
                }

                IntStream slots = convertTopDocsToSlots(topDocs, rootDocsBySlot);
                // _percolator_document_slot fields are document fields and should be under "fields" section in a hit
                hit.setDocumentField(fieldName, new DocumentField(fieldName, slots.boxed().collect(Collectors.toList())));
            }
        }
    }

    static IntStream convertTopDocsToSlots(TopDocs topDocs, int[] rootDocsBySlot) {
        IntStream stream = Arrays.stream(topDocs.scoreDocs)
            .mapToInt(scoreDoc -> scoreDoc.doc);
        if (rootDocsBySlot != null) {
            stream = stream.map(docId -> Arrays.binarySearch(rootDocsBySlot, docId));
        }
        return stream;
    }

    static int[] buildRootDocsSlots(BitSet rootDocs) {
        int slot = 0;
        int[] rootDocsBySlot = new int[rootDocs.cardinality()];
        BitSetIterator iterator = new BitSetIterator(rootDocs, 0);
        for (int rootDocId = iterator.nextDoc(); rootDocId != NO_MORE_DOCS; rootDocId = iterator.nextDoc()) {
            rootDocsBySlot[slot++] = rootDocId;
        }
        return rootDocsBySlot;
    }
}
