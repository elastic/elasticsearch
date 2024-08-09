/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever.rankdoc;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.NumericComparator;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link SortField} that sorts documents by their rank as computed through the {@code RankDocsRetrieverBuilder}.
 * This is used when we want to score and rank the documents irrespective of their original scores,
 * but based on the provided rank they were assigned, e.g. through an RRF retriever.
 **/
public class RankDocsSortField extends SortField {

    public static final String NAME = "_rank";

    public RankDocsSortField(RankDoc[] rankDocs) {
        super(NAME, new FieldComparatorSource() {
            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
                return new RankDocsComparator(numHits, rankDocs);
            }
        });
    }

    private static class RankDocsComparator extends NumericComparator<Integer> {
        private final int[] values;
        private final Map<Integer, Integer> rankDocMap;
        private int topValue;
        private int bottom;

        private RankDocsComparator(int numHits, RankDoc[] rankDocs) {
            super(NAME, Integer.MAX_VALUE, false, Pruning.NONE, Integer.BYTES);
            this.values = new int[numHits];
            this.rankDocMap = Arrays.stream(rankDocs).collect(Collectors.toMap(k -> k.doc, v -> v.rank));
        }

        @Override
        public int compare(int slot1, int slot2) {
            return Integer.compare(values[slot1], values[slot2]);
        }

        @Override
        public Integer value(int slot) {
            return Integer.valueOf(values[slot]);
        }

        @Override
        public void setTopValue(Integer value) {
            topValue = value;
        }

        @Override
        public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
            IntToIntFunction docToRank = doc -> rankDocMap.getOrDefault(context.docBase + doc, Integer.MAX_VALUE);
            return new LeafFieldComparator() {
                @Override
                public void setBottom(int slot) throws IOException {
                    bottom = values[slot];
                }

                @Override
                public int compareBottom(int doc) {
                    return Integer.compare(bottom, docToRank.apply(doc));
                }

                @Override
                public int compareTop(int doc) {
                    return Integer.compare(topValue, docToRank.apply(doc));
                }

                @Override
                public void copy(int slot, int doc) {
                    values[slot] = docToRank.apply(doc);
                }

                @Override
                public void setScorer(Scorable scorer) {}
            };
        }
    }
}
