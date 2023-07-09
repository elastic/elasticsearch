/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;

import java.io.IOException;
import java.util.List;

public sealed interface IdLoader permits IdLoader.TsIdLoader, IdLoader.StoredIdLoader {

    static IdLoader fromLeafStoredFieldLoader() {
        return new StoredIdLoader();
    }

    static IdLoader createTsIdLoader(IndexRouting.ExtractFromSource indexRouting, List<String> routingPaths) {
        return new TsIdLoader(indexRouting, routingPaths);
    }

    Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException;

    sealed interface Leaf permits StoredLeaf, TsIdLeaf {

        String getId(int subDocId);

    }

    final class TsIdLoader implements IdLoader {

        private final IndexRouting.ExtractFromSource indexRouting;
        private final List<String> routingPaths;

        TsIdLoader(IndexRouting.ExtractFromSource indexRouting, List<String> routingPaths) {
            this.routingPaths = routingPaths;
            this.indexRouting = indexRouting;
        }

        public IdLoader.Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException {
            IndexRouting.ExtractFromSource.Builder[] builders = new IndexRouting.ExtractFromSource.Builder[docIdsInLeaf.length];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = indexRouting.builder();
            }

            for (String routingField : routingPaths) {
                // Routing field must always be keyword fields, so it is ok to use SortedSetDocValues directly here.
                SortedSetDocValues dv = DocValues.getSortedSet(reader, routingField);
                for (int i = 0; i < docIdsInLeaf.length; i++) {
                    int docId = docIdsInLeaf[i];
                    var builder = builders[i];
                    if (dv.advanceExact(docId)) {
                        for (int j = 0; j < dv.docValueCount(); j++) {
                            builder.addMatching(routingField, dv.lookupOrd(dv.nextOrd()));
                        }
                    }
                }
            }

            String[] ids = new String[docIdsInLeaf.length];
            // Each document always has exactly one tsid and one timestamp:
            SortedDocValues tsIdDocValues = DocValues.getSorted(reader, TimeSeriesIdFieldMapper.NAME);
            SortedNumericDocValues timestampDocValues = DocValues.getSortedNumeric(reader, DataStream.TIMESTAMP_FIELD_NAME);
            for (int i = 0; i < docIdsInLeaf.length; i++) {
                int docId = docIdsInLeaf[i];
                var routingBuilder = builders[i];

                tsIdDocValues.advanceExact(docId);
                assert tsIdDocValues.getValueCount() == 1;
                BytesRef tsid = tsIdDocValues.lookupOrd(tsIdDocValues.ordValue());
                timestampDocValues.advanceExact(docId);
                assert timestampDocValues.docValueCount() == 1;
                long timestamp = timestampDocValues.nextValue();

                ids[i] = TsidExtractingIdFieldMapper.createId(false, routingBuilder, tsid, timestamp, new byte[16]);
            }
            return new TsIdLeaf(docIdsInLeaf, ids);
        }
    }

    final class StoredIdLoader implements IdLoader {

        public StoredIdLoader() {
        }

        @Override
        public Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException {
            return new StoredLeaf(loader);
        }
    }

    final class TsIdLeaf implements Leaf {

        private final String[] ids;
        private final int[] docIdsInLeaf;

        private int idx = -1;

        TsIdLeaf(int[] docIdsInLeaf, String[] ids) {
            this.ids = ids;
            this.docIdsInLeaf = docIdsInLeaf;
        }

        public String getId(int subDocId) {
            idx++;
            if (docIdsInLeaf[idx] != subDocId) {
                throw new IllegalArgumentException(
                    "expected to be called with [" + docIdsInLeaf[idx] + "] but was called with " + subDocId + " instead"
                );
            }
            return ids[idx];
        }
    }

    final class StoredLeaf implements Leaf {

        private final LeafStoredFieldLoader loader;

        StoredLeaf(LeafStoredFieldLoader loader) {
            this.loader = loader;
        }

        @Override
        public String getId(int subDocId) {
            return loader.id();
        }
    }

}
