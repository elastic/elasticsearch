/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;

import java.io.IOException;
import java.util.List;

/**
 * Responsible for loading the _id from stored fields or for TSDB synthesizing the _id from the routing, _tsid and @timestamp fields.
 */
public sealed interface IdLoader permits IdLoader.LogsdbLoader, IdLoader.TsIdLoader, IdLoader.StoredIdLoader {

    /**
     * @return returns an {@link IdLoader} instance the loads the _id from stored field.
     */
    static IdLoader fromLeafStoredFieldLoader() {
        return new StoredIdLoader();
    }

    /**
     * @return returns an {@link IdLoader} instance that syn synthesizes _id from routing, _tsid and @timestamp fields.
     */
    static IdLoader createTsIdLoader(IndexRouting.ExtractFromSource indexRouting, List<String> routingPaths) {
        return new TsIdLoader(indexRouting, routingPaths);
    }

    /**
     * @return returns an {@link IdLoader} instance that loads _id from doc values
     */
    static IdLoader createDocValueIdLoader() {
        return new LogsdbLoader();
    }

    Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException;

    /**
     * Returns a leaf instance for a leaf reader that returns the _id for segment level doc ids.
     */
    sealed interface Leaf permits DocValueLeaf, StoredLeaf {

        /**
         * @param subDocId The segment level doc id for which the return the _id
         * @return the _id for the provided subDocId
         */
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
            IndexRouting.ExtractFromSource.Builder[] builders = null;
            if (indexRouting != null) {
                builders = new IndexRouting.ExtractFromSource.Builder[docIdsInLeaf.length];
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
                                BytesRef routingValue = dv.lookupOrd(dv.nextOrd());
                                builder.addMatching(routingField, routingValue);
                            }
                        }
                    }
                }
            }

            String[] ids = new String[docIdsInLeaf.length];
            // Each document always has exactly one tsid and one timestamp:
            SortedDocValues tsIdDocValues = DocValues.getSorted(reader, TimeSeriesIdFieldMapper.NAME);
            SortedNumericDocValues timestampDocValues = DocValues.getSortedNumeric(reader, DataStream.TIMESTAMP_FIELD_NAME);
            SortedDocValues routingHashDocValues = builders == null
                ? DocValues.getSorted(reader, TimeSeriesRoutingHashFieldMapper.NAME)
                : null;
            for (int i = 0; i < docIdsInLeaf.length; i++) {
                int docId = docIdsInLeaf[i];

                boolean found = tsIdDocValues.advanceExact(docId);
                assert found;
                BytesRef tsid = tsIdDocValues.lookupOrd(tsIdDocValues.ordValue());
                found = timestampDocValues.advanceExact(docId);
                assert found;
                assert timestampDocValues.docValueCount() == 1;
                long timestamp = timestampDocValues.nextValue();
                if (builders != null) {
                    var routingBuilder = builders[i];
                    ids[i] = TsidExtractingIdFieldMapper.createId(false, routingBuilder, tsid, timestamp, new byte[16]);
                } else {
                    found = routingHashDocValues.advanceExact(docId);
                    assert found;
                    BytesRef routingHashBytes = routingHashDocValues.lookupOrd(routingHashDocValues.ordValue());
                    int routingHash = TimeSeriesRoutingHashFieldMapper.decode(
                        Uid.decodeId(routingHashBytes.bytes, routingHashBytes.offset, routingHashBytes.length)
                    );
                    ids[i] = TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp);
                }
            }
            return new DocValueLeaf(docIdsInLeaf, ids);
        }
    }

    final class LogsdbLoader implements IdLoader {
        public IdLoader.Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException {
            String[] ids = new String[docIdsInLeaf.length];
            // Each document always has exactly one tsid and one timestamp:
            SortedDocValues idDocValues = DocValues.getSorted(reader, LogsdbIdFieldMapper.NAME);
            for (int i = 0; i < docIdsInLeaf.length; i++) {
                int docId = docIdsInLeaf[i];
                boolean found = idDocValues.advanceExact(docId);
                assert found;
                BytesRef id = idDocValues.lookupOrd(idDocValues.ordValue());
                ids[i] = Uid.decodeId(id.bytes, id.offset, id.length);
            }
            return new DocValueLeaf(docIdsInLeaf, ids);
        }
    }

    final class StoredIdLoader implements IdLoader {

        public StoredIdLoader() {}

        @Override
        public Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException {
            return new StoredLeaf(loader);
        }
    }

    final class DocValueLeaf implements Leaf {

        private final String[] ids;
        private final int[] docIdsInLeaf;

        private int idx = -1;

        DocValueLeaf(int[] docIdsInLeaf, String[] ids) {
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
