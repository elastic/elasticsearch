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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingHashBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Responsible for loading the _id from stored fields or for TSDB synthesizing the _id from the routing, _tsid and @timestamp fields.
 */
public sealed interface IdLoader permits IdLoader.TsIdLoader, IdLoader.StoredIdLoader {

    /**
     * @return returns an {@link IdLoader} instance to load the value of the _id field.
     */
    static IdLoader create(IndexSettings indexSettings, MappingLookup mappingLookup) {
        if (indexSettings.getMode() == IndexMode.TIME_SERIES) {
            IndexRouting.ExtractFromSource.ForRoutingPath indexRouting = null;
            List<String> routingPaths = null;
            if (indexSettings.getIndexVersionCreated().before(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)) {
                indexRouting = (IndexRouting.ExtractFromSource.ForRoutingPath) indexSettings.getIndexRouting();
                routingPaths = indexSettings.getIndexMetadata().getRoutingPaths();
                for (String routingField : routingPaths) {
                    if (routingField.contains("*")) {
                        // In case the routing fields include path matches, find any matches and add them as distinct fields
                        // to the routing path.
                        Set<String> matchingRoutingPaths = new TreeSet<>(routingPaths);
                        for (Mapper mapper : mappingLookup.fieldMappers()) {
                            if (mapper instanceof KeywordFieldMapper && indexRouting.matchesField(mapper.fullPath())) {
                                matchingRoutingPaths.add(mapper.fullPath());
                            }
                        }
                        routingPaths = new ArrayList<>(matchingRoutingPaths);
                        break;
                    }
                }
            }
            return createTsIdLoader(indexRouting, routingPaths, indexSettings.useTimeSeriesSyntheticId());
        } else {
            return fromLeafStoredFieldLoader();
        }
    }

    /**
     * @return returns an {@link IdLoader} instance the loads the _id from stored field.
     */
    static IdLoader fromLeafStoredFieldLoader() {
        return new StoredIdLoader();
    }

    /**
     * @return returns an {@link IdLoader} instance that syn synthesizes _id from routing, _tsid and @timestamp fields.
     */
    static IdLoader createTsIdLoader(
        IndexRouting.ExtractFromSource.ForRoutingPath indexRouting,
        List<String> routingPaths,
        boolean useSyntheticId
    ) {
        return new TsIdLoader(indexRouting, routingPaths, useSyntheticId);
    }

    Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException;

    BlockLoader blockLoader(long ordinalsByteSize);

    /**
     * Returns a leaf instance for a leaf reader that returns the _id for segment level doc ids.
     */
    sealed interface Leaf permits StoredLeaf, TsIdLeaf {

        /**
         * @param subDocId The segment level doc id for which the return the _id
         * @return the _id for the provided subDocId
         */
        String getId(int subDocId);

    }

    final class TsIdLoader implements IdLoader {

        private final IndexRouting.ExtractFromSource.ForRoutingPath indexRouting;
        private final List<String> routingPaths;
        private final boolean useSyntheticId;

        TsIdLoader(IndexRouting.ExtractFromSource.ForRoutingPath indexRouting, List<String> routingPaths, boolean useSyntheticId) {
            this.routingPaths = routingPaths;
            this.indexRouting = indexRouting;
            this.useSyntheticId = useSyntheticId;
        }

        public IdLoader.Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException {
            RoutingHashBuilder[] builders = null;
            if (indexRouting != null) {
                // this branch is for legacy indices before IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID
                builders = new RoutingHashBuilder[docIdsInLeaf.length];
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
                    if (useSyntheticId) {
                        ids[i] = TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp, routingHash);
                    } else {
                        ids[i] = TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp);
                    }
                }
            }
            return new TsIdLeaf(docIdsInLeaf, ids);
        }

        @Override
        public BlockLoader blockLoader(long ordinalsByteSize) {
            return new BlockDocValuesReader.DocValuesBlockLoader() {
                @Override
                public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
                    if (indexRouting != null) {
                        return new LegacyTsIdFieldReader(breaker, ordinalsByteSize, context.reader(), indexRouting, routingPaths);
                    } else {
                        return new TsIdFieldReader(breaker, ordinalsByteSize, context.reader(), useSyntheticId);
                    }
                }

                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.bytesRefs(expectedCount);
                }
            };
        }

        private static class TsIdFieldReader extends BlockDocValuesReader {
            final long ordinalsByteSize;
            final SortedDocValues tsidDVs;
            final SortedNumericDocValues timestampDVs;
            final SortedDocValues routingHashDVs;
            final boolean useSyntheticId;

            TsIdFieldReader(CircuitBreaker breaker, long ordinalsByteSize, LeafReader leafReader, boolean useSyntheticId)
                throws IOException {
                super(breaker);
                this.ordinalsByteSize = ordinalsByteSize;
                breaker.addWithoutBreaking(estimatedSize(ordinalsByteSize));
                this.tsidDVs = DocValues.getSorted(leafReader, TimeSeriesIdFieldMapper.NAME);
                this.timestampDVs = DocValues.getSortedNumeric(leafReader, DataStream.TIMESTAMP_FIELD_NAME);
                this.routingHashDVs = DocValues.getSorted(leafReader, TimeSeriesRoutingHashFieldMapper.NAME);
                this.useSyntheticId = useSyntheticId;
            }

            @Override
            protected int docId() {
                return tsidDVs.docID();
            }

            @Override
            public String toString() {
                return "TsIdFieldReader";
            }

            @Override
            public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
                throws IOException {
                try (var builder = factory.bytesRefs(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        read(docs.get(i), null, builder);
                    }
                    return builder.build();
                }
            }

            @Override
            public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
                if (tsidDVs.advanceExact(docId) == false
                    || timestampDVs.advanceExact(docId) == false
                    || routingHashDVs.advanceExact(docId) == false) {
                    assert false : "_tsid or @timestamp or _ts_routing_hash missing for docId " + docId;
                    throw new IllegalStateException("_tsid or @timestamp or _ts_routing_hash missing for docId " + docId);
                }
                BytesRef tsid = tsidDVs.lookupOrd(tsidDVs.ordValue());
                long timestamp = timestampDVs.nextValue();
                BytesRef routingHashBytes = routingHashDVs.lookupOrd(routingHashDVs.ordValue());
                int routingHash = TimeSeriesRoutingHashFieldMapper.decode(
                    Uid.decodeId(routingHashBytes.bytes, routingHashBytes.offset, routingHashBytes.length)
                );
                final String id;
                if (useSyntheticId) {
                    id = TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp, routingHash);
                } else {
                    id = TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp);
                }
                ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(new BytesRef(id));
            }

            @Override
            public void close() {
                breaker.addWithoutBreaking(-estimatedSize(ordinalsByteSize));
            }

            private static long estimatedSize(long ordinalsByteSize) {
                return TrackingNumericDocValues.ESTIMATED_SIZE + ordinalsByteSize + TrackingNumericDocValues.ESTIMATED_SIZE;
            }

        }

        private static class LegacyTsIdFieldReader extends BlockDocValuesReader {
            final long ordinalsByteSize;
            final RoutingHashBuilder routingBuilder;
            final SortedDocValues tsidDVs;
            final SortedNumericDocValues timestampDVs;
            final SortedDocValues[] routingHashDVs;
            final List<String> routingPaths;
            final byte[] scratch = new byte[16];

            LegacyTsIdFieldReader(
                CircuitBreaker breaker,
                long ordinalsByteSize,
                LeafReader leafReader,
                IndexRouting.ExtractFromSource.ForRoutingPath indexRouting,
                List<String> routingPaths
            ) throws IOException {
                super(breaker);
                this.ordinalsByteSize = ordinalsByteSize;
                breaker.addEstimateBytesAndMaybeBreak(estimatedSize(ordinalsByteSize, routingPaths.size()), "load blocks");
                this.routingBuilder = indexRouting.builder();
                this.routingPaths = routingPaths;
                this.routingHashDVs = new SortedDocValues[routingPaths.size()];
                for (int i = 0; i < routingPaths.size(); i++) {
                    this.routingHashDVs[i] = DocValues.getSorted(leafReader, routingPaths.get(i));
                }
                this.tsidDVs = DocValues.getSorted(leafReader, TimeSeriesIdFieldMapper.NAME);
                this.timestampDVs = DocValues.getSortedNumeric(leafReader, DataStream.TIMESTAMP_FIELD_NAME);
            }

            @Override
            protected int docId() {
                return tsidDVs.docID();
            }

            @Override
            public String toString() {
                return "LegacyTsIdFieldReader";
            }

            @Override
            public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
                throws IOException {
                try (var builder = factory.bytesRefs(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        read(docs.get(i), null, builder);
                    }
                    return builder.build();
                }
            }

            @Override
            public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
                if (tsidDVs.advanceExact(docId) == false || timestampDVs.advanceExact(docId) == false) {
                    assert false : "_tsid or @timestamp missing for docId " + docId;
                    throw new IllegalStateException("_tsid or @timestamp missing for docId " + docId);
                }
                routingBuilder.clear();
                BytesRef tsid = tsidDVs.lookupOrd(tsidDVs.ordValue());
                long timestamp = timestampDVs.nextValue();
                for (int i = 0; i < routingHashDVs.length; i++) {
                    SortedDocValues dv = routingHashDVs[i];
                    if (dv.advanceExact(docId)) {
                        BytesRef v = dv.lookupOrd(dv.ordValue());
                        routingBuilder.addMatching(routingPaths.get(i), v);
                    }
                }
                var id = TsidExtractingIdFieldMapper.createId(false, routingBuilder, tsid, timestamp, scratch);
                ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(new BytesRef(id));
            }

            @Override
            public void close() {
                breaker.addWithoutBreaking(-estimatedSize(ordinalsByteSize, routingPaths.size()));
            }

            private static long estimatedSize(long ordinalsByteSize, int routingPathCount) {
                return routingPathCount * ordinalsByteSize + ordinalsByteSize + TrackingNumericDocValues.ESTIMATED_SIZE;
            }
        }
    }

    final class StoredIdLoader implements IdLoader {
        public StoredIdLoader() {

        }

        @Override
        public Leaf leaf(LeafStoredFieldLoader loader, LeafReader reader, int[] docIdsInLeaf) throws IOException {
            return new StoredLeaf(loader);
        }

        @Override
        public BlockLoader blockLoader(long ordinalsByteSize) {
            return new BlockStoredFieldsReader.IdBlockLoader();
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
