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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.NumericDvSingletonOrSorted;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.SortedDvSingletonOrSet;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

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

    BlockLoader blockLoader(ByteSizeValue ordinalsByteSize);

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
        public BlockLoader blockLoader(ByteSizeValue ordinalsByteSize) {
            return new BlockDocValuesReader.DocValuesBlockLoader() {
                @Override
                public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
                    if (indexRouting != null) {
                        return new LegacyTsIdFieldReader(breaker, ordinalsByteSize, context, indexRouting, routingPaths);
                    } else {
                        return new TsIdFieldReader(breaker, ordinalsByteSize, context, useSyntheticId);
                    }
                }

                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.bytesRefs(expectedCount);
                }
            };
        }

        private static class TsIdFieldReader extends BlockDocValuesReader {
            final TrackingSortedDocValues tsidDVs;
            final TrackingSortedNumericDocValues timestampDVs;
            final TrackingSortedDocValues routingHashDVs;
            final boolean useSyntheticId;

            TsIdFieldReader(CircuitBreaker breaker, ByteSizeValue ordinalsByteSize, LeafReaderContext ctx, boolean useSyntheticId)
                throws IOException {
                super(null);
                boolean success = false;
                TrackingSortedDocValues tsidDVs = null;
                TrackingSortedNumericDocValues timestampDVs = null;
                TrackingSortedDocValues routingHashDVs = null;
                try {
                    tsidDVs = SortedDvSingletonOrSet.get(breaker, ordinalsByteSize, ctx, TimeSeriesIdFieldMapper.NAME).forceSingle();
                    timestampDVs = NumericDvSingletonOrSorted.get(breaker, ctx, DataStream.TIMESTAMP_FIELD_NAME).forceSorted();
                    routingHashDVs = SortedDvSingletonOrSet.get(breaker, ordinalsByteSize, ctx, TimeSeriesRoutingHashFieldMapper.NAME)
                        .forceSingle();
                    success = true;
                } finally {
                    if (success == false) {
                        Releasables.close(tsidDVs, timestampDVs, routingHashDVs);
                    }
                }
                this.tsidDVs = tsidDVs;
                this.timestampDVs = timestampDVs;
                this.routingHashDVs = routingHashDVs;
                this.useSyntheticId = useSyntheticId;
            }

            @Override
            protected int docId() {
                return tsidDVs.docValues().docID();
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
                        read(docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(int docId, BlockLoader.BytesRefBuilder builder) throws IOException {
                if (tsidDVs.docValues().advanceExact(docId) == false
                    || timestampDVs.docValues().advanceExact(docId) == false
                    || routingHashDVs.docValues().advanceExact(docId) == false) {
                    assert false : "_tsid or @timestamp or _ts_routing_hash missing for docId " + docId;
                    throw new IllegalStateException("_tsid or @timestamp or _ts_routing_hash missing for docId " + docId);
                }
                BytesRef tsid = tsidDVs.docValues().lookupOrd(tsidDVs.docValues().ordValue());
                long timestamp = timestampDVs.docValues().nextValue();
                BytesRef routingHashBytes = routingHashDVs.docValues().lookupOrd(routingHashDVs.docValues().ordValue());
                int routingHash = TimeSeriesRoutingHashFieldMapper.decode(
                    Uid.decodeId(routingHashBytes.bytes, routingHashBytes.offset, routingHashBytes.length)
                );
                final String id;
                if (useSyntheticId) {
                    id = TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp, routingHash);
                } else {
                    id = TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp);
                }
                builder.appendBytesRef(new BytesRef(id));
            }

            @Override
            public void close() {
                Releasables.close(tsidDVs, timestampDVs, routingHashDVs);
            }
        }

        private static class LegacyTsIdFieldReader extends BlockDocValuesReader {
            final RoutingHashBuilder routingBuilder;
            final TrackingSortedDocValues tsidDVs;
            final TrackingSortedNumericDocValues timestampDVs;
            final TrackingSortedDocValues[] routingHashDVs;
            final List<String> routingPaths;
            final byte[] scratch = new byte[16];

            LegacyTsIdFieldReader(
                CircuitBreaker breaker,
                ByteSizeValue ordinalsByteSize,
                LeafReaderContext ctx,
                IndexRouting.ExtractFromSource.ForRoutingPath indexRouting,
                List<String> routingPaths
            ) throws IOException {
                super(null);
                boolean success = false;
                TrackingSortedDocValues tsidDVs = null;
                TrackingSortedNumericDocValues timestampDVs = null;
                TrackingSortedDocValues[] routingHashDVs = new TrackingSortedDocValues[routingPaths.size()];
                try {
                    tsidDVs = SortedDvSingletonOrSet.get(breaker, ordinalsByteSize, ctx, TimeSeriesIdFieldMapper.NAME).forceSingle();
                    timestampDVs = NumericDvSingletonOrSorted.get(breaker, ctx, DataStream.TIMESTAMP_FIELD_NAME).forceSorted();
                    for (int i = 0; i < routingPaths.size(); i++) {
                        routingHashDVs[i] = SortedDvSingletonOrSet.get(breaker, ordinalsByteSize, ctx, routingPaths.get(i)).forceSingle();
                    }
                } finally {
                    if (success == false) {
                        Releasables.close(tsidDVs, timestampDVs, Releasables.wrap(routingHashDVs));
                    }
                }
                this.routingBuilder = indexRouting.builder();
                this.routingPaths = routingPaths;
                this.tsidDVs = tsidDVs;
                this.timestampDVs = timestampDVs;
                this.routingHashDVs = routingHashDVs;
            }

            @Override
            protected int docId() {
                return tsidDVs.docValues().docID();
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
                        read(docs.get(i), builder);
                    }
                    return builder.build();
                }
            }

            private void read(int docId, BlockLoader.BytesRefBuilder builder) throws IOException {
                if (tsidDVs.docValues().advanceExact(docId) == false || timestampDVs.docValues().advanceExact(docId) == false) {
                    assert false : "_tsid or @timestamp missing for docId " + docId;
                    throw new IllegalStateException("_tsid or @timestamp missing for docId " + docId);
                }
                routingBuilder.clear();
                BytesRef tsid = tsidDVs.docValues().lookupOrd(tsidDVs.docValues().ordValue());
                long timestamp = timestampDVs.docValues().nextValue();
                for (int i = 0; i < routingHashDVs.length; i++) {
                    SortedDocValues dv = routingHashDVs[i].docValues();
                    if (dv.advanceExact(docId)) {
                        BytesRef v = dv.lookupOrd(dv.ordValue());
                        routingBuilder.addMatching(routingPaths.get(i), v);
                    }
                }
                var id = TsidExtractingIdFieldMapper.createId(false, routingBuilder, tsid, timestamp, scratch);
                builder.appendBytesRef(new BytesRef(id));
            }

            @Override
            public void close() {
                Releasables.close(tsidDVs, timestampDVs, Releasables.wrap(routingHashDVs));
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
        public BlockLoader blockLoader(ByteSizeValue ordinalsByteSize) {
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
