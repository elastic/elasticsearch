/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KEYED_FIELD_SUFFIX;
import static org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KEYED_IGNORED_VALUES_FIELD_SUFFIX;

/**
 * Block loader for {@link FlattenedFieldMapper.RootFlattenedFieldType} that loads values from doc values, falling back to stored fields
 * when necessary to load ignored values.
 */
final class RootFlattenedDocValuesBlockLoader implements BlockLoader {
    private final Mapper.IgnoreAbove ignoreAbove;

    private final BlockFlattenedDocValuesSyntheticFieldLoader fieldLoader;
    private final List<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders;

    RootFlattenedDocValuesBlockLoader(String name, Mapper.IgnoreAbove ignoreAbove, boolean usesBinaryDocValues) {
        this.ignoreAbove = ignoreAbove;
        this.fieldLoader = new BlockFlattenedDocValuesSyntheticFieldLoader(
            name,
            name + KEYED_FIELD_SUFFIX,
            ignoreAbove.valuesPotentiallyIgnored() ? name + KEYED_IGNORED_VALUES_FIELD_SUFFIX : null,
            null,
            usesBinaryDocValues
        );
        this.storedFieldLoaders = fieldLoader.storedFieldLoaders().toList();

    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        if (ignoreAbove.valuesPotentiallyIgnored()) {
            return new StoredFieldsSpec(false, false, fieldLoader.storedFieldLoaders().map(Map.Entry::getKey).collect(Collectors.toSet()));
        } else {
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        return null;
    }

    public AllReader reader(LeafReaderContext context) throws IOException {
        var reader = fieldLoader.docValuesLoader(context.reader(), null);
        var trackingReader = reader != null ? new TrackingLoader(reader) : null;

        return new AllReader() {
            private final Thread creationThread = Thread.currentThread();

            @Override
            public boolean canReuse(int startingDocID) {
                // Logic emulated from BlockDocValuesReader#canReuse
                return creationThread == Thread.currentThread() && trackingReader.getCurDocId() <= startingDocID;
            }

            @Override
            public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                for (var loaderEntry : storedFieldLoaders) {
                    var values = storedFields.storedFields().get(loaderEntry.getKey());
                    if (values != null) {
                        loaderEntry.getValue().load(values);
                    }
                }
                read(docId, (BytesRefBuilder) builder);
            }

            @Override
            public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        int docId = docs.get(i);
                        read(docId, builder);
                    }
                    return builder.build();
                }
            }

            public void read(int docId, BytesRefBuilder builder) throws IOException {
                if (trackingReader != null) {
                    trackingReader.advanceToDoc(docId);
                }
                fieldLoader.writeToBlock(builder);
            }

            @Override
            public String toString() {
                return getClass().getSimpleName();
            }
        };
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public IOSupplier<ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        // stored fields aren't supported when reading column-at-a-time
        if (ignoreAbove.valuesPotentiallyIgnored()) {
            return null;
        }

        return () -> reader(context);
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        return reader(context);
    }

    /**
     * A DocValuesLoader that tracks the last advanced docId.
     */
    private static class TrackingLoader implements SourceLoader.SyntheticFieldLoader.DocValuesLoader {
        private final SourceLoader.SyntheticFieldLoader.DocValuesLoader loader;
        private int curDocId = -1;

        TrackingLoader(SourceLoader.SyntheticFieldLoader.DocValuesLoader loader) {
            this.loader = loader;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            curDocId = docId;
            return loader.advanceToDoc(docId);
        }

        public int getCurDocId() {
            return curDocId;
        }
    }

    /**
     * Subclass of {@link FlattenedDocValuesSyntheticFieldLoader} to write the synthetic source to a block loader
     * instead of to the "_source" field.
     */
    private static class BlockFlattenedDocValuesSyntheticFieldLoader extends FlattenedDocValuesSyntheticFieldLoader {
        BlockFlattenedDocValuesSyntheticFieldLoader(
            String fieldFullPath,
            String keyedFieldFullPath,
            String keyedIgnoredValuesFieldFullPath,
            String leafName,
            boolean usesBinaryDocValues
        ) {
            super(fieldFullPath, keyedFieldFullPath, keyedIgnoredValuesFieldFullPath, leafName, usesBinaryDocValues);
        }

        public void writeToBlock(BlockLoader.BytesRefBuilder builder) throws IOException {
            if (docValues.count() == 0 && ignoredValues.isEmpty()) {
                builder.appendNull();
                return;
            }

            var writer = getWriter();

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            jsonBuilder.startObject();
            writer.write(jsonBuilder);
            jsonBuilder.endObject();

            builder.appendBytesRef(BytesReference.bytes(jsonBuilder).toBytesRef());
        }
    }
}
