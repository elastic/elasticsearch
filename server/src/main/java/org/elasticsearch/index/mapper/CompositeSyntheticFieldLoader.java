/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * A {@link SourceLoader.SyntheticFieldLoader} that uses a set of sub-loaders
 * to produce synthetic source for the field.
 * Typical use case is to gather field values from doc_values and append malformed values
 * stored in a different field in case of ignore_malformed being enabled.
 */
public class CompositeSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final String leafFieldName;
    private final String fullFieldName;
    private final Collection<Layer> parts;
    private boolean storedFieldLoadersHaveValues;
    private boolean docValuesLoadersHaveValues;

    public CompositeSyntheticFieldLoader(String leafFieldName, String fullFieldName, Layer... parts) {
        this(leafFieldName, fullFieldName, Arrays.asList(parts));
    }

    public CompositeSyntheticFieldLoader(String leafFieldName, String fullFieldName, Collection<Layer> parts) {
        this.leafFieldName = leafFieldName;
        this.fullFieldName = fullFieldName;
        this.parts = parts;
        this.storedFieldLoadersHaveValues = false;
        this.docValuesLoadersHaveValues = false;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return parts.stream().flatMap(Layer::storedFieldLoaders).map(e -> Map.entry(e.getKey(), new StoredFieldLoader() {
            @Override
            public void load(List<Object> newValues) {
                storedFieldLoadersHaveValues = true;
                e.getValue().load(newValues);
            }
        }));
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var loaders = new ArrayList<DocValuesLoader>(parts.size());
        for (var part : parts) {
            var partLoader = part.docValuesLoader(leafReader, docIdsInLeaf);
            if (partLoader != null) {
                loaders.add(partLoader);
            }
        }

        if (loaders.isEmpty()) {
            return null;
        }

        return docId -> {
            boolean hasDocs = false;
            for (var loader : loaders) {
                hasDocs |= loader.advanceToDoc(docId);
            }

            this.docValuesLoadersHaveValues = hasDocs;
            return hasDocs;
        };
    }

    @Override
    public boolean hasValue() {
        return storedFieldLoadersHaveValues || docValuesLoadersHaveValues;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        long totalCount = 0;
        for (Layer part : parts) {
            totalCount += part.valueCount();
        }

        if (totalCount == 0) {
            return;
        }

        if (totalCount == 1) {
            b.field(leafFieldName);
            for (var part : parts) {
                part.write(b);
            }
            return;
        }

        b.startArray(leafFieldName);
        for (var part : parts) {
            part.write(b);
        }
        b.endArray();
        softReset();
    }

    private void softReset() {
        storedFieldLoadersHaveValues = false;
        docValuesLoadersHaveValues = false;
    }

    @Override
    public void reset() {
        softReset();
        parts.forEach(SourceLoader.SyntheticFieldLoader::reset);
    }

    @Override
    public String fieldName() {
        return this.fullFieldName;
    }

    /**
     * Represents one layer of loading synthetic source values for a field
     * as a part of {@link CompositeSyntheticFieldLoader}.
     * <br>
     * Note that the contract of {@link SourceLoader.SyntheticFieldLoader#write(XContentBuilder)}
     * is slightly different here since it only needs to write field values without encompassing object or array.
     */
    public interface Layer extends SourceLoader.SyntheticFieldLoader {
        /**
         * Number of values that this loader will write for a given document.
         * @return
         */
        long valueCount();
    }

    public interface DocValuesLayer extends Layer {
        @Override
        default Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.empty();
        }

        @Override
        default void reset() {
            // Not applicable to loaders using only doc values
            // since DocValuesLoader#advanceToDoc will reset the state anyway.
        }
    }

    /**
     * Layer that loads malformed values stored in a dedicated field with a conventional name.
     * @see IgnoreMalformedStoredValues
     */
    public static class MalformedValuesLayer extends StoredFieldLayer {
        public MalformedValuesLayer(String fieldName) {
            super(IgnoreMalformedStoredValues.name(fieldName));
        }

        @Override
        protected void writeValue(Object value, XContentBuilder b) throws IOException {
            if (value instanceof BytesRef r) {
                XContentDataHelper.decodeAndWrite(b, r);
            } else {
                b.value(value);
            }
        }
    }

    /**
     * Layer that loads field values from a provided stored field.
     */
    public abstract static class StoredFieldLayer implements Layer {
        private final String fieldName;
        private List<Object> values;

        public StoredFieldLayer(String fieldName) {
            this.fieldName = fieldName;
            this.values = emptyList();
        }

        @Override
        public long valueCount() {
            return values.size();
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of(Map.entry(fieldName, newValues -> values = newValues));
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            return null;
        }

        @Override
        public boolean hasValue() {
            return values.isEmpty() == false;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            for (Object v : values) {
                writeValue(v, b);
            }
            reset();
        }

        @Override
        public void reset() {
            values = emptyList();
        }

        /**
         * Write a value read from stored field using appropriate format.
         */
        protected abstract void writeValue(Object value, XContentBuilder b) throws IOException;

        @Override
        public String fieldName() {
            return fieldName;
        }
    }
}
