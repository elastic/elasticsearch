/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final String fieldName;
    private final String fullFieldName;
    private final SyntheticFieldLoaderLayer[] parts;
    private boolean hasValue;

    public CompositeSyntheticFieldLoader(String fieldName, String fullFieldName, SyntheticFieldLoaderLayer... parts) {
        this.fieldName = fieldName;
        this.fullFieldName = fullFieldName;
        this.parts = parts;
        this.hasValue = false;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return Arrays.stream(parts).flatMap(SyntheticFieldLoaderLayer::storedFieldLoaders).map(e -> Map.entry(e.getKey(), values -> {
            hasValue = true;
            e.getValue().load(values);
        }));
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var loaders = new ArrayList<DocValuesLoader>(parts.length);
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

            this.hasValue |= hasDocs;
            return hasDocs;
        };
    }

    @Override
    public boolean hasValue() {
        return hasValue;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        var totalCount = Arrays.stream(parts).mapToLong(SyntheticFieldLoaderLayer::valueCount).sum();

        if (totalCount == 0) {
            return;
        }

        if (totalCount == 1) {
            b.field(fieldName);
            for (var part : parts) {
                part.write(b);
            }
            return;
        }

        b.startArray(fieldName);
        for (var part : parts) {
            part.write(b);
        }
        b.endArray();
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
    public interface SyntheticFieldLoaderLayer extends SourceLoader.SyntheticFieldLoader {
        /**
         * Number of values that this loader will write.
         * @return
         */
        long valueCount();
    }

    /**
     * Layer that loads malformed values stored in a dedicated field with a conventional name.
     * @see IgnoreMalformedStoredValues
     */
    public static class MalformedValuesLayer implements SyntheticFieldLoaderLayer {
        private final String fieldName;
        private List<Object> values;

        public MalformedValuesLayer(String fieldName) {
            this.fieldName = IgnoreMalformedStoredValues.name(fieldName);
            this.values = emptyList();
        }

        @Override
        public long valueCount() {
            return values.size();
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of(Map.entry(fieldName, values -> this.values = values));
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
                if (v instanceof BytesRef r) {
                    XContentDataHelper.decodeAndWrite(b, r);
                } else {
                    b.value(v);
                }
            }
            values = emptyList();
        }

        @Override
        public String fieldName() {
            return fieldName;
        }
    }
}
