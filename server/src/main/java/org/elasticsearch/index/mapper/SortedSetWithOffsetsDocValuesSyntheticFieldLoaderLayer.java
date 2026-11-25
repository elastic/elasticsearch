/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Load {@code _source} fields from {@link SortedSetDocValues} and associated {@link BinaryDocValues}. The former contains the unique values
 * in sorted order and the latter the offsets for each instance of the values. This allows synthesizing array elements in order as was
 * specified at index time. Note that this works only for leaf arrays.
 */
final class SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private final String offsetsFieldName;
    private final Function<BytesRef, BytesRef> converter;
    private ValuesWithOffsetsDocValuesLoader docValues;

    /**
     * @param name              The name of the field to synthesize
     * @param offsetsFieldName  The related offset field used to correctly synthesize the field if it is a leaf array
     */
    SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer(String name, String offsetsFieldName) {
        this(name, offsetsFieldName, Function.identity());
    }

    /**
     * @param name              The name of the field to synthesize
     * @param offsetsFieldName  The related offset field used to correctly synthesize the field if it is a leaf array
     * @param converter         This field value loader layer synthesizes the values read from doc values as utf8 string. If the doc value
     *                          values aren't serializable as utf8 string then it is the responsibility of the converter to covert into a
     *                          format that can be serialized as utf8 string. For example IP field mapper doc values can't directly be
     *                          serialized as utf8 string.
     */
    SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer(String name, String offsetsFieldName, Function<BytesRef, BytesRef> converter) {
        this.name = Objects.requireNonNull(name);
        this.offsetsFieldName = Objects.requireNonNull(offsetsFieldName);
        this.converter = Objects.requireNonNull(converter);
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        SortedSetDocValues valueDocValues = DocValues.getSortedSet(leafReader, name);
        return docValues = ValuesWithOffsetsDocValuesLoader.sortedSetLoader(
            valueDocValues,
            DocValues.getSorted(leafReader, offsetsFieldName),
            converter
        );
    }

    @Override
    public boolean hasValue() {
        if (docValues != null) {
            return docValues.count() > 0;
        } else {
            return false;
        }
    }

    @Override
    public long valueCount() {
        if (docValues != null) {
            return docValues.count();
        } else {
            return 0;
        }
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (docValues != null) {
            docValues.write(b);
        }
    }

}
