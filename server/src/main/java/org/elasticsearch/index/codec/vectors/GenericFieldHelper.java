/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GenericFieldHelper {

    public interface Field {
        String rawVectorFormatName();
        boolean useDirectIOReads();
    }

    @FunctionalInterface
    public interface LoadFlatVectorsReader {
        FlatVectorsReader getReader(String formatName, boolean useDirectIO) throws IOException;
    }

    private record FlatVectorsReaderKey(String formatName, boolean useDirectIO) {
        private FlatVectorsReaderKey(Field field) {
            this(field.rawVectorFormatName(), field.useDirectIOReads());
        }

        @Override
        public String toString() {
            return formatName + (useDirectIO ? " with Direct IO" : "");
        }
    }

    private final LoadFlatVectorsReader loadReader;
    private final Map<FlatVectorsReaderKey, FlatVectorsReader> readers = new HashMap<>();
    private final Map<Integer, FlatVectorsReader> readersForFields = new HashMap<>();

    public GenericFieldHelper(LoadFlatVectorsReader loadReader) {
        this.loadReader = loadReader;
    }

    public void addField(int fieldNumber, Field field) throws IOException {
        FlatVectorsReaderKey key = new FlatVectorsReaderKey(field);
        FlatVectorsReader reader = readers.get(key);
        if (reader == null) {
            reader = loadReader.getReader(field.rawVectorFormatName(), field.useDirectIOReads());
            if (reader == null) {
                throw new IllegalStateException("Cannot find flat vector format: " + field.rawVectorFormatName());
            }
            readers.put(key, reader);
        }
        readersForFields.put(fieldNumber, reader);
    }

    public Map<Integer, FlatVectorsReader> getReadersForFields() {
        return readersForFields;
    }
}
