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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Keeps track of field-specific raw vector readers for vector reads
 */
public class GenericFlatVectorReaders {

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

    private final Map<FlatVectorsReaderKey, FlatVectorsReader> readers = new HashMap<>();
    private final Map<Integer, FlatVectorsReader> readersForFields = new HashMap<>();

    public void loadField(int fieldNumber, Field field, LoadFlatVectorsReader loadReader) throws IOException {
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

    public FlatVectorsReader getReaderForField(int fieldNumber) {
        FlatVectorsReader reader = readersForFields.get(fieldNumber);
        if (reader == null) {
            throw new IllegalArgumentException("Invalid field number [" + fieldNumber + "]");
        }
        return reader;
    }

    public Collection<FlatVectorsReader> allReaders() {
        return Collections.unmodifiableCollection(readers.values());
    }

    public GenericFlatVectorReaders getMergeInstance() throws IOException {
        GenericFlatVectorReaders mergeReaders = new GenericFlatVectorReaders();

        // link the original instance with the merge instance
        Map<FlatVectorsReader, FlatVectorsReader> mergeInstances = new IdentityHashMap<>();
        for (var reader : readers.entrySet()) {
            FlatVectorsReader mergeInstance = reader.getValue().getMergeInstance();
            mergeInstances.put(reader.getValue(), mergeInstance);
            mergeReaders.readers.put(reader.getKey(), mergeInstance);
        }
        // link up the fields to the merge readers
        for (var field : readersForFields.entrySet()) {
            mergeReaders.readersForFields.put(field.getKey(), mergeInstances.get(field.getValue()));
        }
        return mergeReaders;
    }
}
