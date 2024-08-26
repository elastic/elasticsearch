/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * Saves malformed values to stored fields so they can be loaded for synthetic
 * {@code _source}.
 */
public abstract class IgnoreMalformedStoredValues {
    /**
     * Creates a stored field that stores malformed data to be used in synthetic source.
     * Name of the stored field is original name of the field with added conventional suffix.
     * @param name original name of the field
     * @param parser parser to grab field content from
     * @return
     * @throws IOException
     */
    public static StoredField storedField(String name, XContentParser parser) throws IOException {
        return XContentDataHelper.storedField(name(name), parser);
    }

    /**
     * Creates a stored field that stores malformed data to be used in synthetic source.
     * Name of the stored field is original name of the field with added conventional suffix.
     * @param name original name of the field
     * @param builder malformed data
     * @return
     * @throws IOException
     */
    public static StoredField storedField(String name, XContentBuilder builder) throws IOException {
        return XContentDataHelper.storedField(name(name), builder);
    }

    /**
     * Build a {@link IgnoreMalformedStoredValues} that never contains any values.
     */
    public static IgnoreMalformedStoredValues empty() {
        return EMPTY;
    }

    /**
     * Build a {@link IgnoreMalformedStoredValues} that loads from stored fields.
     */
    public static IgnoreMalformedStoredValues stored(String fieldName) {
        return new Stored(fieldName);
    }

    /**
     * A {@link Stream} mapping stored field paths to a place to put them
     * so they can be included in the next document.
     */
    public abstract Stream<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders();

    /**
     * How many values has this field loaded for this document?
     */
    public abstract int count();

    /**
     * Write values for this document.
     */
    public abstract void write(XContentBuilder b) throws IOException;

    private static final Empty EMPTY = new Empty();

    private static class Empty extends IgnoreMalformedStoredValues {
        @Override
        public Stream<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders() {
            return Stream.empty();
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {}
    }

    private static class Stored extends IgnoreMalformedStoredValues {
        private final String fieldName;

        private List<Object> values = emptyList();

        Stored(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Stream<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of(Map.entry(name(fieldName), new SourceLoader.SyntheticFieldLoader.StoredFieldLoader() {
                @Override
                public void advanceToDoc(int docId) {
                    values = emptyList();
                }

                @Override
                public void load(List<Object> newValues) {
                    values = newValues;
                }
            }));
        }

        @Override
        public int count() {
            return values.size();
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
    }

    public static String name(String fieldName) {
        return fieldName + "._ignore_malformed";
    }
}
