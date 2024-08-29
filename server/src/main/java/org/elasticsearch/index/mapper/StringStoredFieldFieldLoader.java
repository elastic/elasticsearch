/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

public abstract class StringStoredFieldFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final String name;
    private final String simpleName;

    private List<Object> values = emptyList();

    public StringStoredFieldFieldLoader(String name, String simpleName) {
        this.name = name;
        this.simpleName = simpleName;
    }

    @Override
    public final Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return Stream.of(Map.entry(name, new SourceLoader.SyntheticFieldLoader.StoredFieldLoader() {
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
    public final boolean hasValue() {
        return values.isEmpty() == false;
    }

    @Override
    public final void write(XContentBuilder b) throws IOException {
        switch (values.size()) {
            case 0:
                return;
            case 1:
                b.field(simpleName);
                write(b, values.get(0));
                return;
            default:
                b.startArray(simpleName);
                for (Object value : values) {
                    write(b, value);
                }
                b.endArray();
        }
    }

    protected abstract void write(XContentBuilder b, Object value) throws IOException;

    @Override
    public final DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        return null;
    }

    @Override
    public String fieldName() {
        return name;
    }
}
