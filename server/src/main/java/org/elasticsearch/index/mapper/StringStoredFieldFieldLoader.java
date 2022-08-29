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

public class StringStoredFieldFieldLoader
    implements
        SourceLoader.SyntheticFieldLoader,
        SourceLoader.SyntheticFieldLoader.StoredFieldLoader {
    private final String name;
    private final String simpleName;
    private List<Object> values;

    public StringStoredFieldFieldLoader(String name, String simpleName) {
        this.name = name;
        this.simpleName = simpleName;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return Stream.of(Map.entry(name, this));
    }

    @Override
    public void load(List<Object> values) {
        this.values = values;
    }

    @Override
    public boolean hasValue() {
        return values != null && values.isEmpty() == false;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (values == null || values.isEmpty()) {
            return;
        }
        if (values.size() == 1) {
            b.field(simpleName, values.get(0).toString());
            values = null;
            return;
        }
        b.startArray(simpleName);
        for (Object value : values) {
            b.value(value.toString());
        }
        b.endArray();
        values = null;
    }

    @Override
    public final DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        return null;
    }
}
