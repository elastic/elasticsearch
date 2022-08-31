/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.core.Nullable;
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

    @Nullable
    private final String extraStoredName;
    private List<Object> extraValues = emptyList();

    public StringStoredFieldFieldLoader(String name, String simpleName, @Nullable String extraStoredName) {
        this.name = name;
        this.simpleName = simpleName;
        this.extraStoredName = extraStoredName;
    }

    @Override
    public final Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        Stream<Map.Entry<String, StoredFieldLoader>> standard = Stream.of(Map.entry(name, values -> this.values = values));
        if (extraStoredName == null) {
            return standard;
        }
        return Stream.concat(standard, Stream.of(Map.entry(extraStoredName, values -> this.extraValues = values)));
    }

    @Override
    public final boolean hasValue() {
        return values.isEmpty() == false || extraValues.isEmpty() == false;
    }

    @Override
    public final void write(XContentBuilder b) throws IOException {
        int size = values.size() + extraValues.size();
        switch (size) {
            case 0:
                return;
            case 1:
                b.field(simpleName);
                if (values.size() > 0) {
                    assert values.size() == 1;
                    assert extraValues.isEmpty();
                    write(b, values.get(0));
                } else {
                    assert values.isEmpty();
                    assert extraValues.size() == 1;
                    write(b, extraValues.get(0));
                }
                values = emptyList();
                extraValues = emptyList();
                return;
            default:
                b.startArray(simpleName);
                for (Object value : values) {
                    write(b, value);
                }
                for (Object value : extraValues) {
                    write(b, value);
                }
                b.endArray();
                values = emptyList();
                extraValues = emptyList();
                return;
        }
    }

    protected abstract void write(XContentBuilder b, Object value) throws IOException;

    @Override
    public final DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        return null;
    }
}
