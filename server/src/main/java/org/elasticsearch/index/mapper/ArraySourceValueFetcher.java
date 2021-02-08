/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.lookup.ValuesLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link ValueFetcher} that knows how to extract values
 * from the document source.
 *
 * This class differs from {@link SourceValueFetcher} in that it directly handles
 * array values in parsing. Field types should use this class if their corresponding
 * mapper returns true for {@link FieldMapper#parsesArrayValue()}.
 */
public abstract class ArraySourceValueFetcher implements ValueFetcher {
    private final Set<String> sourcePaths;
    private final @Nullable Object nullValue;

    public ArraySourceValueFetcher(Set<String> sourcePaths, Object nullValue) {
        this.sourcePaths = sourcePaths;
        this.nullValue = nullValue;
    }

    @Override
    public List<Object> fetchValues(ValuesLookup lookup) {
        List<Object> values = new ArrayList<>();
        for (String path : sourcePaths) {
            Object sourceValue = lookup.source().extractValue(path, nullValue);
            if (sourceValue == null) {
                return List.of();
            }
            values.addAll((List<?>) parseSourceValue(sourceValue));
        }
        return values;
    }

    /**
     * Given a value that has been extracted from a document's source, parse it into a standard
     * format. This parsing logic should closely mirror the value parsing in
     * {@link FieldMapper#parseCreateField} or {@link FieldMapper#parse}.
     */
    protected abstract Object parseSourceValue(Object value);
}
