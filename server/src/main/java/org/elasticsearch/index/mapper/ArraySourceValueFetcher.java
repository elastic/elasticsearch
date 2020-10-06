/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.lookup.SourceLookup;

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

    public ArraySourceValueFetcher(String fieldName, MapperService mapperService) {
        this(fieldName, mapperService, null);
    }

    /**
     * @param fieldName The name of the field.
     * @param mapperService A mapper service.
     * @param nullValue A optional substitute value if the _source value is 'null'.
     */
    public ArraySourceValueFetcher(String fieldName, MapperService mapperService, Object nullValue) {
        this.sourcePaths = mapperService.sourcePath(fieldName);
        this.nullValue = nullValue;
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) {
        List<Object> values = new ArrayList<>();
        for (String path : sourcePaths) {
            Object sourceValue = lookup.extractValue(path, nullValue);
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
