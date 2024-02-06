/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class MappedFieldTypeTest extends ESTestCase {

    public void testFieldHasValue() {
        MappedFieldType fieldType = getMappedFieldType("field");
        List<FieldInfos> fieldInfosList = List.of(new FieldInfos(new FieldInfo[] { getFieldInfoWithName("field") }));
        assertTrue(fieldType.fieldHasValue(fieldInfosList));
    }

    public void testFieldEmpty() {
        MappedFieldType fieldType = getMappedFieldType("field");
        List<FieldInfos> fieldInfosList = List.of(new FieldInfos(new FieldInfo[] { getFieldInfoWithName("anotherField") }));
        assertFalse(fieldType.fieldHasValue(fieldInfosList));
    }

    public void testFieldEmptyBecauseEmptyFieldInfosList() {
        MappedFieldType fieldType = getMappedFieldType("field");
        List<FieldInfos> fieldInfosList = List.of();
        assertFalse(fieldType.fieldHasValue(fieldInfosList));
    }

    private MappedFieldType getMappedFieldType(String name) {
        return new MappedFieldType(name, false, false, false, TextSearchInfo.NONE, Collections.emptyMap()) {

            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return null;
            }

            @Override
            public String typeName() {
                return null;
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                return null;
            }
        };
    }

    private FieldInfo getFieldInfoWithName(String name) {
        return new FieldInfo(
            name,
            1,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            new HashMap<>(),
            1,
            1,
            1,
            1,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            randomBoolean()
        );
    }
}
