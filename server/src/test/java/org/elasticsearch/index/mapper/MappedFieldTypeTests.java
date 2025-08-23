/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

public class MappedFieldTypeTests extends ESTestCase {

    private static final class DummyFieldType extends MappedFieldType {

        DummyFieldType(
            String name,
            boolean isIndexed,
            boolean isStored,
            boolean hasDocValues,
            TextSearchInfo textSearchInfo,
            Map<String, String> meta
        ) {
            super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return null;
        }

        @Override
        public String typeName() {
            return "";
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return null;
        }
    }

    public void test_calling_isSyntheticSourceEnabled_when_uninitialized_throws() {
        DummyFieldType dummyFieldType = new DummyFieldType("name", false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        assertThrows(NullPointerException.class, dummyFieldType::isSyntheticSourceEnabled);
    }

    public void test_calling_isWithinMultiField_when_uninitialized_throws() {
        DummyFieldType dummyFieldType = new DummyFieldType("name", false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        assertThrows(NullPointerException.class, dummyFieldType::isSyntheticSourceEnabled);
    }

}
