/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

public class ValueFetcherTests extends ESTestCase {

    public void testNoValuesReturnsNull() throws Exception {
        assertNull(fetcher(List.of(), List.of()).fetchDocumentField("f", Source.empty(null), 0));
    }

    public void testEmptyIgnoredValuesUsesSharedEmptyListIdentity() throws Exception {
        String value = randomAlphanumericOfLength(8);
        DocumentField field = fetcher(List.of(value), List.of()).fetchDocumentField("f", Source.empty(null), 0);

        assertNotNull(field);
        assertEquals(List.of(value), field.getValues());
        // The fast path in DocumentField#getIgnoredValues returns the shared emptyList identity unchanged.
        assertSame(Collections.emptyList(), field.getIgnoredValues());
    }

    public void testIgnoredValuesAreRetained() throws Exception {
        String value = randomAlphanumericOfLength(8);
        String ignored = randomAlphanumericOfLength(8);
        DocumentField field = fetcher(List.of(value), List.of(ignored)).fetchDocumentField("f", Source.empty(null), 0);

        assertNotNull(field);
        assertEquals(List.of(value), field.getValues());
        assertEquals(List.of(ignored), field.getIgnoredValues());
    }

    private static ValueFetcher fetcher(List<Object> values, List<Object> ignored) {
        return new ValueFetcher() {
            @Override
            public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
                ignoredValues.addAll(ignored);
                return values;
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    }
}
