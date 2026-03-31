/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PreloadedFieldLookupProviderTests extends ESTestCase {

    public void testFallback() throws IOException {
        PreloadedFieldLookupProvider lookup = new PreloadedFieldLookupProvider();
        lookup.setPreloadedStoredFieldNames(Collections.singleton("foo"));
        lookup.setPreloadedStoredFieldValues("id", Map.of("foo", List.of("bar")));

        MappedFieldType idFieldType = mock(MappedFieldType.class);
        when(idFieldType.name()).thenReturn(IdFieldMapper.NAME);
        when(idFieldType.valueForDisplay(any())).then(invocation -> (invocation.getArguments()[0]));
        FieldLookup idFieldLookup = new FieldLookup(idFieldType);
        lookup.populateFieldLookup(idFieldLookup, 0);
        assertEquals("id", idFieldLookup.getValue());
        assertNull(lookup.getBackUpLoader());    // fallback didn't get used because 'foo' is in the list

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("foo");
        when(fieldType.valueForDisplay(any())).then(invocation -> ((String) invocation.getArguments()[0]).toUpperCase(Locale.ROOT));
        FieldLookup fieldLookup = new FieldLookup(fieldType);

        lookup.populateFieldLookup(fieldLookup, 0);
        assertEquals("BAR", fieldLookup.getValue());
        assertNull(lookup.getBackUpLoader());    // fallback didn't get used because 'foo' is in the list

        MappedFieldType unloadedFieldType = mock(MappedFieldType.class);
        when(unloadedFieldType.name()).thenReturn("unloaded");
        when(unloadedFieldType.valueForDisplay(any())).then(
            invocation -> ((BytesRef) invocation.getArguments()[0]).utf8ToString().toUpperCase(Locale.ROOT)
        );
        FieldLookup unloadedFieldLookup = new FieldLookup(unloadedFieldType);

        MemoryIndex mi = new MemoryIndex();
        mi.addField(new StringField("unloaded", "value", Field.Store.YES), null);
        LeafReaderContext ctx = mi.createSearcher().getIndexReader().leaves().get(0);

        lookup.setNextReader(ctx);
        lookup.populateFieldLookup(unloadedFieldLookup, 0);
        assertEquals("VALUE", unloadedFieldLookup.getValue());
    }
}
