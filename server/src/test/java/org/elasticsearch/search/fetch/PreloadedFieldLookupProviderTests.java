/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PreloadedFieldLookupProviderTests extends ESTestCase {

    public void testFallback() throws IOException {
        PreloadedFieldLookupProvider lookup = new PreloadedFieldLookupProvider();
        lookup.storedFields = Map.of("foo", List.of("bar"));

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("foo");
        when(fieldType.valueForDisplay(any())).then(invocation -> ((String) invocation.getArguments()[0]).toUpperCase(Locale.ROOT));
        FieldLookup fieldLookup = new FieldLookup(fieldType);

        lookup.populateFieldLookup(fieldLookup, 0);
        assertEquals("BAR", fieldLookup.getValue());
        assertNull(lookup.backUpLoader);    // fallback didn't get used because 'foo' is in the list

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

    public void testNoStoredFields() throws IOException {
        final PreloadedFieldLookupProvider lookup = new PreloadedFieldLookupProvider();

        final MappedFieldType fooField = mock(MappedFieldType.class);
        when(fooField.name()).thenReturn("foo");
        when(fooField.valueForDisplay(any())).then(invocation -> ((String) invocation.getArguments()[0]).toUpperCase(Locale.ROOT));
        final FieldLookup fieldLookup = new FieldLookup(fooField);

        // NOTE: we have no stored fields and we still have to call setNextReader so we don't have a LeafFieldLookupProvider
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> lookup.populateFieldLookup(fieldLookup, 0)
        );
        assertThat(illegalStateException.getMessage(), Matchers.equalTo("unable to lookup field [foo]"));
        assertNull(lookup.backUpLoader);

        final MappedFieldType barField = mock(MappedFieldType.class);
        when(barField.name()).thenReturn("bar");
        when(barField.valueForDisplay(any())).then(
            invocation -> ((BytesRef) invocation.getArguments()[0]).utf8ToString().toUpperCase(Locale.ROOT)
        );
        final FieldLookup barFieldLookup = new FieldLookup(barField);

        final MemoryIndex mi = new MemoryIndex();
        mi.addField(new StringField("bar", "value", Field.Store.YES), null);
        final LeafReaderContext ctx = mi.createSearcher().getIndexReader().leaves().get(0);

        // NOTE: no exception after calling 'setNextReader` as we end up using the backup loader
        lookup.setNextReader(ctx);
        lookup.populateFieldLookup(barFieldLookup, 0);
        assertEquals("VALUE", barFieldLookup.getValue());

        // NOTE: no exception after adding stored fields
        lookup.storedFields = Map.of("foo", List.of("bar"));
        lookup.populateFieldLookup(fieldLookup, 0);
        assertEquals("BAR", fieldLookup.getValue());
    }

}
