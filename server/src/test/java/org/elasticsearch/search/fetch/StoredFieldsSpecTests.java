/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.fetch.subphase.StoredFieldsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoredFieldsSpecTests extends ESTestCase {

    public void testDefaults() throws IOException {
        SearchSourceBuilder search = new SearchSourceBuilder();
        // defaults - return source and metadata fields
        FetchContext fc = new FetchContext(searchContext(search));

        FetchSubPhaseProcessor sourceProcessor = new FetchSourcePhase().getProcessor(fc);
        assertNotNull(sourceProcessor);
        StoredFieldsSpec sourceSpec = sourceProcessor.storedFieldsSpec();
        assertTrue(sourceSpec.requiresSource());
        assertThat(sourceSpec.requiredStoredFields(), hasSize(0));

        FetchSubPhaseProcessor storedFieldsProcessor = new StoredFieldsPhase().getProcessor(fc);
        assertNotNull(storedFieldsProcessor);
        StoredFieldsSpec storedFieldsSpec = storedFieldsProcessor.storedFieldsSpec();
        assertFalse(storedFieldsSpec.requiresSource());
        assertEquals(storedFieldsSpec.requiredStoredFields(), Set.of("_id"));

        StoredFieldsSpec merged = sourceSpec.merge(storedFieldsSpec);
        assertTrue(merged.requiresSource());
        assertEquals(merged.requiredStoredFields(), Set.of("_id"));
    }

    public void testStoredFieldsDisabled() throws IOException {
        SearchSourceBuilder search = new SearchSourceBuilder();
        search.storedField("_none_");
        FetchContext fc = new FetchContext(searchContext(search));

        assertNull(new StoredFieldsPhase().getProcessor(fc));
        assertNull(new FetchSourcePhase().getProcessor(fc));
    }

    private static SearchContext searchContext(SearchSourceBuilder sourceBuilder) {
        SearchContext sc = mock(SearchContext.class);
        when(sc.fetchSourceContext()).thenReturn(sourceBuilder.fetchSource());
        when(sc.storedFieldsContext()).thenReturn(sourceBuilder.storedFields());
        return sc;
    }

}
