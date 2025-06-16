/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsPhase;
import org.elasticsearch.search.fetch.subphase.StoredFieldsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoredFieldsSpecTests extends ESTestCase {

    public void testDefaults() {
        SearchSourceBuilder search = new SearchSourceBuilder();
        var context = searchContext(search);
        // defaults - return source and metadata fields
        FetchContext fc = new FetchContext(context, context.newSourceLoader(null));

        FetchSubPhaseProcessor sourceProcessor = new FetchSourcePhase().getProcessor(fc);
        assertNotNull(sourceProcessor);
        StoredFieldsSpec sourceSpec = sourceProcessor.storedFieldsSpec();
        assertTrue(sourceSpec.requiresSource());
        assertThat(sourceSpec.requiredStoredFields(), hasSize(0));

        FetchSubPhaseProcessor storedFieldsProcessor = new StoredFieldsPhase().getProcessor(fc);
        assertNotNull(storedFieldsProcessor);
        StoredFieldsSpec storedFieldsSpec = storedFieldsProcessor.storedFieldsSpec();
        assertFalse(storedFieldsSpec.requiresSource());
        assertThat(storedFieldsSpec.requiredStoredFields(), hasSize(0));

        StoredFieldsSpec merged = sourceSpec.merge(storedFieldsSpec);
        assertTrue(merged.requiresSource());
        assertThat(merged.requiredStoredFields(), hasSize(0));
    }

    public void testStoredFieldsDisabled() {
        SearchSourceBuilder search = new SearchSourceBuilder();
        search.storedField("_none_");
        var context = searchContext(search);
        FetchContext fc = new FetchContext(context, context.newSourceLoader(null));

        assertNull(new StoredFieldsPhase().getProcessor(fc));
        assertNull(new FetchSourcePhase().getProcessor(fc));
    }

    public void testScriptFieldsEnableMetadata() {
        SearchSourceBuilder search = new SearchSourceBuilder();
        search.scriptField("field", new Script("script"));
        var context = searchContext(search);
        FetchContext fc = new FetchContext(context, null);

        FetchSubPhaseProcessor subPhaseProcessor = new ScriptFieldsPhase().getProcessor(fc);
        assertNotNull(subPhaseProcessor);
        StoredFieldsSpec spec = subPhaseProcessor.storedFieldsSpec();
        assertFalse(spec.requiresSource());
        assertTrue(spec.requiresMetadata());
    }

    private static SearchContext searchContext(SearchSourceBuilder sourceBuilder) {
        SearchContext sc = mock(SearchContext.class);
        when(sc.fetchSourceContext()).thenReturn(sourceBuilder.fetchSource());
        when(sc.storedFieldsContext()).thenReturn(sourceBuilder.storedFields());
        ScriptFieldsContext scriptContext = new ScriptFieldsContext();
        if (sourceBuilder.scriptFields() != null) {
            for (SearchSourceBuilder.ScriptField scriptField : sourceBuilder.scriptFields()) {
                scriptContext.add(new ScriptFieldsContext.ScriptField(scriptField.fieldName(), null, true));
            }
        }
        when(sc.scriptFields()).thenReturn(scriptContext);
        return sc;
    }

}
