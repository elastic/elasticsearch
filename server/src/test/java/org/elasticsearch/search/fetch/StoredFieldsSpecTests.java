/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.SearchExecutionContext;
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
        // defaults - return source and metadata fields
        FetchContext fc = new FetchContext(searchContext(search));
        SearchExecutionContext sec = mock(SearchExecutionContext.class);
        Settings settings = settings(
            randomFrom(IndexVersion.current(), IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD, IndexVersions.TIME_SERIES_ID_HASHING)
        ).build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("foo").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        when(sec.getIndexSettings()).thenReturn(new IndexSettings(indexMetadata, Settings.EMPTY));
        when(fc.getSearchExecutionContext()).thenReturn(sec);

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
        FetchContext fc = new FetchContext(searchContext(search));

        assertNull(new StoredFieldsPhase().getProcessor(fc));
        assertNull(new FetchSourcePhase().getProcessor(fc));
    }

    public void testScriptFieldsEnableMetadata() {
        SearchSourceBuilder search = new SearchSourceBuilder();
        search.scriptField("field", new Script("script"));
        FetchContext fc = new FetchContext(searchContext(search));

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
