/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsPhase;
import org.elasticsearch.search.fetch.subphase.StoredFieldsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
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

    public void testNoCloneOnMerge() {
        StoredFieldsSpec spec = StoredFieldsSpec.NO_REQUIREMENTS;
        spec = spec.merge(StoredFieldsSpec.NEEDS_SOURCE);
        assertThat(spec.requiredStoredFields(), sameInstance(StoredFieldsSpec.NO_REQUIREMENTS.requiredStoredFields()));

        StoredFieldsSpec needsCat = new StoredFieldsSpec(false, false, Set.of("cat"));
        StoredFieldsSpec withCat = spec.merge(needsCat);
        spec = withCat.merge(StoredFieldsSpec.NO_REQUIREMENTS);
        assertThat(spec.requiredStoredFields(), sameInstance(withCat.requiredStoredFields()));
    }

    public void testMergeSourcePaths() {
        StoredFieldsSpec spec = StoredFieldsSpec.NO_REQUIREMENTS;
        spec = spec.merge(
            new StoredFieldsSpec(true, false, Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE, Set.of("cat"))
        );
        assertThat(spec.ignoredSourceFormat(), equalTo(IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE));
        assertThat(spec.requiresSource(), equalTo(true));
        assertThat(spec.requiresMetadata(), equalTo(false));
        assertThat(spec.requiredStoredFields(), empty());
        assertThat(spec.sourcePaths(), containsInAnyOrder("cat"));

        spec = spec.merge(
            new StoredFieldsSpec(true, false, Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE, Set.of("dog"))
        );
        assertThat(spec.ignoredSourceFormat(), equalTo(IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE));
        assertThat(spec.requiresSource(), equalTo(true));
        assertThat(spec.requiresMetadata(), equalTo(false));
        assertThat(spec.requiredStoredFields(), empty());
        assertThat(spec.sourcePaths(), containsInAnyOrder("cat", "dog"));

        spec = spec.merge(
            new StoredFieldsSpec(true, false, Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE, Set.of("hamster"))
        );
        assertThat(spec.ignoredSourceFormat(), equalTo(IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE));
        assertThat(spec.requiresSource(), equalTo(true));
        assertThat(spec.requiresMetadata(), equalTo(false));
        assertThat(spec.requiredStoredFields(), empty());
        assertThat(spec.sourcePaths(), containsInAnyOrder("cat", "dog", "hamster"));
        var pref = spec.sourcePaths();

        spec = spec.merge(
            new StoredFieldsSpec(
                false, // if set to true, then the new spec will require complete and source and so source paths would then be empty
                false,
                Set.of("other_field"),
                IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE,
                Set.of()
            )
        );
        assertThat(spec.ignoredSourceFormat(), equalTo(IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE));
        assertThat(spec.requiresSource(), equalTo(true));
        assertThat(spec.requiresMetadata(), equalTo(false));
        assertThat(spec.requiredStoredFields(), containsInAnyOrder("other_field"));
        assertThat(spec.sourcePaths(), containsInAnyOrder("cat", "dog", "hamster"));
        assertThat(spec.sourcePaths(), sameInstance(pref));
    }

    public void testMergeSourcePathsRequireCompleteSource() {
        var ignoredSourceFormat = IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE;
        StoredFieldsSpec spec = new StoredFieldsSpec(true, false, Set.of(), ignoredSourceFormat, Set.of("field1", "field2"));
        assertThat(spec.ignoredSourceFormat(), equalTo(ignoredSourceFormat));
        assertThat(spec.requiresSource(), equalTo(true));
        assertThat(spec.requiresMetadata(), equalTo(false));
        assertThat(spec.requiredStoredFields(), empty());
        assertThat(spec.sourcePaths(), containsInAnyOrder("field1", "field2"));

        // Clears source paths, because this spec requires complete source (since no source paths are defined)
        spec = spec.merge(new StoredFieldsSpec(true, false, Set.of(), ignoredSourceFormat, Set.of()));
        assertThat(spec.ignoredSourceFormat(), equalTo(ignoredSourceFormat));
        assertThat(spec.requiresSource(), equalTo(true));
        assertThat(spec.requiresMetadata(), equalTo(false));
        assertThat(spec.requiredStoredFields(), empty());
        assertThat(spec.sourcePaths(), empty());
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
