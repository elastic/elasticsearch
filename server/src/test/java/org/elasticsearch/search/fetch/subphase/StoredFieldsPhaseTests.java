/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoredFieldsPhaseTests extends ESTestCase {

    private static final String FIELD = "my_stored_field";

    private List<Long> charged;
    private FetchSubPhaseProcessor processor;
    private TestSearchContext searchContext;

    @Before
    public void setUpPhase() {
        charged = new ArrayList<>();
        // Minimal stored MappedFieldType — isStored=true so StoredFieldsPhase includes it.
        MappedFieldType storedFieldType = new MappedFieldType(FIELD, IndexType.NONE, true, Map.of()) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String typeName() {
                return "test";
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new UnsupportedOperationException();
            }
        };

        SearchExecutionContext sec = mock(SearchExecutionContext.class);
        when(sec.getMatchingFieldNames(FIELD)).thenReturn(Set.of(FIELD));
        when(sec.getFieldType(FIELD)).thenReturn(storedFieldType);
        when(sec.isMetadataField(any())).thenReturn(false);

        StoredFieldsContext storedFieldsCtx = StoredFieldsContext.fromList(List.of(FIELD));

        searchContext = new TestSearchContext(sec) {
            @Override
            public StoredFieldsContext storedFieldsContext() {
                return storedFieldsCtx;
            }
        };
        FetchContext fetchContext = new FetchContext(searchContext, null);
        fetchContext.setDocumentFieldsByteChecker(bytes -> charged.add(bytes));

        processor = new StoredFieldsPhase().getProcessor(fetchContext);
        assertNotNull(processor);
    }

    @After
    public void tearDownPhase() throws Exception {
        searchContext.close();
    }

    public void testChargesBytesForStoredFields() throws Exception {
        MemoryIndex index = new MemoryIndex();
        var leafCtx = index.createSearcher().getIndexReader().leaves().get(0);
        SearchHit hit = SearchHit.unpooled(0, null);
        try {
            Map<String, List<Object>> loadedFields = Map.of(FIELD, List.of("hello", "world"));
            processor.process(new FetchSubPhase.HitContext(hit, leafCtx, 0, loadedFields, Source.empty(null), null));
            assertNotNull(hit.field(FIELD));
            assertThat(charged.stream().mapToLong(Long::longValue).sum(), greaterThan(0L));
        } finally {
            hit.decRef();
        }
    }

    public void testNoChargeWhenNoMatchingLoadedFields() throws Exception {
        MemoryIndex index = new MemoryIndex();
        var leafCtx = index.createSearcher().getIndexReader().leaves().get(0);
        SearchHit hit = SearchHit.unpooled(0, null);
        try {
            // loaded fields map does not contain FIELD
            processor.process(new FetchSubPhase.HitContext(hit, leafCtx, 0, Map.of(), Source.empty(null), null));
            assertEquals(0L, charged.stream().mapToLong(Long::longValue).sum());
        } finally {
            hit.decRef();
        }
    }
}
