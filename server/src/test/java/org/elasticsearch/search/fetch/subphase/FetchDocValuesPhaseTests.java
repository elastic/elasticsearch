/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchDocValuesPhaseTests extends ESTestCase {

    private static final String FIELD = "numeric";

    public void testChargesDocValueFieldBytes() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new NumericDocValuesField(FIELD, 42L));
        iw.addDocument(doc);
        iw.commit();
        IndexReader reader = iw.getReader();
        iw.close();

        try (TestRun run = new TestRun()) {
            LeafReaderContext leaf = reader.leaves().get(0);
            run.processor.setNextReader(leaf);
            SearchHit hit = SearchHit.unpooled(0, null);
            try {
                run.processor.process(new FetchSubPhase.HitContext(hit, leaf, 0, Map.of(), Source.empty(null), null));
                assertNotNull(hit.getFields().get(FIELD));
                // baseline is 0 for a new field, so the full estimate (object + name + values) is charged
                assertEquals(hit.field(FIELD).ramBytesUsedEstimate(), run.charged.stream().mapToLong(Long::longValue).sum());
            } finally {
                hit.decRef();
            }
        }
        reader.close();
        dir.close();
    }

    /**
     * Verifies that when a DocumentField already exists on the hit (e.g. from StoredFieldsPhase),
     * FetchDocValuesPhase charges only the size delta of the newly appended values, not the
     * full field size — avoiding double-counting bytes already charged by the earlier phase.
     */
    public void testChargesOnlyDeltaWhenFieldPreExists() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new NumericDocValuesField(FIELD, 99L));
        iw.addDocument(doc);
        iw.commit();
        IndexReader reader = iw.getReader();
        iw.close();

        try (TestRun run = new TestRun()) {
            LeafReaderContext leaf = reader.leaves().get(0);
            run.processor.setNextReader(leaf);
            SearchHit hit = SearchHit.unpooled(0, null);
            try {
                // simulate a field already set by a prior phase (e.g. StoredFieldsPhase)
                DocumentField existing = new DocumentField(FIELD, new ArrayList<>(List.of("stored-value")));
                long beforeBytes = existing.ramBytesUsedEstimate();
                hit.setDocumentField(existing);

                run.processor.process(new FetchSubPhase.HitContext(hit, leaf, 0, Map.of(), Source.empty(null), null));

                long afterBytes = hit.field(FIELD).ramBytesUsedEstimate();
                long expectedDelta = afterBytes - beforeBytes;
                assertThat(expectedDelta, greaterThan(0L));
                assertEquals(expectedDelta, run.charged.stream().mapToLong(Long::longValue).sum());
            } finally {
                hit.decRef();
            }
        }
        reader.close();
        dir.close();
    }

    private final class TestRun implements AutoCloseable {
        final List<Long> charged = new ArrayList<>();
        final FetchSubPhaseProcessor processor;
        final TestSearchContext searchContext;

        TestRun() throws IOException {
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
            SearchExecutionContext sec = mock(SearchExecutionContext.class);
            when(sec.getMatchingFieldNames(FIELD)).thenReturn(Set.of(FIELD));
            when(sec.getIndexSettings()).thenReturn(indexSettings);

            MappedFieldType fieldType = mock(MappedFieldType.class);
            when(fieldType.docValueFormat(any(), any())).thenReturn(DocValueFormat.RAW);
            when(sec.getFieldType(FIELD)).thenReturn(fieldType);
            when(sec.getForField(any(), any())).thenReturn(
                new SortedNumericIndexFieldData(
                    FIELD,
                    IndexNumericFieldData.NumericType.LONG,
                    CoreValuesSourceType.NUMERIC,
                    null,
                    IndexType.NONE
                )
            );

            FetchDocValuesContext dvContext = new FetchDocValuesContext(sec, List.of(new FieldAndFormat(FIELD, null)));

            searchContext = new TestSearchContext(sec) {
                @Override
                public FetchDocValuesContext docValuesContext() {
                    return dvContext;
                }
            };
            FetchContext fetchContext = new FetchContext(searchContext, null);
            fetchContext.setDocumentFieldsByteChecker(bytes -> charged.add(bytes));

            processor = new FetchDocValuesPhase().getProcessor(fetchContext);
            assertNotNull(processor);
        }

        @Override
        public void close() throws Exception {
            searchContext.close();
        }
    }
}
