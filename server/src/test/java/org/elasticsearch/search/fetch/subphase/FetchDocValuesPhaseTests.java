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
import org.elasticsearch.index.fielddata.IndexFieldData;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchDocValuesPhaseTests extends ESTestCase {

    /**
     * A document that has no value for a requested {@code docvalue_fields} field must not carry that field on its hit at all, rather
     * than carrying a present-but-empty {@link DocumentField}. This matches the {@code fields} path, where
     * {@link org.elasticsearch.index.mapper.ValueFetcher#fetchDocumentField} returns null for an empty result and the field is skipped.
     */
    public void testFieldIsAbsentWhenDocumentHasNoValue() throws IOException {
        String fieldName = "field";
        Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
        int numDocs = randomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            // Only every third document has a value; the rest exercise the empty-values path.
            if (i % 3 == 0) {
                doc.add(new NumericDocValuesField(fieldName, i));
            }
            iw.addDocument(doc);
        }
        iw.commit();
        IndexReader reader = iw.getReader();
        iw.close();

        FetchSubPhaseProcessor processor = buildProcessor(
            new FieldConfig(
                fieldName,
                new SortedNumericIndexFieldData(
                    fieldName,
                    IndexNumericFieldData.NumericType.LONG,
                    CoreValuesSourceType.NUMERIC,
                    null,
                    IndexType.NONE
                )
            )
        );

        for (LeafReaderContext context : reader.leaves()) {
            processor.setNextReader(context);
            for (int doc = 0; doc < context.reader().maxDoc(); doc++) {
                int globalDoc = context.docBase + doc;
                SearchHit searchHit = new SearchHit(globalDoc);
                try {
                    processor.process(new FetchSubPhase.HitContext(searchHit, context, doc, Map.of(), Source.empty(null), null));
                    if (globalDoc % 3 == 0) {
                        DocumentField field = searchHit.field(fieldName);
                        assertNotNull("expected a value for doc [" + globalDoc + "]", field);
                        // Asserting the value also validates that doc ids map back to insertion order, which is what lets the
                        // modulo above identify which documents were given a value.
                        assertThat(field.getValues(), contains((long) globalDoc));
                    } else {
                        assertNull("expected no field for doc [" + globalDoc + "]", searchHit.field(fieldName));
                        assertThat(searchHit.getFields(), anEmptyMap());
                    }
                } finally {
                    searchHit.decRef();
                }
            }
        }

        reader.close();
        dir.close();
    }

    /**
     * Describes one {@code docvalue_fields} entry for {@link #buildProcessor}: the field name and the field data the mocked
     * {@link SearchExecutionContext} should hand back for it.
     */
    private record FieldConfig(String name, IndexFieldData<?> fieldData) {}

    /**
     * Builds a {@link FetchDocValuesPhase} processor over the given fields, each backed by doc values.
     */
    private static FetchSubPhaseProcessor buildProcessor(FieldConfig... fields) {
        SearchExecutionContext sec = mock(SearchExecutionContext.class);

        Map<String, IndexFieldData<?>> fieldData = new HashMap<>();
        List<FieldAndFormat> fieldAndFormats = new ArrayList<>(fields.length);
        for (FieldConfig field : fields) {
            MappedFieldType fieldType = mock(MappedFieldType.class);
            when(fieldType.name()).thenReturn(field.name());
            when(fieldType.docValueFormat(any(), any())).thenReturn(DocValueFormat.RAW);
            when(sec.getFieldType(eq(field.name()))).thenReturn(fieldType);
            fieldData.put(field.name(), field.fieldData());
            // Hard-code the format to null: the mocked field type above resolves every format to RAW, so requesting a specific
            // one here would suggest a knob that has no effect.
            fieldAndFormats.add(new FieldAndFormat(field.name(), null));
        }

        when(sec.getIndexSettings()).thenReturn(IndexSettingsModule.newIndexSettings("index", Settings.EMPTY));
        when(sec.getMatchingFieldNames(any())).then(invocation -> {
            String fieldName = (String) invocation.getArguments()[0];
            return fieldData.containsKey(fieldName) ? Set.of(fieldName) : Set.of();
        });
        when(sec.getForField(any(), any())).then(invocation -> {
            MappedFieldType fieldType = (MappedFieldType) invocation.getArguments()[0];
            return fieldData.get(fieldType.name());
        });

        FetchContext fetchContext = mock(FetchContext.class);
        FetchDocValuesContext docValuesContext = new FetchDocValuesContext(sec, fieldAndFormats);
        when(fetchContext.docValuesContext()).thenReturn(docValuesContext);
        when(fetchContext.getSearchExecutionContext()).thenReturn(sec);
        when(fetchContext.omitEmptyDocValueFields()).thenReturn(true);

        FetchSubPhaseProcessor processor = new FetchDocValuesPhase().getProcessor(fetchContext);
        assertNotNull(processor);
        return processor;
    }
}
