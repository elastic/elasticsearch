/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchFieldsPhaseTests extends ESTestCase {

    public void testDocValueFetcher() throws IOException {

        Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
        int numDocs = randomIntBetween(200, 300);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("field", i));
            iw.addDocument(doc);
            if (i % 27 == 0) {
                iw.commit();
            }
        }

        iw.commit();
        IndexReader reader = iw.getReader();
        iw.close();

        FetchFieldsContext ffc = new FetchFieldsContext(List.of(new FieldAndFormat("field", null)));

        SearchExecutionContext sec = mock(SearchExecutionContext.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.valueFetcher(any(), any())).thenReturn(
            new DocValueFetcher(
                DocValueFormat.RAW,
                new SortedNumericIndexFieldData("field", IndexNumericFieldData.NumericType.LONG, CoreValuesSourceType.NUMERIC, null)
            )
        );
        when(sec.getFieldType(any())).thenReturn(fieldType);
        when(sec.getMatchingFieldNames(any())).thenReturn(Set.of("field"));
        when(sec.nestedLookup()).thenReturn(NestedLookup.EMPTY);
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.fetchFieldsContext()).thenReturn(ffc);
        when(fetchContext.getSearchExecutionContext()).thenReturn(sec);

        FetchFieldsPhase phase = new FetchFieldsPhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(fetchContext);
        assertNotNull(processor);

        for (LeafReaderContext context : reader.leaves()) {
            processor.setNextReader(context);
            for (int doc = 0; doc < context.reader().maxDoc(); doc++) {
                SearchHit searchHit = new SearchHit(doc + context.docBase);
                processor.process(new FetchSubPhase.HitContext(searchHit, context, doc, Map.of(), Source.empty(null)));
                assertNotNull(searchHit.getFields().get("field"));
            }
        }

        reader.close();
        dir.close();

    }
}
