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
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.StoredValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
                new SortedNumericIndexFieldData("field", IndexNumericFieldData.NumericType.LONG, CoreValuesSourceType.NUMERIC, null, false)
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
                SearchHit searchHit = SearchHit.unpooled(doc + context.docBase);
                processor.process(new FetchSubPhase.HitContext(searchHit, context, doc, Map.of(), Source.empty(null)));
                assertNotNull(searchHit.getFields().get("field"));
            }
        }

        reader.close();
        dir.close();
    }

    public void testStoredFieldsSpec() {
        SearchExecutionContext sec = mock(SearchExecutionContext.class);
        when(sec.getMatchingFieldNames(any())).then(invocation -> Set.of(invocation.getArguments()[0]));
        when(sec.nestedLookup()).thenReturn(NestedLookup.EMPTY);
        when(sec.isMetadataField(any())).then(invocation -> invocation.getArguments()[0].toString().startsWith("_"));

        when(sec.getFieldType(eq(RoutingFieldMapper.NAME))).thenReturn(RoutingFieldMapper.FIELD_TYPE);
        // this would normally not be mapped -> getMatchingFieldsNames would not resolve it (unless for older archive indices)
        when(sec.getFieldType(eq(LegacyTypeFieldMapper.NAME))).thenReturn(LegacyTypeFieldMapper.FIELD_TYPE);
        when(sec.getFieldType(eq(IgnoredFieldMapper.NAME))).thenReturn(IgnoredFieldMapper.FIELD_TYPE);

        MappedFieldType storedFt = new StringFieldType(
            "stored",
            randomBoolean(),
            true,
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("stored"))).thenReturn(storedFt);

        MappedFieldType metadataFt = new StringFieldType(
            "_metadata",
            randomBoolean(),
            true,
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new StoredValueFetcher(context.lookup(), "_metadata");
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("_metadata"))).thenReturn(metadataFt);

        MappedFieldType fieldFt = new StringFieldType(
            "field",
            false,
            randomBoolean(),
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new DocValueFetcher(
                    DocValueFormat.RAW,
                    new SortedNumericIndexFieldData(
                        "field",
                        IndexNumericFieldData.NumericType.LONG,
                        CoreValuesSourceType.NUMERIC,
                        null,
                        false
                    )
                );
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("field"))).thenReturn(fieldFt);

        MappedFieldType storedFieldFt = new StringFieldType(
            "stored_field",
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new StoredValueFetcher(context.lookup(), "stored_field");
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("stored_field"))).thenReturn(storedFieldFt);

        StoredFieldsContext storedFieldsContext = StoredFieldsContext.fromList(List.of("stored", "_metadata"));
        FetchFieldsContext ffc = new FetchFieldsContext(
            List.of(new FieldAndFormat("field", null), new FieldAndFormat("stored_field", null))
        );
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.fetchFieldsContext()).thenReturn(ffc);
        when(fetchContext.storedFieldsContext()).thenReturn(storedFieldsContext);
        when(fetchContext.getSearchExecutionContext()).thenReturn(sec);
        FetchFieldsPhase fetchFieldsPhase = new FetchFieldsPhase();
        FetchSubPhaseProcessor processor = fetchFieldsPhase.getProcessor(fetchContext);
        StoredFieldsSpec storedFieldsSpec = processor.storedFieldsSpec();
        assertEquals(5, storedFieldsSpec.requiredStoredFields().size());
        // the following three fields are included because they are metadata fields and they are stored
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_routing"));
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_ignored"));
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_type"));
        // _metadata is included because it is a metadata field and it is stored
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_metadata"));
        // field is not included as it is not stored (exposes a doc value fetcher)
        // stored is not included because it was required via stored_fields and fetch fields phase
        // only retrieves metadata fields requested via stored_fields
        // stored_field was requested via fields hence it is included
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("stored_field"));
    }

    public void testStoredFieldsSpecWildcardPatterns() {
        SearchExecutionContext sec = mock(SearchExecutionContext.class);
        when(sec.getMatchingFieldNames(any())).thenReturn(
            Set.of(
                RoutingFieldMapper.NAME,
                LegacyTypeFieldMapper.NAME,
                IgnoredFieldMapper.NAME,
                "stored",
                "_metadata",
                "field",
                "stored_field"
            )
        );
        when(sec.nestedLookup()).thenReturn(NestedLookup.EMPTY);
        when(sec.isMetadataField(any())).then(invocation -> invocation.getArguments()[0].toString().startsWith("_"));

        when(sec.getFieldType(eq(RoutingFieldMapper.NAME))).thenReturn(RoutingFieldMapper.FIELD_TYPE);
        // this would normally not be mapped -> getMatchingFieldsNames would not resolve it (unless for older archive indices)
        when(sec.getFieldType(eq(LegacyTypeFieldMapper.NAME))).thenReturn(LegacyTypeFieldMapper.FIELD_TYPE);
        when(sec.getFieldType(eq(IgnoredFieldMapper.NAME))).thenReturn(IgnoredFieldMapper.FIELD_TYPE);

        MappedFieldType storedFt = new StringFieldType(
            "stored",
            randomBoolean(),
            true,
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new StoredValueFetcher(context.lookup(), "stored");
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("stored"))).thenReturn(storedFt);

        MappedFieldType metadataFt = new StringFieldType(
            "_metadata",
            randomBoolean(),
            true,
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new StoredValueFetcher(context.lookup(), "_metadata");
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("_metadata"))).thenReturn(metadataFt);

        MappedFieldType fieldFt = new StringFieldType(
            "field",
            false,
            randomBoolean(),
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new DocValueFetcher(
                    DocValueFormat.RAW,
                    new SortedNumericIndexFieldData(
                        "field",
                        IndexNumericFieldData.NumericType.LONG,
                        CoreValuesSourceType.NUMERIC,
                        null,
                        false
                    )
                );
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("field"))).thenReturn(fieldFt);

        MappedFieldType storedFieldFt = new StringFieldType(
            "stored_field",
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            TextSearchInfo.NONE,
            Collections.emptyMap()
        ) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return new StoredValueFetcher(context.lookup(), "stored_field");
            }

            @Override
            public String typeName() {
                return "test";
            }
        };
        when(sec.getFieldType(eq("stored_field"))).thenReturn(storedFieldFt);

        StoredFieldsContext storedFieldsContext = StoredFieldsContext.fromList(List.of("*"));
        FetchFieldsContext ffc = new FetchFieldsContext(List.of(new FieldAndFormat("*", null)));
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.fetchFieldsContext()).thenReturn(ffc);
        when(fetchContext.storedFieldsContext()).thenReturn(storedFieldsContext);
        when(fetchContext.getSearchExecutionContext()).thenReturn(sec);
        FetchFieldsPhase fetchFieldsPhase = new FetchFieldsPhase();
        FetchSubPhaseProcessor processor = fetchFieldsPhase.getProcessor(fetchContext);
        StoredFieldsSpec storedFieldsSpec = processor.storedFieldsSpec();
        assertEquals(6, storedFieldsSpec.requiredStoredFields().size());
        // the following three fields are included because they are metadata fields and they are stored
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_routing"));
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_ignored"));
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_type"));
        // _metadata is included because it is a metadata field and it is stored
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("_metadata"));
        // field is not included as it is not stored (exposes a doc value fetcher)
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("stored_field"));
        assertTrue(storedFieldsSpec.requiredStoredFields().contains("stored"));
    }
}
