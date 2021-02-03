/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.mockito.Mockito;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TypeFieldTypeTests extends MapperServiceTestCase {

    public void testDocValues() throws Exception {

        MapperService mapperService = createMapperService(XContentFactory.jsonBuilder()
            .startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = mapperService.documentMapper();
        ParsedDocument document = mapper.parse(source(b -> {}));

        withLuceneIndex(mapperService, iw -> iw.addDocument(document.rootDoc()), r -> {
            MappedFieldType ft = mapperService.fieldType(TypeFieldType.NAME);
            IndexOrdinalsFieldData fd = (IndexOrdinalsFieldData) ft.fielddataBuilder("test", () -> {
                throw new UnsupportedOperationException();
            }).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
            LeafOrdinalsFieldData afd = fd.load(r.leaves().get(0));
            SortedSetDocValues values = afd.getOrdinalsValues();
            assertTrue(values.advanceExact(0));
            assertEquals(0, values.nextOrd());
            assertEquals(SortedSetDocValues.NO_MORE_ORDS, values.nextOrd());
            assertEquals(new BytesRef("type"), values.lookupOrd(0));
        });

        assertWarnings("[types removal] Using the _type field in queries and aggregations is deprecated, prefer to use a field instead.");
    }

    public void testTypeFieldIsNotInDocument() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(source(b -> {}));
        assertThat(document.rootDoc().getFields(TypeFieldType.NAME).length, equalTo(0));
    }

    public void testTermsQuery() {
        SearchExecutionContext context = Mockito.mock(SearchExecutionContext.class);

        TypeFieldType ft = new TypeFieldType("_doc");

        Query query = ft.termQuery("_doc", context);
        assertEquals(new MatchAllDocsQuery(), query);

        query = ft.termQueryCaseInsensitive("_dOc", context);
        assertEquals(new MatchAllDocsQuery(), query);


        query = ft.termQuery("other_type", context);
        assertEquals(new MatchNoDocsQuery(), query);

        query = ft.termQueryCaseInsensitive("other_Type", context);
        assertEquals(new MatchNoDocsQuery(), query);

        assertWarnings("[types removal] Using the _type field in queries and aggregations is deprecated, prefer to use a field instead.");
    }

    public void testExistsQuery() {
        SearchExecutionContext context = Mockito.mock(SearchExecutionContext.class);
        TypeFieldType ft = new TypeFieldType("_doc");
        Query query = ft.existsQuery(context);
        assertEquals(new MatchAllDocsQuery(), query);
        assertWarnings("[types removal] Using the _type field in queries and aggregations is deprecated, prefer to use a field instead.");
    }
}
