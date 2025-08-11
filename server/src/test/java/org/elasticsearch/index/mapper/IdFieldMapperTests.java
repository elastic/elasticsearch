/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.core.List;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.mapper.IdFieldMapper.ID_FIELD_DATA_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IdFieldMapperTests extends MapperServiceTestCase {

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        Exception e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source(b -> b.field("_id", 1))));

        assertThat(e.getCause().getMessage(), containsString("Field [_id] is a metadata field and cannot be added inside a document"));
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(source(b -> {}));
        IndexableField[] fields = document.rootDoc().getFields(IdFieldMapper.NAME);
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.DOCS, fields[0].fieldType().indexOptions());
        assertTrue(fields[0].fieldType().stored());
        assertEquals(Uid.encodeId("1"), fields[0].binaryValue());
    }

    public void testEnableFieldData() throws IOException {

        boolean[] enabled = new boolean[1];

        MapperService mapperService = createMapperService(() -> enabled[0], mapping(b -> {}));
        IdFieldMapper.IdFieldType ft = (IdFieldMapper.IdFieldType) mapperService.fieldType("_id");

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> ft.fielddataBuilder("test", () -> {
            throw new UnsupportedOperationException();
        }).build(null, null));
        assertThat(exc.getMessage(), containsString(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));
        assertFalse(ft.isAggregatable());

        enabled[0] = true;
        ft.fielddataBuilder("test", () -> { throw new UnsupportedOperationException(); }).build(null, null);
        assertWarnings(ID_FIELD_DATA_DEPRECATION_MESSAGE);
        assertTrue(ft.isAggregatable());
    }

    public void testFetchIdFieldValue() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        String id = randomAlphaOfLength(12);
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(id, b -> b.field("field", "value"), null)).rootDoc());
        }, iw -> {
            SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup());
            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.lookup()).thenReturn(lookup);
            IdFieldMapper.IdFieldType ft = (IdFieldMapper.IdFieldType) mapperService.fieldType("_id");
            ValueFetcher valueFetcher = ft.valueFetcher(searchExecutionContext, null);
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            lookup.source().setSegmentAndDocument(context, 0);
            valueFetcher.setNextReader(context);
            assertEquals(Collections.singletonList(id), valueFetcher.fetchValues(lookup.source(), List.of()));
        });
    }
}
