/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.query.SearchExecutionContext;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class DoubleIndexingDocTests extends MapperServiceTestCase {
    public void testDoubleIndexingSameDoc() throws Exception {

        MapperService mapperService = createMapperService(mapping(b -> {}));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", 1);
            b.field("field3", 1.1);
            b.field("field4", "2010-01-01");
            b.startArray("field5").value(1).value(2).value(3).endArray();
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        SearchExecutionContext context = mock(SearchExecutionContext.class);

        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(doc.rootDoc());
            iw.addDocument(doc.rootDoc());
        }, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(mapperService.fieldType("field1").termQuery("value1", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));

            topDocs = searcher.search(mapperService.fieldType("field2").termQuery("1", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));

            topDocs = searcher.search(mapperService.fieldType("field3").termQuery("1.1", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));

            topDocs = searcher.search(mapperService.fieldType("field4").termQuery("2010-01-01", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));

            topDocs = searcher.search(mapperService.fieldType("field5").termQuery("1", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));

            topDocs = searcher.search(mapperService.fieldType("field5").termQuery("2", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));

            topDocs = searcher.search(mapperService.fieldType("field5").termQuery("3", context), 10);
            assertThat(topDocs.totalHits.value, equalTo(2L));
        });
    }
}
