/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
