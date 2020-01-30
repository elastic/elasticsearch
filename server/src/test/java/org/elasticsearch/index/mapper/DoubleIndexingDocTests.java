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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DoubleIndexingDocTests extends ESSingleNodeTestCase {
    public void testDoubleIndexingSameDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), Lucene.STANDARD_ANALYZER));

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").endObject()
                .endObject().endObject());
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        MapperService mapperService = index.mapperService();
        DocumentMapper mapper = mapperService.documentMapper();

        QueryShardContext context = index.newQueryShardContext(0, null, () -> 0L, null);

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "value1")
                        .field("field2", 1)
                        .field("field3", 1.1)
                        .field("field4", "2010-01-01")
                        .startArray("field5").value(1).value(2).value(3).endArray()
                        .endObject()),
                XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test")
            .setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();

        writer.addDocument(doc.rootDoc());
        writer.addDocument(doc.rootDoc());

        IndexReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(mapperService.fullName("field1").termQuery("value1", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));

        topDocs = searcher.search(mapperService.fullName("field2").termQuery("1", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));

        topDocs = searcher.search(mapperService.fullName("field3").termQuery("1.1", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));

        topDocs = searcher.search(mapperService.fullName("field4").termQuery("2010-01-01", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));

        topDocs = searcher.search(mapperService.fullName("field5").termQuery("1", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));

        topDocs = searcher.search(mapperService.fullName("field5").termQuery("2", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));

        topDocs = searcher.search(mapperService.fullName("field5").termQuery("3", context), 10);
        assertThat(topDocs.totalHits.value, equalTo(2L));
        writer.close();
        reader.close();
        dir.close();
    }
}
