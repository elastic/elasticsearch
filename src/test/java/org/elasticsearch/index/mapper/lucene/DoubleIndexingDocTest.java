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
package org.elasticsearch.index.mapper.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperUtils;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class DoubleIndexingDocTest extends ElasticsearchSingleNodeTest {

    @Test
    public void testDoubleIndexingSameDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").endObject()
                .endObject().endObject().string();
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping).get();
        DocumentMapper mapper = index.mapperService().documentMapper("type");

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", 1)
                .field("field3", 1.1)
                .field("field4", "2010-01-01")
                .startArray("field5").value(1).value(2).value(3).endArray()
                .endObject()
                .bytes());
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString()).get();

        writer.addDocument(doc.rootDoc());
        writer.addDocument(doc.rootDoc());

        IndexReader reader = DirectoryReader.open(writer, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field1").termQuery("value1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field2").termQuery("1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field3").termQuery("1.1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field4").termQuery("2010-01-01", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field5").termQuery("1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field5").termQuery("2", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field5").termQuery("3", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));
        writer.close();
        reader.close();
        dir.close();
    }
}
