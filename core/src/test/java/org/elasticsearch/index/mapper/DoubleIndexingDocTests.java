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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DoubleIndexingDocTests extends ESSingleNodeTestCase {
    public void testDoubleIndexingSameDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").endObject()
                .endObject().endObject().string();
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping, XContentType.JSON).get();
        DocumentMapper mapper = index.mapperService().documentMapper("type");
        QueryShardContext context = index.newQueryShardContext(0, null, () -> 0L, null);

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", 1)
                .field("field3", 1.1)
                .field("field4", "2010-01-01")
                .startArray("field5").value(1).value(2).value(3).endArray()
                .endObject()
                .bytes(),
                XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("type")
            .setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        mapper = index.mapperService().documentMapper("type");

        writer.addDocument(doc.rootDoc());
        writer.addDocument(doc.rootDoc());

        IndexReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field1").fieldType().termQuery("value1", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field2").fieldType().termQuery("1", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field3").fieldType().termQuery("1.1", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field4").fieldType().termQuery("2010-01-01", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field5").fieldType().termQuery("1", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field5").fieldType().termQuery("2", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));

        topDocs = searcher.search(mapper.mappers().smartNameFieldMapper("field5").fieldType().termQuery("3", context), 10);
        assertThat(topDocs.totalHits, equalTo(2L));
        writer.close();
        reader.close();
        dir.close();
    }
}
