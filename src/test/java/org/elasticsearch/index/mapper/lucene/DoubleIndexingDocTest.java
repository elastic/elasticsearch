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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeLuceneTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class DoubleIndexingDocTest extends ElasticsearchSingleNodeLuceneTestCase {

    @Test
    public void testDoubleIndexingSameDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random(), TEST_VERSION_CURRENT, Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", 1)
                .field("field3", 1.1)
                .field("field4", "2010-01-01")
                .startArray("field5").value(1).value(2).value(3).endArray()
                .endObject()
                .bytes());

        writer.addDocument(doc.rootDoc(), doc.analyzer());
        writer.addDocument(doc.rootDoc(), doc.analyzer());

        IndexReader reader = DirectoryReader.open(writer, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(mapper.mappers().smartName("field1").mapper().termQuery("value1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field2").mapper().termQuery("1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field3").mapper().termQuery("1.1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field4").mapper().termQuery("2010-01-01", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field5").mapper().termQuery("1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field5").mapper().termQuery("2", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field5").mapper().termQuery("3", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));
        writer.close();
        reader.close();
        dir.close();
    }
}
