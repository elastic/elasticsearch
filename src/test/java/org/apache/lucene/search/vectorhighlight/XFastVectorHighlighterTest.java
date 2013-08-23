package org.apache.lucene.search.vectorhighlight;

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.integration.ElasticsearchLuceneTestCase;
import org.junit.Test;

import java.io.IOException;


public class XFastVectorHighlighterTest extends ElasticsearchLuceneTestCase {

  @Test
  public void testLotsOfPhrases() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,  new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET)));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    String[] terms = { "org", "apache", "lucene"};
    int iters = atLeast(1000);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < iters; i++) {
      builder.append(terms[random().nextInt(terms.length)]).append(" ");
      if (random().nextInt(6) == 3) {
        builder.append("elasticsearch").append(" ");
      }
    }
      Document doc = new Document();
      Field field = new Field("field", builder.toString(), type);
      doc.add(field);
      writer.addDocument(doc);
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "org"));
    query.add(new Term("field", "apache"));
    query.add(new Term("field", "lucene"));
    
   
    XFastVectorHighlighter highlighter = new XFastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer, true);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(1, hits.totalHits);
    XFieldQuery fieldQuery  = highlighter.getFieldQuery(query, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[0].doc, "field", 1000, 1);
    for (int i = 0; i < bestFragments.length; i++) {
      String result = bestFragments[i].replaceAll("<b>org apache lucene</b>", "FOOBAR");
      assertFalse(result.contains("org apache lucene"));
    }
    reader.close();
    writer.close();
    dir.close();
  }
}
