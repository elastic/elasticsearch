/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.auth;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.apache.lucene.index.ExtendedIndexSearcher;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.testng.annotations.Test;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author @timetabling
 */
@Test
public class AuthParseElementTests {

    @Test public void testParseAuth() throws Exception {
        byte[] bytes = jsonBuilder().startObject().field("login", "testlogin").field("password", "testpw").endObject().copiedBytes();
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        final IndexReader reader = IndexReader.open(indexWriter, true);
        
        SearchContext context = new SearchContext(0, new SearchShardTarget("nodeId", "index", 0), 
                SearchType.DEFAULT, 1, 0, TimeValue.timeValueHours(1), new String[]{}, new Searcher() {

            public IndexReader reader() {
                return reader;
            }

            public ExtendedIndexSearcher searcher() {
                return new ExtendedIndexSearcher(reader);
            }

            public boolean release() throws ElasticSearchException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, null, null, null);
        new AuthParseElement().parse(parser, context);
        assertThat(context.auth().login(), equalTo("testlogin"));
        assertThat(context.auth().password(), equalTo("testpw"));
    }
}
