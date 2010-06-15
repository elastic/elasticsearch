/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.deps.lucene;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.IndexWriters;
import org.elasticsearch.common.lucene.Lucene;
import org.testng.annotations.Test;

import static org.elasticsearch.common.lucene.DocumentBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class IndexWriterNoBufferTests {

    @Test public void testDefaultBuffer() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);

        for (int i = 0; i < 100; i++) {
            // TODO (just setting the boost value does not seem to work...)
            StringBuilder value = new StringBuilder().append("value");
            for (int j = 0; j < i; j++) {
                value.append(" ").append("value");
            }
            indexWriter.addDocument(doc()
                    .add(field("id", Integer.toString(i)))
                    .add(field("value", value.toString()))
                    .boost(i).build());
        }

        assertThat(IndexWriters.estimateRamSize(indexWriter), greaterThan(0l));

        indexWriter.close();
    }

    @Test public void testNoBuffer() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);
        indexWriter.setMaxBufferedDocs(2);
        indexWriter.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);

        for (int i = 0; i < 100; i++) {
            // TODO (just setting the boost value does not seem to work...)
            StringBuilder value = new StringBuilder().append("value");
            for (int j = 0; j < i; j++) {
                value.append(" ").append("value");
            }
            indexWriter.addDocument(doc()
                    .add(field("id", Integer.toString(i)))
                    .add(field("value", value.toString()))
                    .boost(i).build());
        }

        assertThat(IndexWriters.estimateRamSize(indexWriter), equalTo(0l));

        indexWriter.close();
    }
}
