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

package org.elasticsearch.util.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.testng.annotations.Test;

import static org.elasticsearch.util.lucene.DocumentBuilder.*;
import static org.elasticsearch.util.lucene.IndexWriters.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexWritersTests {

    @Test public void testEstimateSize() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);
        indexWriter.commit();
        assertThat("Index is empty after creation and commit", estimateRamSize(indexWriter), equalTo(0l));


        indexWriter.addDocument(doc().add(field("_id", "1")).add(new NumericField("test", Field.Store.YES, true).setIntValue(2)).build());

        long size = estimateRamSize(indexWriter);
        assertThat("After indexing a small document, should be higher", size, greaterThan(100000l));

        indexWriter.deleteDocuments(new Term("_id", "1"));
        assertThat(estimateRamSize(indexWriter), greaterThan(size));

        indexWriter.commit();
        assertThat(estimateRamSize(indexWriter), equalTo(0l));
    }
}
