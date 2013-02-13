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

package org.elasticsearch.test.unit.deps.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class LuceneFieldCacheTests {

    /**
     * A test that verifies that when using FieldCache for a field that has been added twice (under the same name)
     * to the document, returns the last one.
     */
    @Test
    public void testTwoFieldSameNameNumericFieldCache() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        IntField field = new IntField("int1", 1, IntField.TYPE_NOT_STORED);
        doc.add(field);

        field = new IntField("int1", 2, IntField.TYPE_NOT_STORED);
        doc.add(field);

        indexWriter.addDocument(doc);

        AtomicReader reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(indexWriter, true));
        int[] ints = FieldCache.DEFAULT.getInts(reader, "int1", false);
        assertThat(ints.length, equalTo(1));
        assertThat(ints[0], equalTo(2));
    }
}
