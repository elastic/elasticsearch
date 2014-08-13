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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;

@SuppressCodecs({ "Lucene3x", "Lucene40", "Lucene41", "Lucene42" }) // these codecs dont support missing values
public class TestReplaceMissing extends ElasticsearchLuceneTestCase {
    
    public void test() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setMergePolicy(newLogMergePolicy());
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        Document doc = new Document();
        doc.add(new SortedDocValuesField("field", new BytesRef("cat")));
        iw.addDocument(doc);
        
        doc = new Document();
        iw.addDocument(doc);
        
        doc = new Document();
        doc.add(new SortedDocValuesField("field", new BytesRef("dog")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();
        
        DirectoryReader reader = DirectoryReader.open(dir);
        AtomicReader ar = getOnlySegmentReader(reader);
        SortedDocValues raw = ar.getSortedDocValues("field");
        assertEquals(2, raw.getValueCount());
        
        // existing values
        SortedDocValues dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("cat"));
        assertEquals(2, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("dog", dv.lookupOrd(1).utf8ToString());
        
        assertEquals(0, dv.getOrd(0));
        assertEquals(0, dv.getOrd(1));
        assertEquals(1, dv.getOrd(2));
        
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("dog"));
        assertEquals(2, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("dog", dv.lookupOrd(1).utf8ToString());
        
        assertEquals(0, dv.getOrd(0));
        assertEquals(1, dv.getOrd(1));
        assertEquals(1, dv.getOrd(2));
        
        // non-existing values
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("apple"));
        assertEquals(3, dv.getValueCount());
        assertEquals("apple", dv.lookupOrd(0).utf8ToString());
        assertEquals("cat", dv.lookupOrd(1).utf8ToString());
        assertEquals("dog", dv.lookupOrd(2).utf8ToString());
        
        assertEquals(1, dv.getOrd(0));
        assertEquals(0, dv.getOrd(1));
        assertEquals(2, dv.getOrd(2));
        
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("company"));
        assertEquals(3, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("company", dv.lookupOrd(1).utf8ToString());
        assertEquals("dog", dv.lookupOrd(2).utf8ToString());
        
        assertEquals(0, dv.getOrd(0));
        assertEquals(1, dv.getOrd(1));
        assertEquals(2, dv.getOrd(2));
        
        dv = new BytesRefFieldComparatorSource.ReplaceMissing(raw, new BytesRef("ebay"));
        assertEquals(3, dv.getValueCount());
        assertEquals("cat", dv.lookupOrd(0).utf8ToString());
        assertEquals("dog", dv.lookupOrd(1).utf8ToString());
        assertEquals("ebay", dv.lookupOrd(2).utf8ToString());
        
        assertEquals(0, dv.getOrd(0));
        assertEquals(2, dv.getOrd(1));
        assertEquals(1, dv.getOrd(2));
        
        reader.close();
        dir.close();
    }
}
