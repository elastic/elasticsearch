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

package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;

import java.io.IOException;
import java.util.Arrays;

/** Simple tests for IncludeNestedDocsQuery */
public class IncludeNestedDocsQueryTests extends ElasticsearchLuceneTestCase  {
    
    public void testAcceptDocs() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        
        StringField parentID = new StringField("id", "", Field.Store.YES);
        StringField childID = new StringField("id", "", Field.Store.YES);
        
        Document parent = new Document();
        parent.add(parentID);
        parent.add(new StringField("type", "parent", Field.Store.NO));
        
        Document child = new Document();
        child.add(childID);
        child.add(new StringField("type", "child", Field.Store.NO));
        
        // 6 docs, 2 parent-child pairs
        childID.setStringValue("0");
        parentID.setStringValue("1");
        iw.addDocuments(Arrays.asList(child, parent));
        
        childID.setStringValue("2");
        parentID.setStringValue("3");
        iw.addDocuments(Arrays.asList(child, parent));
        
        childID.setStringValue("4");
        parentID.setStringValue("5");
        iw.addDocuments(Arrays.asList(child, parent));
        
        DirectoryReader ir = DirectoryReader.open(iw, true);
        assertEquals(6, ir.numDocs());
        ir.close();
        
        // construct our parents filter, it might wrap fixedbitset with deletes
        Filter parentsFilter = new QueryWrapperFilter(new TermQuery(new Term("type", "parent"))) {
            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                DocIdSet actual = super.getDocIdSet(context, null);
                // fill a fixedbitset
                FixedBitSet fbs = new FixedBitSet(context.reader().maxDoc());
                fbs.or(actual.iterator());
                DocIdSet ret = BitsFilteredDocIdSet.wrap(fbs, acceptDocs);
                return ret;
            }
        };
        
        // now delete first nested doc
        Query deletion = new TermQuery(new Term("id", "1"));
        iw.deleteDocuments(new IncludeNestedDocsQuery(deletion, parentsFilter));

        ir = DirectoryReader.open(iw, true);
        IndexSearcher is = new IndexSearcher(ir);
        assertEquals(4, ir.numDocs());
        
        // now delete second nested doc
        deletion = new TermQuery(new Term("id", "3"));
        TopDocs td = is.search(new IncludeNestedDocsQuery(deletion, parentsFilter), 5, Sort.INDEXORDER);
        assertEquals(2, td.totalHits);
        
        ir.close();
        iw.close();
        dir.close();
    }
}
