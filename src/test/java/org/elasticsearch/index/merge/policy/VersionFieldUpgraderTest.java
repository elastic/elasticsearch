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

package org.elasticsearch.index.merge.policy;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;

/** Tests upgrading old document versions from _uid payloads to _version docvalues */
public class VersionFieldUpgraderTest extends ElasticsearchTestCase  {
    
    /** Simple test: one doc in the old format, check that it looks correct */
    public void testUpgradeOneDocument() throws Exception {
        Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
        
        // add a document with a _uid having a payload of 3
        Document doc = new Document();
        Token token = new Token("1", 0, 1);
        token.setPayload(new BytesRef(Numbers.longToBytes(3)));
        doc.add(new TextField(UidFieldMapper.NAME, new CannedTokenStream(token)));
        iw.addDocument(doc);
        iw.commit();
        
        CodecReader reader = getOnlySegmentReader(DirectoryReader.open(iw, true));
        CodecReader upgraded = VersionFieldUpgrader.wrap(reader);
        // we need to be upgraded, should be a different instance
        assertNotSame(reader, upgraded);
        
        // make sure we can see our numericdocvalues in fieldinfos
        FieldInfo versionField = upgraded.getFieldInfos().fieldInfo(VersionFieldMapper.NAME);
        assertNotNull(versionField);
        assertEquals(DocValuesType.NUMERIC, versionField.getDocValuesType());
        // should have a value of 3, and be visible in docsWithField
        assertEquals(3, upgraded.getNumericDocValues(VersionFieldMapper.NAME).get(0));
        assertTrue(upgraded.getDocsWithField(VersionFieldMapper.NAME).get(0));
        
        // verify filterreader with checkindex
        TestUtil.checkReader(upgraded);
        
        reader.close();
        iw.close();
        dir.close();
    }
    
    /** test that we are a non-op if the segment already has the version field */
    public void testAlreadyUpgraded() throws Exception {
        Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
        
        // add a document with a _uid having a payload of 3
        Document doc = new Document();
        Token token = new Token("1", 0, 1);
        token.setPayload(new BytesRef(Numbers.longToBytes(3)));
        doc.add(new TextField(UidFieldMapper.NAME, new CannedTokenStream(token)));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 3));
        iw.addDocument(doc);
        iw.commit();
        
        CodecReader reader = getOnlySegmentReader(DirectoryReader.open(iw, true));
        CodecReader upgraded = VersionFieldUpgrader.wrap(reader);
        // we already upgraded: should be same instance
        assertSame(reader, upgraded);
        
        reader.close();
        iw.close();
        dir.close();
    }
    
    /** Test upgrading two documents */
    public void testUpgradeTwoDocuments() throws Exception {
        Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
        
        // add a document with a _uid having a payload of 3
        Document doc = new Document();
        Token token = new Token("1", 0, 1);
        token.setPayload(new BytesRef(Numbers.longToBytes(3)));
        doc.add(new TextField(UidFieldMapper.NAME, new CannedTokenStream(token)));
        iw.addDocument(doc);
        
        doc = new Document();
        token = new Token("2", 0, 1);
        token.setPayload(new BytesRef(Numbers.longToBytes(4)));
        doc.add(new TextField(UidFieldMapper.NAME, new CannedTokenStream(token)));
        iw.addDocument(doc);

        iw.commit();
        
        CodecReader reader = getOnlySegmentReader(DirectoryReader.open(iw, true));
        CodecReader upgraded = VersionFieldUpgrader.wrap(reader);
        // we need to be upgraded, should be a different instance
        assertNotSame(reader, upgraded);
        
        // make sure we can see our numericdocvalues in fieldinfos
        FieldInfo versionField = upgraded.getFieldInfos().fieldInfo(VersionFieldMapper.NAME);
        assertNotNull(versionField);
        assertEquals(DocValuesType.NUMERIC, versionField.getDocValuesType());
        // should have a values of 3 and 4, and be visible in docsWithField
        assertEquals(3, upgraded.getNumericDocValues(VersionFieldMapper.NAME).get(0));
        assertEquals(4, upgraded.getNumericDocValues(VersionFieldMapper.NAME).get(1));
        assertTrue(upgraded.getDocsWithField(VersionFieldMapper.NAME).get(0));
        assertTrue(upgraded.getDocsWithField(VersionFieldMapper.NAME).get(1));
        
        // verify filterreader with checkindex
        TestUtil.checkReader(upgraded);
        
        reader.close();
        iw.close();
        dir.close();
    }
}
