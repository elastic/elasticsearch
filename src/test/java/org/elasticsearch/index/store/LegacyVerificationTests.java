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

package org.elasticsearch.index.store;

import java.nio.charset.StandardCharsets;
import java.util.zip.Adler32;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ElasticsearchTestCase;

/** 
 * Simple tests for LegacyVerification (old segments)
 * @deprecated remove this test when support for lucene 4.x 
 *             segments is not longer needed. 
 */
@Deprecated
public class LegacyVerificationTests extends ElasticsearchTestCase {
    
    public void testAdler32() throws Exception {
        Adler32 expected = new Adler32();
        byte bytes[] = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        expected.update(bytes);
        String expectedString = Store.digestToString(expected.getValue());
        
        Directory dir = newDirectory();
        
        IndexOutput o = dir.createOutput("legacy", IOContext.DEFAULT);
        VerifyingIndexOutput out = new LegacyVerification.Adler32VerifyingIndexOutput(o, expectedString, 8);
        out.writeBytes(bytes, 0, bytes.length);
        out.verify();
        out.close();
        out.verify();
        
        dir.close();
    }
    
    public void testAdler32Corrupt() throws Exception {
        Adler32 expected = new Adler32();
        byte bytes[] = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        expected.update(bytes);
        String expectedString = Store.digestToString(expected.getValue());
        
        byte corruptBytes[] = "abcdefch".getBytes(StandardCharsets.UTF_8);
        Directory dir = newDirectory();
        
        IndexOutput o = dir.createOutput("legacy", IOContext.DEFAULT);
        VerifyingIndexOutput out = new LegacyVerification.Adler32VerifyingIndexOutput(o, expectedString, 8);
        out.writeBytes(corruptBytes, 0, bytes.length);
        try {
            out.verify();
            fail();
        } catch (CorruptIndexException e) {
            // expected exception
        }
        out.close();
        
        try {
            out.verify();
            fail();
        } catch (CorruptIndexException e) {
            // expected exception
        }
        
        dir.close();
    }
    
    public void testLengthOnlyOneByte() throws Exception {
        Directory dir = newDirectory();
        
        IndexOutput o = dir.createOutput("oneByte", IOContext.DEFAULT);
        VerifyingIndexOutput out = new LegacyVerification.LengthVerifyingIndexOutput(o, 1);
        out.writeByte((byte) 3);
        out.verify();
        out.close();
        out.verify();
        
        dir.close();
    }
    
    public void testLengthOnlyCorrupt() throws Exception {
        Directory dir = newDirectory();
        
        IndexOutput o = dir.createOutput("oneByte", IOContext.DEFAULT);
        VerifyingIndexOutput out = new LegacyVerification.LengthVerifyingIndexOutput(o, 2);
        out.writeByte((byte) 3);
        try {
            out.verify();
            fail();
        } catch (CorruptIndexException expected) {
            // expected exception
        }
        
        out.close();
        
        try {
            out.verify();
            fail();
        } catch (CorruptIndexException expected) {
            // expected exception
        }
        
        dir.close();
    }
}
