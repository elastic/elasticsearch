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

package org.elasticsearch.index.analysis;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

public class ByteStreamDuplicateSequenceSpotterTests extends ElasticsearchTestCase {
    
    @Test
    public void testSimpleStreamNoDups()  {
        int bufferSize=10000;
        ByteStreamDuplicateSequenceSpotter spotter=new ByteStreamDuplicateSequenceSpotter(bufferSize);        
        for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
            int len=spotter.addByte(b);
            assertEquals(0, len);
        }
    } 
    
    @Test
    public void testSimpleWrappingStreamNoDups()  {
        //buffer smaller than the set of bytes added
        int bufferSize=100;
        ByteStreamDuplicateSequenceSpotter spotter=new ByteStreamDuplicateSequenceSpotter(bufferSize);        
        for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
            int len=spotter.addByte(b);
            assertEquals(0, len);
        }
    }   
    
    
    @Test
    public void testSimpleStreamWithDups()  {
        int bufferSize=10000;
        ByteStreamDuplicateSequenceSpotter spotter=new ByteStreamDuplicateSequenceSpotter(bufferSize);        
        for (int loopNum = 0; loopNum < 3; loopNum++) {
            byte expectedChainLength=loopNum==0?(byte)0:(byte)1; 
            spotter.reset();
            for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
                byte len=spotter.addByte(b);
                if(loopNum==0){
                    assertEquals(0, len);                    
                }else{
                    assertEquals(expectedChainLength, len);                                        
                }
                if(expectedChainLength<Byte.MAX_VALUE){
                    expectedChainLength++;
                }
            }            
        }
    }       

    @Test
    public void testWrappedStreamWithDups()  {
        int bufferSize=500;
        ByteStreamDuplicateSequenceSpotter spotter=new ByteStreamDuplicateSequenceSpotter(bufferSize);        
        for (int loopNum = 0; loopNum < 3; loopNum++) {
            byte expectedChainLength=loopNum==0?(byte)0:(byte)1; 
            spotter.reset();
            for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
                byte len=spotter.addByte(b);
                if(loopNum==0){
                    assertEquals(0, len);                    
                }else{
                    assertEquals(expectedChainLength, len);                                        
                }
                if(expectedChainLength<Byte.MAX_VALUE){
                    expectedChainLength++;
                }
            }            
        }
    }       
    
    @Test
    public void testStreamWithNovelAndDups()  {
        int bufferSize=10000;        
        ByteStreamDuplicateSequenceSpotter spotter=new ByteStreamDuplicateSequenceSpotter(bufferSize);
        testByteAdditions(spotter, new byte[]{1, 2,3},new byte[]{0,0,0});        
        testByteAdditions(spotter, new byte[]{3, 5, 2},new byte[]{1,0,1});        
        testByteAdditions(spotter, new byte[]{3, 4, 5},new byte[]{2,0,1});
        testByteAdditions(spotter, new byte[]{1, 2},new byte[]{1,2});
        spotter.reset();
        testByteAdditions(spotter, new byte[]{3, 4},new byte[]{1,2});
    }     

    private void testByteAdditions(ByteStreamDuplicateSequenceSpotter spotter, byte[] additions, byte[] expectedLengths) {
        for (int i = 0; i < additions.length; i++) {
            byte reportedChainLength=spotter.addByte(additions[i]);
            assertEquals(expectedLengths[i], reportedChainLength);                                        
        }
    }
    
}
