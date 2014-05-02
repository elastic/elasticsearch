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
package org.elasticsearch.common;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;

/**
 *
 */
public class UUIDTests extends ElasticsearchTestCase {

    static UUIDGenerator timeUUIDGen = new TimeBasedUUID();
    static UUIDGenerator randomUUIDGen = new RandomBasedUUID();

    @Test
    public void testRandomUUID(){
        int iterations = 10000;
        HashSet<String> uuidSet = generateUUIDSet(iterations, randomUUIDGen);
        assertEquals(iterations,uuidSet.size());
    }

    @Test
    public void testTimeUUID(){
        int count = 100000;
        HashSet uuidSet = generateUUIDSet(count, timeUUIDGen);
        assertEquals(count,uuidSet.size());
    }


    @Test
    public void testThreadedTimeUUID(){
        testUUIDThreaded(timeUUIDGen);
    }

    @Test
    public void testThreadedRandomUUID(){
        testUUIDThreaded(randomUUIDGen);
    }


    HashSet generateUUIDSet(int count, UUIDGenerator uuidSource){
        HashSet<String> uuidSet = new HashSet<>();
        for (int i = 0; i < count; ++i){
            String timeUUID = uuidSource.getBase64UUID();
            if(uuidSet.contains(timeUUID)){
                assertFalse(uuidSet.contains(timeUUID));
            }
            uuidSet.add(timeUUID);
        }
        return uuidSet;
    }

    class UUIDGenRunner implements Runnable{
        int count;
        public HashSet uuidSet = null;
        UUIDGenerator uuidSource;


        public UUIDGenRunner(int count, UUIDGenerator uuidSource){
            this.count = count;
            this.uuidSource = uuidSource;
        }

        @Override
        public void run(){
            uuidSet = generateUUIDSet(count, uuidSource );
            assertEquals(count,uuidSet.size());
        }
    }

    public void testUUIDThreaded(UUIDGenerator uuidSource){
        HashSet<UUIDGenRunner> runners = new HashSet<>();
        HashSet<Thread> threads = new HashSet<>();
        int count = 100;
        int uuids = 10000;
        for (int i = 0; i < count; ++i){
            UUIDGenRunner runner = new UUIDGenRunner(uuids, uuidSource);
            Thread t = new Thread(runner);
            threads.add(t);
            runners.add(runner);
        }
        for (Thread t : threads){
            t.start();
        }
        boolean retry = false;
        do {
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException ie) {
                    retry = true;
                }
            }
        } while (retry);

        HashSet<String> globalSet = new HashSet<>();
        for( UUIDGenRunner runner : runners ){
            assertEquals(uuids,runner.uuidSet.size());
            globalSet.addAll(runner.uuidSet);
        }
        assertEquals(count*uuids,globalSet.size());
    }

    @Test
    public void testUUIDIntegrity(){
        long lastSeq = 0;
        long lastTime = 0;
        byte[] mungedAddress = null;
        try {
            for (int i = 0; i < 10000; ++i) {
                String uuid = timeUUIDGen.getBase64UUID();
                long sequence = TimeBasedUUID.getSequenceNumberFromUUID(uuid);
                long time = TimeBasedUUID.getTimestampFromUUID(uuid);
                byte[] address = TimeBasedUUID.getAddressBytesFromUUID(uuid);
                if (mungedAddress != null) {
                    assertEquals(lastSeq+1,sequence);
                    assertTrue(time-lastTime<10000);
                    assertArrayEquals(mungedAddress,address);
                }
                lastSeq = sequence;
                lastTime = time;
                mungedAddress = address;
            }
        } catch (IOException ie){
            assertFalse(ie!=null);
        }
    }


}
