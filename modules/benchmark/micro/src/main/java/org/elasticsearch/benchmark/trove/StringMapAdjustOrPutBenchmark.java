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

package org.elasticsearch.benchmark.trove;

import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.trove.StringIdentityHashingStrategy;
import org.elasticsearch.common.trove.map.custom_hash.TObjectIntCustomHashMap;
import org.elasticsearch.common.trove.map.hash.THashMap;
import org.elasticsearch.common.trove.map.hash.TIntIntHashMap;
import org.elasticsearch.common.trove.map.hash.TIntObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.trove.strategy.IdentityHashingStrategy;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.jsr166y.ThreadLocalRandom;

import java.util.HashMap;
import java.util.IdentityHashMap;

public class StringMapAdjustOrPutBenchmark {

    public static void main(String[] args) {

        int NUMBER_OF_KEYS = (int) SizeValue.parseSizeValue("20").singles();
        int STRING_SIZE = 5;
        long PUT_OPERATIONS = SizeValue.parseSizeValue("5m").singles();
        long ITERATIONS = 10;
        boolean REUSE = true;


        String[] values = new String[NUMBER_OF_KEYS];
        for (int i = 0; i < values.length; i++) {
            values[i] = RandomStringGenerator.randomAlphabetic(STRING_SIZE);
        }

        StopWatch stopWatch;

        stopWatch = new StopWatch().start();
        TObjectIntHashMap<String> map = new TObjectIntHashMap<String>();
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                map.clear();
            } else {
                map = new TObjectIntHashMap<String>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                map.adjustOrPutValue(values[(int) (i % NUMBER_OF_KEYS)], 1, 1);
            }
        }
        map.clear();
        map = null;

        stopWatch.stop();
        System.out.println("TObjectIntHashMap: " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");

        stopWatch = new StopWatch().start();
        TObjectIntCustomHashMap<String> iMap = new TObjectIntCustomHashMap<String>(new StringIdentityHashingStrategy());
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                iMap.clear();
            } else {
                map = new TObjectIntHashMap<String>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                iMap = new TObjectIntCustomHashMap<String>(new StringIdentityHashingStrategy());
            }
        }
        stopWatch.stop();
        System.out.println("TObjectIntCustomHashMap(StringIdentity): " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");
        iMap.clear();
        iMap = null;

        stopWatch = new StopWatch().start();
        iMap = new TObjectIntCustomHashMap<String>(new IdentityHashingStrategy<String>());
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                iMap.clear();
            } else {
                map = new TObjectIntHashMap<String>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                iMap = new TObjectIntCustomHashMap<String>(new IdentityHashingStrategy<String>());
            }
        }
        stopWatch.stop();
        System.out.println("TObjectIntCustomHashMap(PureIdentity): " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");
        iMap.clear();
        iMap = null;

        // now test with THashMap
        stopWatch = new StopWatch().start();
        THashMap<String, StringEntry> tMap = new THashMap<String, StringEntry>();
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                tMap.clear();
            } else {
                tMap = new THashMap<String, StringEntry>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                String key = values[(int) (i % NUMBER_OF_KEYS)];
                StringEntry stringEntry = tMap.get(key);
                if (stringEntry == null) {
                    stringEntry = new StringEntry(key, 1);
                    tMap.put(key, stringEntry);
                } else {
                    stringEntry.counter++;
                }
            }
        }

        tMap.clear();
        tMap = null;

        stopWatch.stop();
        System.out.println("THashMap: " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");

        stopWatch = new StopWatch().start();
        HashMap<String, StringEntry> hMap = new HashMap<String, StringEntry>();
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                hMap.clear();
            } else {
                hMap = new HashMap<String, StringEntry>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                String key = values[(int) (i % NUMBER_OF_KEYS)];
                StringEntry stringEntry = hMap.get(key);
                if (stringEntry == null) {
                    stringEntry = new StringEntry(key, 1);
                    hMap.put(key, stringEntry);
                } else {
                    stringEntry.counter++;
                }
            }
        }

        hMap.clear();
        hMap = null;

        stopWatch.stop();
        System.out.println("HashMap: " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");


        stopWatch = new StopWatch().start();
        IdentityHashMap<String, StringEntry> ihMap = new IdentityHashMap<String, StringEntry>();
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                ihMap.clear();
            } else {
                hMap = new HashMap<String, StringEntry>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                String key = values[(int) (i % NUMBER_OF_KEYS)];
                StringEntry stringEntry = ihMap.get(key);
                if (stringEntry == null) {
                    stringEntry = new StringEntry(key, 1);
                    ihMap.put(key, stringEntry);
                } else {
                    stringEntry.counter++;
                }
            }
        }
        stopWatch.stop();
        System.out.println("IdentityHashMap: " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");

        ihMap.clear();
        ihMap = null;

        int[] iValues = new int[NUMBER_OF_KEYS];
        for (int i = 0; i < values.length; i++) {
            iValues[i] = ThreadLocalRandom.current().nextInt();
        }

        stopWatch = new StopWatch().start();
        TIntIntHashMap intMap = new TIntIntHashMap();
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                intMap.clear();
            } else {
                intMap = new TIntIntHashMap();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                int key = iValues[(int) (i % NUMBER_OF_KEYS)];
                intMap.adjustOrPutValue(key, 1, 1);
            }
        }
        stopWatch.stop();
        System.out.println("TIntIntHashMap: " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");

        intMap.clear();
        intMap = null;

        // now test with THashMap
        stopWatch = new StopWatch().start();
        TIntObjectHashMap<IntEntry> tIntMap = new TIntObjectHashMap<IntEntry>();
        for (long iter = 0; iter < ITERATIONS; iter++) {
            if (REUSE) {
                tIntMap.clear();
            } else {
                tIntMap = new TIntObjectHashMap<IntEntry>();
            }
            for (long i = 0; i < PUT_OPERATIONS; i++) {
                int key = iValues[(int) (i % NUMBER_OF_KEYS)];
                IntEntry intEntry = tIntMap.get(key);
                if (intEntry == null) {
                    intEntry = new IntEntry(key, 1);
                    tIntMap.put(key, intEntry);
                } else {
                    intEntry.counter++;
                }
            }
        }

        tIntMap.clear();
        tIntMap = null;

        stopWatch.stop();
        System.out.println("TIntObjectHashMap: " + stopWatch.totalTime() + ", " + stopWatch.totalTime().millisFrac() / ITERATIONS + "ms");
    }


    static class StringEntry {
        String key;
        int counter;

        StringEntry(String key, int counter) {
            this.key = key;
            this.counter = counter;
        }
    }

    static class IntEntry {
        int key;
        int counter;

        IntEntry(int key, int counter) {
            this.key = key;
            this.counter = counter;
        }
    }
}