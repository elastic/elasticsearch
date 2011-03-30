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
import org.elasticsearch.common.trove.map.hash.THashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.unit.SizeValue;

import java.util.HashMap;

public class StringMapAdjustOrPutBenchmark {

    public static void main(String[] args) {

        int NUMBER_OF_KEYS = (int) SizeValue.parseSizeValue("200k").singles();
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

        System.out.println("TObjectIntHashMap: TP (seconds) " + ITERATIONS / stopWatch.stop().totalTime().secondsFrac());

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
        System.out.println("THashMap: TP (seconds) " + ITERATIONS / stopWatch.stop().totalTime().secondsFrac());

        tMap.clear();
        tMap = null;

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
        System.out.println("HashMap: TP (seconds) " + ITERATIONS / stopWatch.stop().totalTime().secondsFrac());

    }


    static class StringEntry {
        String key;
        int counter;

        StringEntry(String key, int counter) {
            this.key = key;
            this.counter = counter;
        }
    }
}