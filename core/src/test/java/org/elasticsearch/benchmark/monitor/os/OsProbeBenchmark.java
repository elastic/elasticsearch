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

package org.elasticsearch.benchmark.monitor.os;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.monitor.os.OsProbe;

public class OsProbeBenchmark {

    private static final int ITERATIONS = 100_000;

    public static void main(String[] args) {
        System.setProperty("es.logger.prefix", "");
        final ESLogger logger = ESLoggerFactory.getLogger("benchmark");

        logger.info("--> loading OS probe");
        OsProbe probe = OsProbe.getInstance();

        logger.info("--> warming up...");
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getTotalPhysicalMemorySize();
            probe.getFreePhysicalMemorySize();
            probe.getTotalSwapSpaceSize();
            probe.getFreeSwapSpaceSize();
            probe.getSystemLoadAverage();
        }
        logger.info("--> warmed up");




        logger.info("--> testing 'getTotalPhysicalMemorySize' method...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getTotalPhysicalMemorySize();
        }
        long elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getFreePhysicalMemorySize' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getFreePhysicalMemorySize();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getTotalSwapSpaceSize' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getTotalSwapSpaceSize();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getFreeSwapSpaceSize' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getFreeSwapSpaceSize();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getSystemLoadAverage' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getSystemLoadAverage();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));
    }
}
