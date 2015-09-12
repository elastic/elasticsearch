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

package org.elasticsearch.benchmark.monitor.process;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.monitor.process.ProcessProbe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

@SuppressForbidden(reason = "use of om.sun.management.ThreadMXBean to compare performance")
public class ProcessProbeBenchmark {

    private static final int ITERATIONS = 100_000;

    public static void main(String[] args) {
        System.setProperty("es.logger.prefix", "");
        final ESLogger logger = ESLoggerFactory.getLogger("benchmark");

        logger.info("--> loading process probe");
        ProcessProbe probe = ProcessProbe.getInstance();

        logger.info("--> warming up...");
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getOpenFileDescriptorCount();
            probe.getMaxFileDescriptorCount();
            probe.getTotalVirtualMemorySize();
            probe.getProcessCpuPercent();
            probe.getProcessCpuTotalTime();
        }
        logger.info("--> warmed up");




        logger.info("--> testing 'getOpenFileDescriptorCount' method...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getOpenFileDescriptorCount();
        }
        long elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getMaxFileDescriptorCount' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getMaxFileDescriptorCount();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getTotalVirtualMemorySize' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getTotalVirtualMemorySize();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getProcessCpuPercent' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getProcessCpuPercent();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));

        logger.info("--> testing 'getProcessCpuTotalTime' method...");
        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            probe.getProcessCpuTotalTime();
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> total [{}] ms, avg [{}] ms", elapsed, (elapsed / (double)ITERATIONS));




        logger.info("--> calculating process CPU user time with 'getAllThreadIds + getThreadUserTime' methods...");
        final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        final long[] threadIds = threadMxBean.getAllThreadIds();
        long sum = 0;

        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            for (long threadId : threadIds) {
                sum += threadMxBean.getThreadUserTime(threadId);
            }
        }
        elapsed = System.currentTimeMillis() - start;
        logger.info("--> execution time [total: {} ms, avg: {} ms] for {} iterations with average result of {}",
                elapsed, (elapsed / (double)ITERATIONS), ITERATIONS, (sum / (double)ITERATIONS));

        if (threadMxBean instanceof com.sun.management.ThreadMXBean) {
            logger.info("--> calculating process CPU user time with 'getAllThreadIds + getThreadUserTime(long[])' methods...");
            final com.sun.management.ThreadMXBean threadMxBean2 = (com.sun.management.ThreadMXBean)threadMxBean;
            sum = 0;

            start = System.currentTimeMillis();
            for (int i = 0; i < ITERATIONS; i++) {
                long[] user = threadMxBean2.getThreadUserTime(threadIds);
                for (int n = 0 ; n != threadIds.length; ++n) {
                    sum += user[n];
                }
            }
            elapsed = System.currentTimeMillis() - start;
            logger.info("--> execution time [total: {} ms, avg: {} ms] for {} iterations with average result of {}",
                    elapsed, (elapsed / (double)ITERATIONS), ITERATIONS, (sum / (double)ITERATIONS));

        }
    }
}
