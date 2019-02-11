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

package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * In Hadoop 2.8.0, there is a thread that is started by the filesystem to clean up old execution stats.
 * This thread ignores all interrupts, catching InterruptedException, logging it, and continuing on
 * with its work. The thread is a daemon, so it thankfully does not stop the JVM from closing, and it
 * is started only once in a class's static initialization. This currently breaks our testing as this
 * thread leaks out of the client and is picked up by the test framework. This thread filter is meant
 * to ignore the offending thread until a version of Hadoop is released that addresses the incorrect
 * interrupt handling.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-12829">https://issues.apache.org/jira/browse/HADOOP-12829</a>
 * @see "org.apache.hadoop.fs.FileSystem.Statistics.StatisticsDataReferenceCleaner"
 * @see "org.apache.hadoop.fs.FileSystem.Statistics"
 */
public final class HdfsClientThreadLeakFilter implements ThreadFilter {

    private static final String OFFENDING_THREAD_NAME =
        "org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner";

    @Override
    public boolean reject(Thread t) {
        return t.getName().equals(OFFENDING_THREAD_NAME);
    }
}
