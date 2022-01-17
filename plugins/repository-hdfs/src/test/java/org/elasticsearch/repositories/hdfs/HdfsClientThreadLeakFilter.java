/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    private static final String OFFENDING_THREAD_NAME = "org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner";

    @Override
    public boolean reject(Thread t) {
        return t.getName().equals(OFFENDING_THREAD_NAME);
    }
}
