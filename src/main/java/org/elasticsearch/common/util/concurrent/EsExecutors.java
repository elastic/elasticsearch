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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.*;

/**
 * @author kimchy (shay.banon)
 */
public class EsExecutors {

    public static ExecutorService newCachedThreadPool(TimeValue keepAlive, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                keepAlive.millis(), TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                threadFactory);
    }

    public static String threadName(Settings settings, String namePrefix) {
        String name = settings.get("name");
        if (name == null) {
            name = "elasticsearch";
        } else {
            name = "elasticsearch[" + name + "]";
        }
        return name + namePrefix;
    }

    public static ThreadFactory daemonThreadFactory(Settings settings, String namePrefix) {
        return daemonThreadFactory(threadName(settings, namePrefix));
    }

    /**
     * A priority based thread factory, for all Thread priority constants:
     * <tt>Thread.MIN_PRIORITY, Thread.NORM_PRIORITY, Thread.MAX_PRIORITY</tt>;
     * <p/>
     * This factory is used instead of Executers.DefaultThreadFactory to allow
     * manipulation of priority and thread owner name.
     *
     * @param namePrefix a name prefix for this thread
     * @return a thread factory based on given priority.
     */
    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        final ThreadFactory f = java.util.concurrent.Executors.defaultThreadFactory();
        final String o = namePrefix + "-";

        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = f.newThread(r);

                /*
                 * Thread name: owner-pool-N-thread-M, where N is the sequence
                 * number of this factory, and M is the sequence number of the
                 * thread created by this factory.
                 */
                t.setName(o + t.getName());

                /* override default definition t.setDaemon(false); */
                t.setDaemon(true);

                return t;
            }
        };
    }

    /**
     * Cannot instantiate.
     */
    private EsExecutors() {
    }
}
