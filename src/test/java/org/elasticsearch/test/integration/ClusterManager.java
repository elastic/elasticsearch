/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.test.integration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterManager {

    private static final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private static TestCluster cluster;
    private static int generation = 0;

    public synchronized static TestCluster accquireCluster() {
        if (cluster == null) {
            cluster = new TestCluster(generation++);
        }
        TestCluster c = cluster;
        if (!c.tryAccquire()) {
            c = new TestCluster(generation++);
            boolean tryAccquire = c.tryAccquire();
            assert tryAccquire;
            cluster = c;
        }
        
        c.reset();
        return c;

    }
    
    public static synchronized void releaseCluster(final TestCluster toRelease) {
        toRelease.decrementReference();
        // TODO find a better way
//        service.schedule(new Runnable() {
//            @Override
//            public void run() {
//                toRelease.close();
//            }
//        }, 3, TimeUnit.MINUTES);
    }
}
