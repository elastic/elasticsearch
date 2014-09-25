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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import org.elasticsearch.common.network.MulticastChannel;
import org.elasticsearch.test.hamcrest.RegexMatcher;
import org.elasticsearch.tribe.TribeTests;

import java.util.regex.Pattern;

/**
 * Simple thread filter for randomized runner
 * This filter rejectes all threads that are known to leak across
 * tests / suites ie. the global test cluster threads etc.
 * It will cause threads leaking from threadpools / executors in unittests
 * to fail the test.
 */
public final class ElasticsearchThreadFilter implements ThreadFilter {

    private final Pattern nodePrefix = Pattern.compile("\\[(" +
            "(" + Pattern.quote(InternalTestCluster.TRANSPORT_CLIENT_PREFIX) + ")?(" +
            Pattern.quote(ElasticsearchIntegrationTest.GLOBAL_CLUSTER_NODE_PREFIX) + "|" +
            Pattern.quote(ElasticsearchIntegrationTest.SUITE_CLUSTER_NODE_PREFIX) + "|" +
            Pattern.quote(ElasticsearchIntegrationTest.TEST_CLUSTER_NODE_PREFIX) + "|" +
            Pattern.quote(TribeTests.SECOND_CLUSTER_NODE_PREFIX) + ")"
            + ")\\d+\\]");

    @Override
    public boolean reject(Thread t) {
        String threadName = t.getName();

        if (threadName.contains("[" + MulticastChannel.SHARED_CHANNEL_NAME + "]")
                || threadName.contains("[" + ElasticsearchSingleNodeTest.nodeName() + "]")
                || threadName.contains("Keep-Alive-Timer")) {
            return true;
        }
        return nodePrefix.matcher(t.getName()).find();
    }
}