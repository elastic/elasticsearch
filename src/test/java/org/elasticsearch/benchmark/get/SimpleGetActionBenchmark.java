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

package org.elasticsearch.benchmark.get;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

// simple test for embedded / single remote lookup
public class SimpleGetActionBenchmark {

    public static void main(String[] args) {
        long OPERATIONS = SizeValue.parseSizeValue("300k").singles();

        Node node = NodeBuilder.nodeBuilder().node();

        Client client;
        if (false) {
            client = NodeBuilder.nodeBuilder().client(true).node().client();
        } else {
            client = node.client();
        }

        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        StopWatch stopWatch = new StopWatch().start();
        for (long i = 0; i < OPERATIONS; i++) {
            client.prepareGet("test", "type1", "1").execute().actionGet();
        }
        stopWatch.stop();

        System.out.println("Ran in " + stopWatch.totalTime() + ", per second: " + (((double) OPERATIONS) / stopWatch.totalTime().secondsFrac()));

        node.close();
    }
}