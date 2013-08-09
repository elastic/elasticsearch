/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
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

package org.elasticsearch.discovery.ec2;


import org.elasticsearch.node.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Just an empty Node Start test to check eveything if fine when
 * starting.
 * This test is marked as ignored.
 * If you want to run your own test, please modify first test/resources/elasticsearch.yml file
 * with your own AWS credentials
 */
public class Ec2DiscoveryITest {

    @Test @Ignore
    public void testStart() {
        NodeBuilder.nodeBuilder().node();
    }

}
