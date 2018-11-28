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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class Zen2IT extends ESIntegTestCase {
    public void testNonMasterNodeDoesNotDisruptElections() throws IOException {
        // Non-master nodes cannot publish cluster states (they have no STATE channels) but can theoretically win elections, which disrupts
        // the cluster and prevents master-eligible nodes from winning their elections, repeatedly.
        internalCluster().startNodes(2);
        internalCluster().startDataOnlyNode();
        internalCluster().stopCurrentMasterNode();
        // There's only one master-eligible node left so it should elect itself pretty quickly.
        assertFalse(client().admin().cluster().prepareHealth().setTimeout(TimeValue.timeValueSeconds(10)).get().isTimedOut());
    }
}
