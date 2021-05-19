/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.http.HttpHost;
import static org.junit.Assert.assertEquals;

public class NodePriorityStrategyTests extends RestClientTestCase {
    public void testNoPriority() {
        List<Node> nodes = new ArrayList<>();
        int size = between(2, 5);
        for (int i = 0; i < size; i++) {
            nodes.add(dummyNode(randomBoolean(), randomBoolean(), randomBoolean()));
        }
        List<Node> expected = new ArrayList<>(nodes);
        List<List<Node>> nodeGroups = NodePriorityStrategy.NO_PRIORITY.groupByPriority(nodes);
        assertEquals(1, nodeGroups.size());
        assertEquals(expected, nodeGroups.get(0));
    }

    private static Node dummyNode(boolean master, boolean data, boolean ingest){
        return dummyNode(master, data, ingest, false, false, false, false, false);
    }
    private static Node dummyNode(boolean master, boolean data, boolean ingest,
                                  boolean dataContent, boolean dataHot, boolean dataWarm, boolean dataCold, boolean dataFrozen) {
        final Set<String> roles = new TreeSet<>();
        if (master) {
            roles.add("master");
        }
        if (data) {
            roles.add("data");
        }
        if (dataContent) {
            roles.add("data_content");
        }
        if (dataHot) {
            roles.add("data_hot");
        }
        if (dataWarm) {
            roles.add("data_warm");
        }
        if (dataCold) {
            roles.add("data_cold");
        }
        if (dataFrozen) {
            roles.add("data_frozen");
        }
        if (ingest) {
            roles.add("ingest");
        }
        return new Node(new HttpHost("dummy"), Collections.<HttpHost>emptySet(),
            randomAsciiAlphanumOfLength(5), randomAsciiAlphanumOfLength(5),
            new Node.Roles(roles),
            Collections.<String, List<String>>emptyMap());
    }
}
