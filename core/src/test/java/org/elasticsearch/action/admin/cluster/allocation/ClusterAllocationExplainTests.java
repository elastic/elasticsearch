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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.ESSingleNodeTestCase;


/**
 * Tests for the cluster allocation explanation
 */
public final class ClusterAllocationExplainTests extends ESSingleNodeTestCase {

    //NORELEASE TODO: fix these or just use the IT tests?
    public void testShardExplain() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings("index.number_of_shards", 1, "index.number_of_replicas", 1).get();
        ClusterAllocationExplainResponse resp = client().admin().cluster().prepareAllocationExplain()
                .setIndex("test").setShard(0).setPrimary(false).get();

        ClusterAllocationExplanation cae = resp.getExplanation();
        assertNotNull("should always have an explanation", cae);
        assertEquals("test", cae.getShard().getIndexName());
        assertEquals(0, cae.getShard().getId());
        assertEquals(false, cae.isPrimary());
        assertNull(cae.getShardAllocationDecision().getAllocateDecision().getTargetNode());
        assertNotEquals(AllocationDecision.FETCH_PENDING, cae.getShardAllocationDecision().getAllocateDecision().getAllocationDecision());
        assertNotNull(cae.getUnassignedInfo());
        NodeAllocationResult explanation = cae.getShardAllocationDecision().getAllocateDecision().getNodeDecisions().iterator().next();
        Decision d = explanation.getCanAllocateDecision();
        assertNotNull("should have a decision", d);
        assertEquals(Decision.Type.NO, d.type());
        assertEquals(AllocationDecision.NO, explanation.getNodeDecision());
        assertTrue(d.toString(), d.toString().contains("NO(the shard cannot be allocated to the same node"));
        assertTrue(d instanceof Decision.Multi);
        Decision.Multi md = (Decision.Multi) d;
        Decision ssd = md.getDecisions().get(0);
        assertEquals(Decision.Type.NO, ssd.type());
        assertTrue(ssd.toString(), ssd.toString().contains("NO(the shard cannot be allocated to the same node"));

        resp = client().admin().cluster().prepareAllocationExplain().setIndex("test").setShard(0).setPrimary(true).get();

        cae = resp.getExplanation();
        assertNotNull("should always have an explanation", cae);
        assertEquals("test", cae.getShard().getIndexName());
        assertEquals(0, cae.getShard().getId());
        assertEquals(true, cae.isPrimary());
        assertEquals(AllocationDecision.NO, cae.getShardAllocationDecision().getMoveDecision().getAllocationDecision());
        assertNotNull("shard should have assigned node id", cae.getCurrentNode());
        assertNull("assigned shard should not have unassigned info", cae.getUnassignedInfo());
        explanation = cae.getShardAllocationDecision().getMoveDecision().getNodeDecisions().iterator().next();
        d = explanation.getCanAllocateDecision();
        assertNotNull("should have a decision", d);
        assertEquals(Decision.Type.NO, d.type());
        assertTrue(d.toString(), d.toString().contains(
            "NO(the shard cannot be allocated to the node on which it already exists [[test][0]"));
        assertTrue(d instanceof Decision.Multi);
        md = (Decision.Multi) d;
        ssd = md.getDecisions().get(0);
        assertEquals(Decision.Type.NO, ssd.type());
        assertTrue(ssd.toString(), ssd.toString().contains(
            "NO(the shard cannot be allocated to the node on which it already exists [[test][0]"));

        resp = client().admin().cluster().prepareAllocationExplain().useAnyUnassignedShard().get();
        cae = resp.getExplanation();
        assertNotNull("should always have an explanation", cae);
        assertEquals("test", cae.getShard().getIndexName());
        assertEquals(0, cae.getShard().getId());
        assertEquals(false, cae.isPrimary());
    }
}
