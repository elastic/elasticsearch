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

import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.ESSingleNodeTestCase;


/**
 * Tests for the cluster allocation explanation
 */
public final class ClusterAllocationExplainTests extends ESSingleNodeTestCase {

    public void testShardExplain() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings("index.number_of_shards", 1, "index.number_of_replicas", 1).get();
        client().admin().cluster().health(Requests.clusterHealthRequest("test").waitForYellowStatus()).get();
        ClusterAllocationExplainResponse resp = client().admin().cluster().prepareAllocationExplain()
                .setIndex("test").setShard(0).setPrimary(false).get();

        ClusterAllocationExplanation cae = resp.getExplanation();
        assertNotNull("should always have an explanation", cae);
        assertEquals("test", cae.getShard().getIndexName());
        assertEquals(0, cae.getShard().getId());
        assertEquals(false, cae.isPrimary());
        assertNull(cae.getAssignedNodeId());
        assertFalse(cae.isStillFetchingShardData());
        assertNotNull(cae.getUnassignedInfo());
        NodeExplanation explanation = cae.getNodeExplanations().values().iterator().next();
        ClusterAllocationExplanation.FinalDecision fd = explanation.getFinalDecision();
        ClusterAllocationExplanation.StoreCopy storeCopy = explanation.getStoreCopy();
        String finalExplanation = explanation.getFinalExplanation();
        Decision d = explanation.getDecision();
        assertNotNull("should have a decision", d);
        assertEquals(Decision.Type.NO, d.type());
        assertEquals(ClusterAllocationExplanation.FinalDecision.NO, fd);
        assertEquals(ClusterAllocationExplanation.StoreCopy.AVAILABLE, storeCopy);
        assertTrue(d.toString(), d.toString().contains("NO(the shard cannot be allocated on the same node id"));
        assertTrue(d instanceof Decision.Multi);
        Decision.Multi md = (Decision.Multi) d;
        Decision ssd = md.getDecisions().get(0);
        assertEquals(Decision.Type.NO, ssd.type());
        assertTrue(ssd.toString(), ssd.toString().contains("NO(the shard cannot be allocated on the same node id"));
        Float weight = explanation.getWeight();
        assertNotNull("should have a weight", weight);

        resp = client().admin().cluster().prepareAllocationExplain().setIndex("test").setShard(0).setPrimary(true).get();

        cae = resp.getExplanation();
        assertNotNull("should always have an explanation", cae);
        assertEquals("test", cae.getShard().getIndexName());
        assertEquals(0, cae.getShard().getId());
        assertEquals(true, cae.isPrimary());
        assertFalse(cae.isStillFetchingShardData());
        assertNotNull("shard should have assigned node id", cae.getAssignedNodeId());
        assertNull("assigned shard should not have unassigned info", cae.getUnassignedInfo());
        explanation = cae.getNodeExplanations().values().iterator().next();
        d = explanation.getDecision();
        fd = explanation.getFinalDecision();
        storeCopy = explanation.getStoreCopy();
        finalExplanation = explanation.getFinalExplanation();
        assertNotNull("should have a decision", d);
        assertEquals(Decision.Type.NO, d.type());
        assertEquals(ClusterAllocationExplanation.FinalDecision.ALREADY_ASSIGNED, fd);
        assertEquals(ClusterAllocationExplanation.StoreCopy.AVAILABLE, storeCopy);
        assertTrue(d.toString(), d.toString().contains("NO(the shard cannot be allocated on the same node id"));
        assertTrue(d instanceof Decision.Multi);
        md = (Decision.Multi) d;
        ssd = md.getDecisions().get(0);
        assertEquals(Decision.Type.NO, ssd.type());
        assertTrue(ssd.toString(), ssd.toString().contains("NO(the shard cannot be allocated on the same node id"));
        weight = explanation.getWeight();
        assertNotNull("should have a weight", weight);

        resp = client().admin().cluster().prepareAllocationExplain().useAnyUnassignedShard().get();
        cae = resp.getExplanation();
        assertNotNull("should always have an explanation", cae);
        assertEquals("test", cae.getShard().getIndexName());
        assertEquals(0, cae.getShard().getId());
        assertEquals(false, cae.isPrimary());
    }
}
