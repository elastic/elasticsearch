/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RuntimeFieldScriptRecoveryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(PainlessPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    /**
     * Verifies that a node can boot when an index contains a runtime field with a Painless script
     * that fails to compile, and that the mapping can be fixed via the API after boot.
     * <p>
     * This reproduces a scenario where a script valid in 7.17 (using {@code def values = ...}) fails
     * in 8.x because {@code StringFieldScript.getValues()} now injects an implicit {@code values}
     * variable into the Painless scope, causing a "variable [values] is already defined" compilation error.
     */
    public void testNodeBootsWithBrokenRuntimeFieldScript() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();

        logger.info("--> creating test index with a document");
        indicesAdmin().prepareCreate("test").setMapping("""
            {
              "properties": {
                "field1": { "type": "keyword" }
              }
            }""").get();
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        ensureYellow();

        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final IndexMetadata metadata = state.getMetadata().getProject().index("test");

        // Inject a mapping with a runtime field whose Painless script declares 'def values = ...',
        // which conflicts with the implicit 'values' variable injected by StringFieldScript.getValues().
        // This simulates a mapping that was valid in 7.17 (where getValues() did not exist on the
        // script base class) but fails to compile in 8.x+.
        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).putMapping("""
            {
              "_doc": {
                "runtime": {
                  "failure_reason": {
                    "type": "keyword",
                    "script": {
                      "source": "def values = doc['field1'].value; emit(values)"
                    }
                  }
                },
                "properties": {
                  "field1": { "type": "keyword" }
                }
              }
            }""");

        logger.info("--> restarting nodes with broken runtime field mapping");
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(brokenMeta)));

        logger.info("--> node should boot despite broken script - wait for the node to join the cluster");
        clusterAdmin().health(
            new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, new String[] {}).waitForNodes("1").waitForEvents(Priority.LANGUID)
        ).actionGet();

        logger.info("--> verify the shard is unassigned due to the broken script compilation error");
        assertBusy(() -> {
            final IndexRoutingTable indexRoutingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .routingTable()
                .index("test");
            assertNotNull(indexRoutingTable);
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
                assertTrue(shardRoutingTable.primaryShard().unassigned());
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().failedAllocations(), greaterThan(0));
            }
        }, 60, TimeUnit.SECONDS);

        logger.info("--> fix the broken runtime field by submitting a complete mapping with the variable renamed");
        assertAcked(indicesAdmin().preparePutMapping("test").setSource("""
            {
              "properties": {
                "field1": { "type": "keyword" }
              },
              "runtime": {
                "failure_reason": {
                  "type": "keyword",
                  "script": {
                    "source": "def vals = doc['field1'].value; emit(vals)"
                  }
                }
              }
            }""", XContentType.JSON));

        logger.info("--> retry allocation after fixing the mapping");
        ClusterRerouteUtils.rerouteRetryFailed(client());

        logger.info("--> wait for the index to recover to yellow status");
        clusterAdmin().health(
            new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, new String[] { "test" }).waitForYellowStatus()
                .waitForEvents(Priority.LANGUID)
                .waitForNoRelocatingShards(true)
        ).actionGet();

        logger.info("--> verify the document is accessible");
        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsMap().get("field1"), equalTo("value1"));

        logger.info("--> verify the fixed runtime field works by querying it");
        assertHitCountAndNoFailures(prepareSearch("test").setQuery(termQuery("failure_reason", "value1")), 1L);
    }

    /**
     * Verifies that a partial mapping update (one that omits existing properties) on an index whose
     * existing mapping cannot be loaded due to a broken script will lose the omitted properties.
     * <p>
     * Because the existing mapping fails to compile, the mapper service cannot load it, so the update
     * acts as a full replacement rather than a merge. Users must send a COMPLETE mapping to avoid this.
     */
    public void testPartialMappingUpdateLosesPropertiesWithBrokenScript() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();

        logger.info("--> creating test index with a keyword property and a document");
        indicesAdmin().prepareCreate("test").setMapping("""
            {
              "properties": {
                "field1": { "type": "keyword" }
              }
            }""").get();
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        ensureYellow();

        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final IndexMetadata metadata = state.getMetadata().getProject().index("test");

        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).putMapping("""
            {
              "_doc": {
                "runtime": {
                  "failure_reason": {
                    "type": "keyword",
                    "script": {
                      "source": "def values = doc['field1'].value; emit(values)"
                    }
                  }
                },
                "properties": {
                  "field1": { "type": "keyword" }
                }
              }
            }""");

        logger.info("--> restarting nodes with broken runtime field mapping");
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(brokenMeta)));

        clusterAdmin().health(
            new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, new String[] {}).waitForNodes("1").waitForEvents(Priority.LANGUID)
        ).actionGet();

        logger.info("--> sending a PARTIAL mapping update that only fixes the runtime field (omits properties)");
        assertAcked(indicesAdmin().preparePutMapping("test").setSource("""
            {
              "runtime": {
                "failure_reason": {
                  "type": "keyword",
                  "script": {
                    "source": "def vals = doc['field1'].value; emit(vals)"
                  }
                }
              }
            }""", XContentType.JSON));

        logger.info("--> verify that the field1 property was lost from the mapping");
        MappingMetadata mappingMetadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .getMetadata()
            .getProject()
            .index("test")
            .mapping();
        Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
        assertNull(
            "properties should be lost after partial mapping update on an index with a broken script",
            mappingSource.get("properties")
        );
        assertNotNull("runtime fields should be present", mappingSource.get("runtime"));
    }
}
