/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.eql.plugin.EqlAsyncGetResultAction;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class PartialSearchResultsIT extends AbstractEqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(randomIntBetween(100, 500)))
            .build();
    }

    /**
     *
     * @return node name where the first index is
     */
    private String createSchema() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final List<String> dataNodes = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .toList();
        final String assignedNodeForIndex1 = randomFrom(dataNodes);

        assertAcked(
            indicesAdmin().prepareCreate("test-1")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.include._name", assignedNodeForIndex1)
                        .build()
                )
                .setMapping("@timestamp", "type=date")
        );
        assertAcked(
            indicesAdmin().prepareCreate("test-2")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.exclude._name", assignedNodeForIndex1)
                        .build()
                )
                .setMapping("@timestamp", "type=date")
        );

        for (int i = 0; i < 5; i++) {
            int val = i * 2;
            prepareIndex("test-1").setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }
        for (int i = 0; i < 5; i++) {
            int val = i * 2 + 1;
            prepareIndex("test-2").setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }
        refresh();
        return assignedNodeForIndex1;
    }

    public void testNoFailures() throws Exception {
        createSchema();

        // ------------------------------------------------------------------------
        // queries with full cluster (no missing shards)
        // ------------------------------------------------------------------------

        // event query
        var request = new EqlSearchRequest().indices("test-*")
            .query("process where true")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + i));
        }
        assertThat(response.shardFailures().length, is(0));

        // sequence query on both shards
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 0"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query with missing event on unavailable shard
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence with maxspan=10s [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(0));

        // sample query on both shards
        request = new EqlSearchRequest().indices("test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 0"));
        assertThat(response.shardFailures().length, is(0));

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards. Let them fail
    // allow_partial_sequence_results has no effect if allow_partial_sequence_results is not set to true.
    // ------------------------------------------------------------------------

    public void testFailures_event() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // event query
        shouldFail("process where true");

    }

    public void testFailures_sequence() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sequence query on both shards
        shouldFail("sequence [process where value == 1] [process where value == 2]");

        // sequence query on the available shard only
        shouldFail("sequence [process where value == 1] [process where value == 3]");

        // sequence query on the unavailable shard only
        shouldFail("sequence [process where value == 0] [process where value == 2]");

        // sequence query with missing event on unavailable shard.
        shouldFail("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]");
    }

    public void testFailures_sample() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sample query on both shards
        shouldFail("sample by key [process where value == 2] [process where value == 1]");

        // sample query on the available shard only
        shouldFail("sample by key [process where value == 3] [process where value == 1]");

        // sample query on the unavailable shard only
        shouldFail("sample by key [process where value == 2] [process where value == 0]");

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards and allow_partial_search_results=true
    // and allow_partial_sequence_result=true
    // ------------------------------------------------------------------------

    public void testAllowPartialSearchAndSequenceResults_event() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // event query
        var request = new EqlSearchRequest().indices("test-*").query("process where true");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

    }

    public void testAllowPartialSearchAndSequenceResults_sequence() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sequence query on both shards
        var request = new EqlSearchRequest().indices("test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }

        var response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(2).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    public void testAllowPartialSearchAndSequenceResults_sample() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sample query on both shards
        var request = new EqlSearchRequest().indices("test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices("test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards and allow_partial_search_results=true
    // and default allow_partial_sequence_results (ie. false)
    // ------------------------------------------------------------------------

    public void testAllowPartialSearchResults_event() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // event query
        var request = new EqlSearchRequest().indices("test-*").query("process where true");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

    }

    public void testAllowPartialSearchResults_sequence() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sequence query on both shards
        var request = new EqlSearchRequest().indices("test-*").query("sequence [process where value == 1] [process where value == 2]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices("test-*").query("sequence [process where value == 1] [process where value == 3]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices("test-*").query("sequence [process where value == 0] [process where value == 2]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices("test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    public void testAllowPartialSearchResults_sample() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sample query on both shards
        var request = new EqlSearchRequest().indices("test-*").query("sample by key [process where value == 2] [process where value == 1]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices("test-*").query("sample by key [process where value == 3] [process where value == 1]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices("test-*").query("sample by key [process where value == 2] [process where value == 0]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    // ------------------------------------------------------------------------
    // same queries, this time async, with missing shards and allow_partial_search_results=true
    // and default allow_partial_sequence_results (ie. false)
    // ------------------------------------------------------------------------

    public void testAsyncAllowPartialSearchResults_event() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // event query
        var response = runAsync("process where true", true);
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

    }

    public void testAsyncAllowPartialSearchResults_sequence() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        // sequence query on both shards
        var response = runAsync("sequence [process where value == 1] [process where value == 2]", true);
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        response = runAsync("sequence [process where value == 1] [process where value == 3]", true);
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        response = runAsync("sequence [process where value == 0] [process where value == 2]", true);
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        response = runAsync(
            "sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]",
            true
        );
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    public void testAsyncAllowPartialSearchResults_sample() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);
        // sample query on both shards
        var response = runAsync("sample by key [process where value == 2] [process where value == 1]", true);
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        response = runAsync("sample by key [process where value == 3] [process where value == 1]", true);
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        response = runAsync("sample by key [process where value == 2] [process where value == 0]", true);
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards and with default xpack.eql.default_allow_partial_results=false
    // ------------------------------------------------------------------------

    public void testClusterSetting_event() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
            )
        ).get();

        // event query
        shouldFail("process where true");

        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }

    public void testClusterSetting_sequence() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
            )
        ).get();
        // sequence query on both shards
        shouldFail("sequence [process where value == 1] [process where value == 2]");

        // sequence query on the available shard only
        shouldFail("sequence [process where value == 1] [process where value == 3]");

        // sequence query on the unavailable shard only
        shouldFail("sequence [process where value == 0] [process where value == 2]");

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        shouldFail("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]");

        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }

    public void testClusterSetting_sample() throws Exception {
        final String assignedNodeForIndex1 = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        internalCluster().stopNode(assignedNodeForIndex1);

        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
            )
        ).get();

        // sample query on both shards
        shouldFail("sample by key [process where value == 2] [process where value == 1]");

        // sample query on the available shard only
        shouldFail("sample by key [process where value == 3] [process where value == 1]");

        // sample query on the unavailable shard only
        shouldFail("sample by key [process where value == 2] [process where value == 0]");

        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }

    private static EqlSearchResponse runAsync(String query, Boolean allowPartialSearchResults) throws InterruptedException,
        ExecutionException {
        EqlSearchRequest request;
        EqlSearchResponse response;
        request = new EqlSearchRequest().indices("test-*").query(query).waitForCompletionTimeout(TimeValue.ZERO);
        if (allowPartialSearchResults != null) {
            if (allowPartialSearchResults == false || randomBoolean()) request = request.allowPartialSearchResults(
                allowPartialSearchResults
            );
        }
        response = client().execute(EqlSearchAction.INSTANCE, request).get();
        while (response.isRunning()) {
            GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(response.id()).setKeepAlive(TimeValue.timeValueMinutes(10))
                .setWaitForCompletionTimeout(TimeValue.timeValueMillis(10));
            response = client().execute(EqlAsyncGetResultAction.INSTANCE, getResultsRequest).get();
        }
        return response;
    }

    private static void shouldFail(String query) throws InterruptedException {
        EqlSearchRequest request = new EqlSearchRequest().indices("test-*").query(query);
        request = request.allowPartialSearchResults(false);
        if (randomBoolean()) {
            request = request.allowPartialSequenceResults(randomBoolean());
        }
        try {
            client().execute(EqlSearchAction.INSTANCE, request).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(SearchPhaseExecutionException.class));
        }
    }
}
