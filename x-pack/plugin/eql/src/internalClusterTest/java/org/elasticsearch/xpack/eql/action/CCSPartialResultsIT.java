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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CCSPartialResultsIT extends AbstractMultiClustersTestCase {

    static String REMOTE_CLUSTER = "cluster_a";

    protected Collection<Class<? extends Plugin>> nodePlugins(String cluster) {
        return Collections.singletonList(LocalStateEQLXPackPlugin.class);
    }

    protected final Client localClient() {
        return client(LOCAL_CLUSTER);
    }

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    /**
     *
     * @return remote node name
     */
    private String createSchema() {
        final Client remoteClient = client(REMOTE_CLUSTER);
        final String remoteNode = cluster(REMOTE_CLUSTER).startDataOnlyNode();
        final String remoteNode2 = cluster(REMOTE_CLUSTER).startDataOnlyNode();

        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate("test-1-remote")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", remoteNode)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("@timestamp", "type=date"),
            TimeValue.timeValueSeconds(60)
        );

        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate("test-2-remote")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", remoteNode2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("@timestamp", "type=date"),
            TimeValue.timeValueSeconds(60)
        );

        for (int i = 0; i < 5; i++) {
            int val = i * 2;
            remoteClient.prepareIndex("test-1-remote")
                .setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }
        for (int i = 0; i < 5; i++) {
            int val = i * 2 + 1;
            remoteClient.prepareIndex("test-2-remote")
                .setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }

        remoteClient.admin().indices().prepareRefresh().get();
        return remoteNode;
    }

    // ------------------------------------------------------------------------
    // queries with full cluster (no missing shards)
    // ------------------------------------------------------------------------

    public void testNoFailures() throws ExecutionException, InterruptedException, IOException {
        createSchema();

        // event query
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("process where true")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        EqlSearchResponse response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + i));
        }
        assertThat(response.shardFailures().length, is(0));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 0"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query with missing event on unavailable shard
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(0));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 0"));
        assertThat(response.shardFailures().length, is(0));

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards and allow_partial_search_results=true
    // and allow_partial_sequence_result=true
    // ------------------------------------------------------------------------

    public void testAllowPartialSearchAndSequence_event() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        // event query
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));
    }

    public void testAllowPartialSearchAndSequence_sequence() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        // sequence query on both shards
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSequenceResults(true);
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(2).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    public void testAllowPartialSearchAndSequence_sample() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        // sample query on both shards
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards and allow_partial_search_results=true
    // and default allow_partial_sequence_results (ie. false)
    // ------------------------------------------------------------------------

    public void testAllowPartialSearch_event() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        // event query
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

    }

    public void testAllowPartialSearch_sequence() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        // sequence query on both shards
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    public void testAllowPartialSearch_sample() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        // sample query on both shards
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        var response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        var sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]");
        if (randomBoolean()) {
            request = request.allowPartialSearchResults(true);
        }
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

    }

    // ------------------------------------------------------------------------
    // same queries, with missing shards and with default xpack.eql.default_allow_partial_results=false
    // ------------------------------------------------------------------------

    public void testClusterSetting_event() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        cluster(REMOTE_CLUSTER).client()
            .execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
                )
            )
            .get();

        // event query
        shouldFailWithDefaults("process where true");

        localClient().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }

    public void testClusterSetting_sequence() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        cluster(REMOTE_CLUSTER).client()
            .execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
                )
            )
            .get();
        // sequence query on both shards
        shouldFailWithDefaults("sequence [process where value == 1] [process where value == 2]");

        // sequence query on the available shard only
        shouldFailWithDefaults("sequence [process where value == 1] [process where value == 3]");

        // sequence query on the unavailable shard only
        shouldFailWithDefaults("sequence [process where value == 0] [process where value == 2]");

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        shouldFailWithDefaults(
            "sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]"
        );

        localClient().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }

    public void testClusterSetting_sample() throws ExecutionException, InterruptedException, IOException {
        var remoteNode = createSchema();
        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(remoteNode);

        cluster(REMOTE_CLUSTER).client()
            .execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                    Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), false)
                )
            )
            .get();

        // sample query on both shards
        shouldFailWithDefaults("sample by key [process where value == 2] [process where value == 1]");

        // sample query on the available shard only
        shouldFailWithDefaults("sample by key [process where value == 3] [process where value == 1]");

        // sample query on the unavailable shard only
        shouldFailWithDefaults("sample by key [process where value == 2] [process where value == 0]");

        localClient().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }

    private void shouldFailWithDefaults(String query) throws InterruptedException {
        EqlSearchRequest request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query(query);
        if (randomBoolean()) {
            request = request.allowPartialSequenceResults(randomBoolean());
        }
        try {
            localClient().execute(EqlSearchAction.INSTANCE, request).get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e.getCause().getCause(), instanceOf(SearchPhaseExecutionException.class));
        }
    }
}
