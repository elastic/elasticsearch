/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Tests the PainlessExecuteAction against a local cluster with one remote cluster configured.
 * Execute action tests are run against both the local cluster and the remote cluster,
 */
public class CrossClusterPainlessExecuteIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final String LOCAL_INDEX = "local_idx";
    private static final String REMOTE_INDEX = "remote_idx";
    private static final String KEYWORD_FIELD = "my_field";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugs = Arrays.asList(PainlessPlugin.class);
        return Stream.concat(super.nodePlugins(clusterAlias).stream(), plugs.stream()).collect(Collectors.toList());
    }

    public void testPainlessExecuteAgainstLocalCluster() throws Exception {
        setupTwoClusters();

        Script script = new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            Strings.format("doc['%s'].value.length() <= params.max_length", KEYWORD_FIELD),
            Map.of("max_length", 4)
        );
        ScriptContext<?> context = FilterScript.CONTEXT;

        PainlessExecuteAction.Request.ContextSetup contextSetup = createContextSetup(LOCAL_INDEX);
        PainlessExecuteAction.Request request = new PainlessExecuteAction.Request(script, context.name, contextSetup);

        ActionFuture<PainlessExecuteAction.Response> actionFuture = client(LOCAL_CLUSTER).admin()
            .cluster()
            .execute(PainlessExecuteAction.INSTANCE, request);

        assertBusy(() -> assertTrue(actionFuture.isDone()));
        PainlessExecuteAction.Response response = actionFuture.actionGet();
        Object result = response.getResult();
        assertThat(result, Matchers.instanceOf(Boolean.class));
        assertTrue((Boolean) result);
    }

    /**
     * Query the local cluster to run the execute actions against the 'cluster_a:remote_idx' index.
     * There is no local index with the REMOTE_INDEX name, so it has to do a cross-cluster action for this to work
     */
    public void testPainlessExecuteAsCrossClusterAction() throws Exception {
        setupTwoClusters();

        Script script = new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            Strings.format("doc['%s'].value.length() <= params.max_length", KEYWORD_FIELD),
            Map.of("max_length", 4)
        );
        ScriptContext<?> context = FilterScript.CONTEXT;

        PainlessExecuteAction.Request.ContextSetup contextSetup = createContextSetup(REMOTE_CLUSTER + ":" + REMOTE_INDEX);
        PainlessExecuteAction.Request request = new PainlessExecuteAction.Request(script, context.name, contextSetup);

        ActionFuture<PainlessExecuteAction.Response> actionFuture = client(LOCAL_CLUSTER).admin()
            .cluster()
            .execute(PainlessExecuteAction.INSTANCE, request);

        assertBusy(() -> assertTrue(actionFuture.isDone()));
        PainlessExecuteAction.Response response = actionFuture.actionGet();
        Object result = response.getResult();
        assertThat(result, Matchers.instanceOf(Boolean.class));
        assertTrue((Boolean) result);
    }

    private static PainlessExecuteAction.Request.ContextSetup createContextSetup(String index) {
        QueryBuilder query = new MatchAllQueryBuilder();
        BytesReference doc;
        XContentType xContentType = XContentType.JSON.canonical();
        try {
            XContentBuilder xContentBuilder = XContentBuilder.builder(xContentType.xContent());
            xContentBuilder.startObject();
            xContentBuilder.field(KEYWORD_FIELD, "four");
            xContentBuilder.endObject();
            doc = BytesReference.bytes(xContentBuilder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        PainlessExecuteAction.Request.ContextSetup contextSetup = new PainlessExecuteAction.Request.ContextSetup(index, doc, query);
        contextSetup.setXContentType(XContentType.JSON);
        return contextSetup;
    }

    private void setupTwoClusters() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate(LOCAL_INDEX).setMapping(KEYWORD_FIELD, "type=keyword"));
        indexDocs(client(LOCAL_CLUSTER), LOCAL_INDEX);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(1);
        final Settings.Builder allocationFilter = Settings.builder();
        if (randomBoolean()) {
            remoteCluster.ensureAtLeastNumDataNodes(3);
            List<String> remoteDataNodes = remoteCluster.clusterService()
                .state()
                .nodes()
                .stream()
                .filter(DiscoveryNode::canContainData)
                .map(DiscoveryNode::getName)
                .toList();
            assertThat(remoteDataNodes.size(), Matchers.greaterThanOrEqualTo(3));
            List<String> seedNodes = randomSubsetOf(between(1, remoteDataNodes.size() - 1), remoteDataNodes);
            disconnectFromRemoteClusters();
            configureRemoteCluster(REMOTE_CLUSTER, seedNodes);
            if (randomBoolean()) {
                // Using proxy connections
                allocationFilter.put("index.routing.allocation.exclude._name", String.join(",", seedNodes));
            } else {
                allocationFilter.put("index.routing.allocation.include._name", String.join(",", seedNodes));
            }
        }
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(REMOTE_INDEX)
                .setMapping(KEYWORD_FIELD, "type=keyword")
                .setSettings(Settings.builder().put(allocationFilter.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(REMOTE_INDEX)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER), REMOTE_INDEX);
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource(KEYWORD_FIELD, "my_value").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }
}
