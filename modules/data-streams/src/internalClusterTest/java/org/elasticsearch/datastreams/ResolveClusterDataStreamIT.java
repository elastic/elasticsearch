/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterActionResponse;
import org.elasticsearch.action.admin.indices.resolve.ResolveClusterInfo;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests the ResolveClusterAction around matching data streams.
 * ResolveClusterIT is a sibling IT test that does additional testing
 * not related to data streams.
 */
public class ResolveClusterDataStreamIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_1 = "remote1";
    private static final String REMOTE_CLUSTER_2 = "remote2";
    private static long EARLIEST_TIMESTAMP = 1691348810000L;
    private static long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, true);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(DataStreamsPlugin.class);
    }

    public void testClusterResolveWithDataStreams() throws Exception {
        Map<String, Object> testClusterInfo = setupThreeClusters(false);
        String localDataStream = (String) testClusterInfo.get("local.datastream");
        String remoteDataStream1 = (String) testClusterInfo.get("remote1.datastream");
        String remoteIndex2 = (String) testClusterInfo.get("remote2.index");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // test all clusters against data streams (present only on local and remote1)
        {
            String[] indexExpressions = new String[] {
                localDataStream,
                REMOTE_CLUSTER_1 + ":" + remoteDataStream1,
                REMOTE_CLUSTER_2 + ":" + remoteDataStream1 // does not exist on remote2
            };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertNull(remote2.getMatchingIndices());
            assertNull(remote2.getBuild());
            assertNotNull(remote2.getError());
            assertThat(remote2.getError(), containsString("no such index [" + remoteDataStream1 + "]"));

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test clusters against datastream or indices, such that all should match
        {
            String[] indexExpressions = new String[] {
                localDataStream,
                REMOTE_CLUSTER_1 + ":" + remoteDataStream1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test wildcards against datastream names
        {
            String[] indexExpressions = new String[] {
                localDataStream.substring(0, 3) + "*",
                REMOTE_CLUSTER_1.substring(0, 3) + "*:" + remoteDataStream1.substring(0, 3) + "*",
                REMOTE_CLUSTER_2 + ":" + remoteIndex2.substring(0, 2) + "*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }

        // test remote only clusters
        {
            String[] indexExpressions = new String[] {
                REMOTE_CLUSTER_1 + ":" + remoteDataStream1,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2.substring(0, 2) + "*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(2, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());
        }
    }

    public void testClusterResolveWithDataStreamsUsingAlias() throws Exception {
        Map<String, Object> testClusterInfo = setupThreeClusters(true);
        String localDataStreamAlias = (String) testClusterInfo.get("local.datastream.alias");
        String remoteDataStream1Alias = (String) testClusterInfo.get("remote1.datastream.alias");
        String remoteIndex2 = (String) testClusterInfo.get("remote2.index");
        boolean skipUnavailable1 = (Boolean) testClusterInfo.get("remote1.skip_unavailable");
        boolean skipUnavailable2 = true;

        // test all clusters against datastream alias (present only on local and remote1)
        {
            String[] indexExpressions = new String[] {
                localDataStreamAlias,
                REMOTE_CLUSTER_1 + ":" + remoteDataStream1Alias,
                REMOTE_CLUSTER_2 + ":" + remoteIndex2 };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indexExpressions);

            ActionFuture<ResolveClusterActionResponse> future = client(LOCAL_CLUSTER).admin()
                .indices()
                .execute(TransportResolveClusterAction.TYPE, request);
            ResolveClusterActionResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertNotNull(response);

            Map<String, ResolveClusterInfo> clusterInfo = response.getResolveClusterInfo();
            assertEquals(3, clusterInfo.size());
            Set<String> expectedClusterNames = Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
            assertThat(clusterInfo.keySet(), equalTo(expectedClusterNames));

            ResolveClusterInfo remote1 = clusterInfo.get(REMOTE_CLUSTER_1);
            assertThat(remote1.isConnected(), equalTo(true));
            assertThat(remote1.getSkipUnavailable(), equalTo(skipUnavailable1));
            assertThat(remote1.getMatchingIndices(), equalTo(true));
            assertNotNull(remote1.getBuild().version());
            assertNull(remote1.getError());

            ResolveClusterInfo remote2 = clusterInfo.get(REMOTE_CLUSTER_2);
            assertThat(remote2.isConnected(), equalTo(true));
            assertThat(remote2.getSkipUnavailable(), equalTo(skipUnavailable2));
            assertThat(remote2.getMatchingIndices(), equalTo(true));
            assertNotNull(remote2.getBuild().version());
            assertNull(remote2.getError());

            ResolveClusterInfo local = clusterInfo.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.isConnected(), equalTo(true));
            assertThat(local.getSkipUnavailable(), equalTo(false));
            assertThat(local.getMatchingIndices(), equalTo(true));
            assertNotNull(local.getBuild().version());
            assertNull(local.getError());
        }
    }

    private Map<String, Object> setupThreeClusters(boolean useAlias) throws IOException, ExecutionException, InterruptedException {
        String dataStreamLocal = "metrics-foo";
        String dataStreamLocalAlias = randomAlphaOfLengthBetween(5, 16);

        // set up data stream on local cluster
        {
            Client client = client(LOCAL_CLUSTER);
            List<String> backingIndices = new ArrayList<>();
            Map<String, AliasMetadata> aliases = null;
            if (useAlias) {
                aliases = new HashMap<>();
                aliases.put(dataStreamLocalAlias, AliasMetadata.builder(dataStreamLocalAlias).writeIndex(randomBoolean()).build());
            }
            putComposableIndexTemplate(client, "id1", List.of(dataStreamLocal + "*"), aliases);
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "metrics-foo"
            );
            assertAcked(client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get());

            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "*" });
            GetDataStreamAction.Response getDataStreamResponse = client.execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            DataStream fooDataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
            String backingIndex = fooDataStream.getIndices().get(0).getName();
            backingIndices.add(backingIndex);
            GetIndexResponse getIndexResponse = client.admin()
                .indices()
                .getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex))
                .actionGet();
            assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
            assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
            Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
            assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

            int numDocsBar = randomIntBetween(2, 16);
            indexDataStreamDocs(client, dataStreamLocal, numDocsBar);
        }

        // set up data stream on remote1 cluster
        String dataStreamRemote1 = "metrics-bar";
        String dataStreamRemote1Alias = randomAlphaOfLengthBetween(5, 16);
        {
            Client client = client(REMOTE_CLUSTER_1);
            List<String> backingIndices = new ArrayList<>();
            Map<String, AliasMetadata> aliases = null;
            if (useAlias) {
                aliases = new HashMap<>();
                aliases.put(dataStreamRemote1Alias, AliasMetadata.builder(dataStreamRemote1Alias).writeIndex(randomBoolean()).build());
            }
            putComposableIndexTemplate(client, "id2", List.of(dataStreamRemote1 + "*"), aliases);
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "metrics-bar"
            );
            assertAcked(client.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get());

            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "*" });
            GetDataStreamAction.Response getDataStreamResponse = client.execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();

            DataStream barDataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
            String backingIndex = barDataStream.getIndices().get(0).getName();
            backingIndices.add(backingIndex);
            GetIndexResponse getIndexResponse = client.admin()
                .indices()
                .getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex))
                .actionGet();
            assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
            assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
            Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
            assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

            int numDocsBar = randomIntBetween(2, 16);
            indexDataStreamDocs(client, dataStreamRemote1, numDocsBar);
        }

        // set up remote2 cluster and non-datastream index

        String remoteIndex2 = "prod123";
        int numShardsRemote2 = randomIntBetween(2, 4);
        final InternalTestCluster remoteCluster2 = cluster(REMOTE_CLUSTER_2);
        remoteCluster2.ensureAtLeastNumDataNodes(randomIntBetween(1, 2));
        final Settings.Builder remoteSettings2 = Settings.builder();
        remoteSettings2.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote2);

        assertAcked(
            client(REMOTE_CLUSTER_2).admin()
                .indices()
                .prepareCreate(remoteIndex2)
                .setSettings(Settings.builder().put(remoteSettings2.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        assertFalse(
            client(REMOTE_CLUSTER_2).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex2)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER_2), remoteIndex2);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER_1);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER_1).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable1 = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.datastream", dataStreamLocal);
        clusterInfo.put("local.datastream.alias", dataStreamLocalAlias);

        clusterInfo.put("remote1.skip_unavailable", skipUnavailable1);
        clusterInfo.put("remote1.datastream", dataStreamRemote1);
        clusterInfo.put("remote1.datastream.alias", dataStreamRemote1Alias);

        clusterInfo.put("remote2.index", remoteIndex2);
        clusterInfo.put("remote2.skip_unavailable", true);

        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(50, 100);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    void putComposableIndexTemplate(Client client, String id, List<String> patterns, @Nullable Map<String, AliasMetadata> aliases)
        throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().aliases(aliases))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client.execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    void indexDataStreamDocs(Client client, String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        client.admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
    }
}
