/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ResettableValue;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamSecurityIT extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateSecurity.class, Netty4Plugin.class, MapperExtrasPlugin.class, DataStreamsPlugin.class, Wildcard.class);
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(
            getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        return super.configUsers() + "only_failures:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "only_failures:only_failures\n";
    }

    @Override
    protected String configRoles() {
        // role that has analyze indices privileges only
        return Strings.format("""
            %s
            only_failures:
              indices:
                - names: '*'
                  privileges: [ 'read_failures', 'write' ]
            """, super.configRoles());
    }

    public void testRemoveGhostReference() throws Exception {
        var headers = Map.of(
            BASIC_AUTH_HEADER,
            basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        final var client = client().filterWithHeader(headers);

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs-*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());

        String dataStreamName = "logs-es";
        var request = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client.execute(CreateDataStreamAction.INSTANCE, request).actionGet());
        assertAcked(client.admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());

        var indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(2));

        ClusterState before = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(before.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DataStream> brokenDataStreamHolder = new AtomicReference<>();
        boolean shouldBreakIndexName = randomBoolean();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask(getTestName(), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DataStream original = currentState.getMetadata().dataStreams().get(dataStreamName);
                    String brokenIndexName = shouldBreakIndexName
                        ? original.getIndices().get(0).getName() + "-broken"
                        : original.getIndices().get(0).getName();
                    DataStream broken = original.copy()
                        .setBackingIndices(
                            original.getDataComponent()
                                .copy()
                                .setIndices(List.of(new Index(brokenIndexName, "broken"), original.getIndices().get(1)))
                                .build()
                        )
                        .build();
                    brokenDataStreamHolder.set(broken);
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.getMetadata()).put(broken).build())
                        .build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("error while adding a broken data stream", e);
                    latch.countDown();
                }
            });
        latch.await();
        var ghostReference = brokenDataStreamHolder.get().getIndices().get(0);

        // Many APIs fail with NPE, because of broken data stream:
        var expectedExceptionClass = shouldBreakIndexName ? ElasticsearchSecurityException.class : NullPointerException.class;
        expectThrows(expectedExceptionClass, () -> client.admin().indices().stats(new IndicesStatsRequest()).actionGet());
        expectThrows(expectedExceptionClass, () -> client.search(new SearchRequest()).actionGet());

        assertAcked(
            client.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.removeBackingIndex(dataStreamName, ghostReference.getName()))
                )
            )
        );
        ClusterState after = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(after.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(1));

        // Data stream resolves now to one backing index.
        // Note, that old backing index still exists, but it is still hidden.
        // The modify data stream api only fixed the data stream by removing a broken reference to a backing index.
        indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(shouldBreakIndexName ? 1 : 2));
    }

    public void testFailureStoreAuthorziation() throws Exception {
        var adminHeaders = Map.of(
            BASIC_AUTH_HEADER,
            basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        final var adminClient = client().filterWithHeader(adminHeaders);
        var onlyFailHeaders = Map.of(
            BASIC_AUTH_HEADER,
            basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        final var failuresClient = client().filterWithHeader(onlyFailHeaders);

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("id");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("stuff-*"))
                .template(
                    Template.builder()
                        .mappings(CompressedXContent.fromJSON("{\"properties\": {\"code\": {\"type\": \"integer\"}}}"))
                        .dataStreamOptions(
                            new DataStreamOptions.Template(
                                ResettableValue.create(new DataStreamFailureStore.Template(ResettableValue.create(true)))
                            )
                        )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        assertAcked(adminClient.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());

        String dataStreamName = "stuff-es";
        var request = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(adminClient.execute(CreateDataStreamAction.INSTANCE, request).actionGet());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                .source("{\"code\": \"well this aint right\"}", XContentType.JSON),
            new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                .source("{\"@timestamp\": \"2015-01-01T12:10:30Z\", \"code\": 404}", XContentType.JSON)
        );
        BulkResponse bulkResponse = adminClient.bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(2));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStreamName;
        String failureIndexPrefix = DataStream.FAILURE_STORE_PREFIX + dataStreamName;

        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailure(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            // assertThat(itemResponse.getIndex(), anyOf(startsWith(backingIndexPrefix), startsWith(failureIndexPrefix)));
        }

        indicesAdmin().refresh(new RefreshRequest(dataStreamName)).actionGet();
        var getResp = failuresClient.admin()
            .indices()
            .getIndex(new GetIndexRequestBuilder(failuresClient, TimeValue.THIRTY_SECONDS).addIndices(dataStreamName + "::*").request());
        var searchResponse = failuresClient.prepareSearch(dataStreamName + "::failures").get();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
    }

}
