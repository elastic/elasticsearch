/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.Strings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DataStreamSecurityIT extends SecurityIntegTestCase {

    private static final String INDEX_ONLY_USER = "index_only_user";
    private static final String DS_ONLY_USER = "ds_only_user";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateSecurity.class, Netty4Plugin.class, MapperExtrasPlugin.class, DataStreamsPlugin.class, Wildcard.class);
    }

    @Override
    protected String configRoles() {
        return Strings.format("""
            %s:
              cluster: [ all ]
              indices:
                - names: '*'
                  allow_restricted_indices: true
                  privileges: [ all ]
            index_only_role:
              cluster: [ manage_index_templates ]
              indices:
                - names: 'my-index-*'
                  privileges: [ all ]
            ds_only_role:
              cluster: [ manage_index_templates ]
              indices:
                - names: 'ds-one'
                  privileges: [ manage ]
            """, SecuritySettingsSource.TEST_ROLE) + '\n' + SecuritySettingsSourceField.ES_TEST_ROOT_ROLE_YML;
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(
            getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        return SecuritySettingsSource.CONFIG_STANDARD_USER
            + INDEX_ONLY_USER
            + ":"
            + usersPasswdHashed
            + "\n"
            + DS_ONLY_USER
            + ":"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return SecuritySettingsSource.CONFIG_STANDARD_USER_ROLES
            + "index_only_role:"
            + INDEX_ONLY_USER
            + "\n"
            + "ds_only_role:"
            + DS_ONLY_USER
            + "\n";
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
        assertThat(before.getMetadata().getProject().dataStreams().get(dataStreamName).getIndices(), hasSize(2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DataStream> brokenDataStreamHolder = new AtomicReference<>();
        boolean shouldBreakIndexName = randomBoolean();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask(getTestName(), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DataStream original = currentState.getMetadata().getProject().dataStreams().get(dataStreamName);
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
        assertThat(after.getMetadata().getProject().dataStreams().get(dataStreamName).getIndices(), hasSize(1));

        // Data stream resolves now to one backing index.
        // Note, that old backing index still exists, but it is still hidden.
        // The modify data stream api only fixed the data stream by removing a broken reference to a backing index.
        indicesStatsResponse = client.admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(shouldBreakIndexName ? 1 : 2));
    }

    /**
     * Verifies that modifying a data stream requires privileges on both the index and the data stream.
     * A user who only has privileges on their own indices cannot add one to a data stream they lack access to.
     */
    public void testModifyDataStreamRequiresDataStreamPrivilege() throws Exception {
        final var adminClient = client().filterWithHeader(
            Map.of(
                BASIC_AUTH_HEADER,
                basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            )
        );
        final var indexOnlyClient = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(INDEX_ONLY_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))
        );

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("ds-two-template");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("ds-two"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .template(
                    Template.builder()
                        .mappings(
                            new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"message\":{\"type\":\"text\"}}}")
                        )
                )
                .build()
        );
        assertAcked(adminClient.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
        assertAcked(
            adminClient.execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "ds-two")
            ).actionGet()
        );

        indexOnlyClient.index(
            new IndexRequest("my-index-1").source("""
                {"@timestamp": "2024-01-01T00:00:00Z", "message": "test"}""", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            indexOnlyClient.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.addBackingIndex("ds-two", "my-index-1"))
                )
            ).actionGet();
        });
        assertThat(
            e.getMessage(),
            containsString(
                "action [indices:admin/data_stream/modify] is unauthorized for user [index_only_user] with effective roles "
                    + "[index_only_role] on indices [ds-two], this action is granted by the index privileges [manage,all]"
            )
        );
    }

    /**
     * Verifies that a user with privileges on both the data stream and the index can add
     * a backing index to the data stream.
     */
    public void testModifyDataStreamWithFullPrivileges() throws Exception {
        final var adminClient = client().filterWithHeader(
            Map.of(
                BASIC_AUTH_HEADER,
                basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            )
        );

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("logs-template");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs-*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .template(
                    Template.builder()
                        .mappings(
                            new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"message\":{\"type\":\"text\"}}}")
                        )
                )
                .build()
        );
        assertAcked(adminClient.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
        assertAcked(
            adminClient.execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "logs-app")
            ).actionGet()
        );

        adminClient.index(
            new IndexRequest("extra-index").source("""
                {"@timestamp": "2024-01-01T00:00:00Z", "message": "data"}""", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();

        assertAcked(
            adminClient.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.addBackingIndex("logs-app", "extra-index"))
                )
            ).actionGet()
        );

        ClusterState state = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(state.getMetadata().getProject().dataStreams().get("logs-app").getIndices(), hasSize(2));
    }

    /**
     * Verifies that removing a backing index from a data stream also requires privileges on that data stream.
     */
    public void testRemoveBackingIndexRequiresDataStreamPrivilege() throws Exception {
        final var adminClient = client().filterWithHeader(
            Map.of(
                BASIC_AUTH_HEADER,
                basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            )
        );
        final var indexOnlyClient = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(INDEX_ONLY_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))
        );

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("other-template");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("other-ds"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .template(Template.builder().mappings(new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}")))
                .build()
        );
        assertAcked(adminClient.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
        assertAcked(
            adminClient.execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "other-ds")
            ).actionGet()
        );
        assertAcked(adminClient.admin().indices().rolloverIndex(new RolloverRequest("other-ds", null)).actionGet());

        ClusterState state = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        String backingIndexName = state.getMetadata().getProject().dataStreams().get("other-ds").getIndices().get(0).getName();

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            indexOnlyClient.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.removeBackingIndex("other-ds", backingIndexName))
                )
            ).actionGet();
        });

        assertThat(
            e.getMessage(),
            containsString(
                "action [indices:admin/data_stream/modify] is unauthorized for user "
                    + "[index_only_user] with effective roles [index_only_role] on indices [.ds-other-ds-"
            )
        );
    }

    /**
     * Verifies that a user with only data stream privileges (no direct backing index privileges)
     * can still remove a backing index from the data stream.
     */
    public void testRemoveBackingIndexSucceedsWithDataStreamPrivilegeOnly() throws Exception {
        final var adminClient = client().filterWithHeader(
            Map.of(
                BASIC_AUTH_HEADER,
                basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            )
        );
        final var dsOnlyClient = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(DS_ONLY_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))
        );

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("ds-one-remove-template");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("ds-one"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .template(Template.builder().mappings(new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}")))
                .build()
        );
        assertAcked(adminClient.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
        assertAcked(
            adminClient.execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "ds-one")
            ).actionGet()
        );
        assertAcked(adminClient.admin().indices().rolloverIndex(new RolloverRequest("ds-one", null)).actionGet());

        ClusterState stateBefore = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        List<Index> indicesBefore = stateBefore.getMetadata().getProject().dataStreams().get("ds-one").getIndices();
        assertThat(indicesBefore, hasSize(2));
        Index indexToRemove = indicesBefore.get(0);
        Index indexToRetain = indicesBefore.get(1);

        // ds_only_user has manage privilege on ds-one but not on its backing indices (.ds-ds-one-*);
        // removing a backing index should succeed because it only requires data stream privilege
        assertAcked(
            dsOnlyClient.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.removeBackingIndex("ds-one", indexToRemove.getName()))
                )
            ).actionGet()
        );

        ClusterState stateAfter = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        List<Index> indicesAfter = stateAfter.getMetadata().getProject().dataStreams().get("ds-one").getIndices();
        assertThat(indicesAfter, hasSize(1));
        assertThat(indicesAfter.get(0).getName(), equalTo(indexToRetain.getName()));
        assertThat(indicesAfter.get(0).getUUID(), equalTo(indexToRetain.getUUID()));

        // adding the removed index back requires direct index privilege on the backing index (.ds-ds-one-*),
        // which ds_only_user lacks — so this must be rejected
        ElasticsearchSecurityException addBackException = expectThrows(ElasticsearchSecurityException.class, () -> {
            dsOnlyClient.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.addBackingIndex("ds-one", indexToRemove.getName()))
                )
            ).actionGet();
        });
        assertThat(
            addBackException.getMessage(),
            containsString(
                "action [indices:admin/data_stream/modify] is unauthorized for user [ds_only_user] with effective roles "
                    + "[ds_only_role] on indices ["
                    + indexToRemove.getName()
                    + "]"
            )
        );
    }

    /**
     * Verifies that modifying a data stream also requires privileges on the index being added.
     * A user with privileges only on a data stream cannot add an arbitrary index they lack access to.
     */
    public void testModifyDataStreamRequiresIndexPrivilege() throws Exception {
        final var adminClient = client().filterWithHeader(
            Map.of(
                BASIC_AUTH_HEADER,
                basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            )
        );
        final var dsOnlyClient = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(DS_ONLY_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))
        );

        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("ds-one-template");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("ds-one"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .template(
                    Template.builder()
                        .mappings(
                            new CompressedXContent("{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"message\":{\"type\":\"text\"}}}")
                        )
                )
                .build()
        );
        assertAcked(adminClient.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet());
        assertAcked(
            adminClient.execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "ds-one")
            ).actionGet()
        );

        adminClient.index(
            new IndexRequest("unrelated-index").source("""
                {"@timestamp": "2024-01-01T00:00:00Z", "message": "data"}""", XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            dsOnlyClient.execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    List.of(DataStreamAction.addBackingIndex("ds-one", "unrelated-index"))
                )
            ).actionGet();
        });
        assertThat(
            e.getMessage(),
            containsString(
                "action [indices:admin/data_stream/modify] is unauthorized for user [ds_only_user] with effective roles "
                    + "[ds_only_role] on indices [unrelated-index], this action is granted by the index privileges [manage,all]"
            )
        );
    }
}
