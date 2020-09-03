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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.cluster.RemoteConnectionInfo;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.client.cluster.RemoteInfoResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.DeleteComponentTemplateRequest;
import org.elasticsearch.client.indices.GetComponentTemplatesRequest;
import org.elasticsearch.client.indices.GetComponentTemplatesResponse;
import org.elasticsearch.client.indices.PutComponentTemplateRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Documentation for Cluster APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class ClusterClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testClusterPutSettings() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::put-settings-request
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        // end::put-settings-request

        // tag::put-settings-create-settings
        String transientSettingKey =
                RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey();
        int transientSettingValue = 10;
        Settings transientSettings =
                Settings.builder()
                .put(transientSettingKey, transientSettingValue, ByteSizeUnit.BYTES)
                .build(); // <1>

        String persistentSettingKey =
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey();
        String persistentSettingValue =
                EnableAllocationDecider.Allocation.NONE.name();
        Settings persistentSettings =
                Settings.builder()
                .put(persistentSettingKey, persistentSettingValue)
                .build(); // <2>
        // end::put-settings-create-settings

        // tag::put-settings-request-cluster-settings
        request.transientSettings(transientSettings); // <1>
        request.persistentSettings(persistentSettings); // <2>
        // end::put-settings-request-cluster-settings

        {
            // tag::put-settings-settings-builder
            Settings.Builder transientSettingsBuilder =
                    Settings.builder()
                    .put(transientSettingKey, transientSettingValue, ByteSizeUnit.BYTES);
            request.transientSettings(transientSettingsBuilder); // <1>
            // end::put-settings-settings-builder
        }
        {
            // tag::put-settings-settings-map
            Map<String, Object> map = new HashMap<>();
            map.put(transientSettingKey
                    , transientSettingValue + ByteSizeUnit.BYTES.getSuffix());
            request.transientSettings(map); // <1>
            // end::put-settings-settings-map
        }
        {
            // tag::put-settings-settings-source
            request.transientSettings(
                    "{\"indices.recovery.max_bytes_per_sec\": \"10b\"}"
                    , XContentType.JSON); // <1>
            // end::put-settings-settings-source
        }

        // tag::put-settings-request-timeout
        request.timeout(TimeValue.timeValueMinutes(2)); // <1>
        request.timeout("2m"); // <2>
        // end::put-settings-request-timeout
        // tag::put-settings-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::put-settings-request-masterTimeout

        // tag::put-settings-execute
        ClusterUpdateSettingsResponse response = client.cluster().putSettings(request, RequestOptions.DEFAULT);
        // end::put-settings-execute

        // tag::put-settings-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        Settings transientSettingsResponse = response.getTransientSettings(); // <2>
        Settings persistentSettingsResponse = response.getPersistentSettings(); // <3>
        // end::put-settings-response
        assertTrue(acknowledged);
        assertThat(transientSettingsResponse.get(transientSettingKey), equalTo(transientSettingValue + ByteSizeUnit.BYTES.getSuffix()));
        assertThat(persistentSettingsResponse.get(persistentSettingKey), equalTo(persistentSettingValue));

        // tag::put-settings-request-reset-transient
        request.transientSettings(Settings.builder().putNull(transientSettingKey).build()); // <1>
        // tag::put-settings-request-reset-transient
        request.persistentSettings(Settings.builder().putNull(persistentSettingKey));
        ClusterUpdateSettingsResponse resetResponse = client.cluster().putSettings(request, RequestOptions.DEFAULT);

        assertTrue(resetResponse.isAcknowledged());
    }

    public void testClusterUpdateSettingsAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();

            // tag::put-settings-execute-listener
            ActionListener<ClusterUpdateSettingsResponse> listener =
                    new ActionListener<ClusterUpdateSettingsResponse>() {
                @Override
                public void onResponse(ClusterUpdateSettingsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-settings-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-settings-execute-async
            client.cluster().putSettingsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-settings-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testClusterGetSettings() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::get-settings-request
        ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        // end::get-settings-request

        // tag::get-settings-request-includeDefaults
        request.includeDefaults(true); // <1>
        // end::get-settings-request-includeDefaults

        // tag::get-settings-request-local
        request.local(true); // <1>
        // end::get-settings-request-local

        // tag::get-settings-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::get-settings-request-masterTimeout

        // tag::get-settings-execute
        ClusterGetSettingsResponse response = client.cluster().getSettings(request, RequestOptions.DEFAULT); // <1>
        // end::get-settings-execute

        // tag::get-settings-response
        Settings persistentSettings = response.getPersistentSettings(); // <1>
        Settings transientSettings = response.getTransientSettings(); // <2>
        Settings defaultSettings = response.getDefaultSettings(); // <3>
        String settingValue = response.getSetting("cluster.routing.allocation.enable"); // <4>
        // end::get-settings-response

        assertThat(defaultSettings.size(), greaterThan(0));
    }

    public void testClusterGetSettingsAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();

        ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();

        // tag::get-settings-execute-listener
        ActionListener<ClusterGetSettingsResponse> listener =
            new ActionListener<ClusterGetSettingsResponse>() {
                @Override
                public void onResponse(ClusterGetSettingsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::get-settings-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::get-settings-execute-async
        client.cluster().getSettingsAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::get-settings-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unused")
    public void testClusterHealth() throws IOException {
        RestHighLevelClient client = highLevelClient();
        client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
        {
            // tag::health-request
            ClusterHealthRequest request = new ClusterHealthRequest();
            // end::health-request
        }
        {
            // tag::health-request-indices-ctr
            ClusterHealthRequest request = new ClusterHealthRequest("index1", "index2");
            // end::health-request-indices-ctr
        }
        {
            // tag::health-request-indices-setter
            ClusterHealthRequest request = new ClusterHealthRequest();
            request.indices("index1", "index2");
            // end::health-request-indices-setter
        }
        ClusterHealthRequest request = new ClusterHealthRequest();

        // tag::health-request-timeout
        request.timeout(TimeValue.timeValueSeconds(50)); // <1>
        request.timeout("50s"); // <2>
        // end::health-request-timeout

        // tag::health-request-master-timeout
        request.masterNodeTimeout(TimeValue.timeValueSeconds(20)); // <1>
        request.masterNodeTimeout("20s"); // <2>
        // end::health-request-master-timeout

        // tag::health-request-wait-status
        request.waitForStatus(ClusterHealthStatus.YELLOW); // <1>
        request.waitForYellowStatus(); // <2>
        // end::health-request-wait-status

        // tag::health-request-wait-events
        request.waitForEvents(Priority.NORMAL); // <1>
        // end::health-request-wait-events

        // tag::health-request-level
        request.level(ClusterHealthRequest.Level.SHARDS); // <1>
        // end::health-request-level

        // tag::health-request-wait-relocation
        request.waitForNoRelocatingShards(true); // <1>
        // end::health-request-wait-relocation

        // tag::health-request-wait-initializing
        request.waitForNoInitializingShards(true); // <1>
        // end::health-request-wait-initializing

        // tag::health-request-wait-nodes
        request.waitForNodes("2"); // <1>
        request.waitForNodes(">=2"); // <2>
        request.waitForNodes("le(2)"); // <3>
        // end::health-request-wait-nodes

        // tag::health-request-wait-active
        request.waitForActiveShards(ActiveShardCount.ALL); // <1>
        request.waitForActiveShards(1); // <2>
        // end::health-request-wait-active

        // tag::health-request-local
        request.local(true); // <1>
        // end::health-request-local

        // tag::health-execute
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        // end::health-execute

        assertThat(response.isTimedOut(), equalTo(false));
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(response, notNullValue());
        // tag::health-response-general
        String clusterName = response.getClusterName(); // <1>
        ClusterHealthStatus status = response.getStatus(); // <2>
        // end::health-response-general

        // tag::health-response-request-status
        boolean timedOut = response.isTimedOut(); // <1>
        RestStatus restStatus = response.status(); // <2>
        // end::health-response-request-status

        // tag::health-response-nodes
        int numberOfNodes = response.getNumberOfNodes(); // <1>
        int numberOfDataNodes = response.getNumberOfDataNodes(); // <2>
        // end::health-response-nodes

        {
            // tag::health-response-shards
            int activeShards = response.getActiveShards(); // <1>
            int activePrimaryShards = response.getActivePrimaryShards(); // <2>
            int relocatingShards = response.getRelocatingShards(); // <3>
            int initializingShards = response.getInitializingShards(); // <4>
            int unassignedShards = response.getUnassignedShards(); // <5>
            int delayedUnassignedShards = response.getDelayedUnassignedShards(); // <6>
            double activeShardsPercent = response.getActiveShardsPercent(); // <7>
            // end::health-response-shards
        }

        // tag::health-response-task
        TimeValue taskMaxWaitingTime = response.getTaskMaxWaitingTime(); // <1>
        int numberOfPendingTasks = response.getNumberOfPendingTasks(); // <2>
        int numberOfInFlightFetch = response.getNumberOfInFlightFetch(); // <3>
        // end::health-response-task

        // tag::health-response-indices
        Map<String, ClusterIndexHealth> indices = response.getIndices(); // <1>
        // end::health-response-indices

        {
            // tag::health-response-index
            ClusterIndexHealth index = indices.get("index"); // <1>
            ClusterHealthStatus indexStatus = index.getStatus();
            int numberOfShards = index.getNumberOfShards();
            int numberOfReplicas = index.getNumberOfReplicas();
            int activeShards = index.getActiveShards();
            int activePrimaryShards = index.getActivePrimaryShards();
            int initializingShards = index.getInitializingShards();
            int relocatingShards = index.getRelocatingShards();
            int unassignedShards = index.getUnassignedShards();
            // end::health-response-index

            // tag::health-response-shard-details
            Map<Integer, ClusterShardHealth> shards = index.getShards(); // <1>
            ClusterShardHealth shardHealth = shards.get(0);
            int shardId = shardHealth.getShardId();
            ClusterHealthStatus shardStatus = shardHealth.getStatus();
            int active = shardHealth.getActiveShards();
            int initializing = shardHealth.getInitializingShards();
            int unassigned = shardHealth.getUnassignedShards();
            int relocating = shardHealth.getRelocatingShards();
            boolean primaryActive = shardHealth.isPrimaryActive();
            // end::health-response-shard-details
        }
    }

    public void testClusterHealthAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            ClusterHealthRequest request = new ClusterHealthRequest();

            // tag::health-execute-listener
            ActionListener<ClusterHealthResponse> listener =
                new ActionListener<ClusterHealthResponse>() {
                    @Override
                    public void onResponse(ClusterHealthResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::health-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::health-execute-async
            client.cluster().healthAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::health-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testRemoteInfo() throws Exception {
        setupRemoteClusterConfig("local_cluster");

        RestHighLevelClient client = highLevelClient();

        // tag::remote-info-request
        RemoteInfoRequest request = new RemoteInfoRequest();
        // end::remote-info-request

        // tag::remote-info-execute
        RemoteInfoResponse response = client.cluster().remoteInfo(request, RequestOptions.DEFAULT); // <1>
        // end::remote-info-execute

        // tag::remote-info-response
        List<RemoteConnectionInfo> infos = response.getInfos();
        // end::remote-info-response

        assertThat(infos.size(), greaterThan(0));
    }

    public void testRemoteInfoAsync() throws Exception {
        setupRemoteClusterConfig("local_cluster");

        RestHighLevelClient client = highLevelClient();

        // tag::remote-info-request
        RemoteInfoRequest request = new RemoteInfoRequest();
        // end::remote-info-request


        // tag::remote-info-execute-listener
            ActionListener<RemoteInfoResponse> listener =
                new ActionListener<>() {
                    @Override
                    public void onResponse(RemoteInfoResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::remote-info-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::health-execute-async
            client.cluster().remoteInfoAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::health-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetComponentTemplates() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            Template template = new Template(Settings.builder().put("index.number_of_replicas", 3).build(), null, null);
            ComponentTemplate componentTemplate = new ComponentTemplate(template, null, null);
            PutComponentTemplateRequest putComponentTemplateRequest =
                new PutComponentTemplateRequest().name("ct1").componentTemplate(componentTemplate);
            client.cluster().putComponentTemplate(putComponentTemplateRequest, RequestOptions.DEFAULT);

            assertTrue(client.cluster().putComponentTemplate(putComponentTemplateRequest, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::get-component-templates-request
        GetComponentTemplatesRequest request = new GetComponentTemplatesRequest("ct1"); // <1>
        // end::get-component-templates-request

        // tag::get-component-templates-request-masterTimeout
        request.setMasterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.setMasterNodeTimeout("1m"); // <2>
        // end::get-component-templates-request-masterTimeout

        // tag::get-component-templates-execute
        GetComponentTemplatesResponse getTemplatesResponse = client.cluster().getComponentTemplate(request, RequestOptions.DEFAULT);
        // end::get-component-templates-execute

        // tag::get-component-templates-response
        Map<String, ComponentTemplate> templates = getTemplatesResponse.getComponentTemplates(); // <1>
        // end::get-component-templates-response

        assertThat(templates.size(), is(1));
        assertThat(templates.get("ct1"), is(notNullValue()));

        // tag::get-component-templates-execute-listener
        ActionListener<GetComponentTemplatesResponse> listener =
            new ActionListener<GetComponentTemplatesResponse>() {
                @Override
                public void onResponse(GetComponentTemplatesResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::get-component-templates-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::get-component-templates-execute-async
        client.cluster().getComponentTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::get-component-templates-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testPutComponentTemplate() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::put-component-template-request
            PutComponentTemplateRequest request = new PutComponentTemplateRequest()
                .name("ct1"); // <1>

            Settings settings = Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .build();
            String mappingJson = "{\n" +
                "  \"properties\": {\n" +
                "    \"message\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
            AliasMetadata twitterAlias = AliasMetadata.builder("twitter_alias").build();
            Template template = new Template(settings, new CompressedXContent(mappingJson), Map.of("twitter_alias", twitterAlias)); // <2>

            request.componentTemplate(new ComponentTemplate(template, null, null));
            assertTrue(client.cluster().putComponentTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-component-template-request
        }

        {
            // tag::put-component-template-request-version
            PutComponentTemplateRequest request = new PutComponentTemplateRequest()
                .name("ct1");
            Settings settings = Settings.builder()
                .put("index.number_of_replicas", 3)
                .build();
            Template template = new Template(settings, null, null);

            request.componentTemplate(new ComponentTemplate(template, 3L, null)); // <1>
            assertTrue(client.cluster().putComponentTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-component-template-request-version

            // tag::put-component-template-request-create
            request.create(true);  // <1>
            // end::put-component-template-request-create

            // tag::put-component-template-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::put-component-template-request-masterTimeout

            request.create(false); // make test happy

            // tag::put-component-template-request-execute
            AcknowledgedResponse putComponentTemplateResponse = client.cluster().putComponentTemplate(request, RequestOptions.DEFAULT);
            // end::put-component-template-request-execute

            // tag::put-component-template-response
            boolean acknowledged = putComponentTemplateResponse.isAcknowledged(); // <1>
            // end::put-component-template-response
            assertTrue(acknowledged);

            // tag::put-component-template-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse putComponentTemplateResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::put-component-template-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-component-template-execute-async
            client.cluster().putComponentTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-component-template-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteComponentTemplate() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            PutComponentTemplateRequest request = new PutComponentTemplateRequest()
                .name("ct1");

            Settings settings = Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .build();
            String mappingJson = "{\n" +
                "  \"properties\": {\n" +
                "    \"message\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
            AliasMetadata twitterAlias = AliasMetadata.builder("twitter_alias").build();
            Template template = new Template(settings, new CompressedXContent(mappingJson), Map.of("twitter_alias", twitterAlias));

            request.componentTemplate(new ComponentTemplate(template, null, null));
            assertTrue(client.cluster().putComponentTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::delete-component-template-request
        DeleteComponentTemplateRequest deleteRequest = new DeleteComponentTemplateRequest("ct1"); // <1>
        // end::delete-component-template-request

        // tag::delete-component-template-request-masterTimeout
        deleteRequest.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
        // end::delete-component-template-request-masterTimeout

        // tag::delete-component-template-execute
        AcknowledgedResponse deleteTemplateAcknowledge = client.cluster().deleteComponentTemplate(deleteRequest, RequestOptions.DEFAULT);
        // end::delete-component-template-execute

        // tag::delete-component-template-response
        boolean acknowledged = deleteTemplateAcknowledge.isAcknowledged(); // <1>
        // end::delete-component-template-response
        assertThat(acknowledged, equalTo(true));

        {
            PutComponentTemplateRequest request = new PutComponentTemplateRequest()
                .name("ct1");

            Settings settings = Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .build();
            Template template = new Template(settings, null, null);
            request.componentTemplate(new ComponentTemplate(template, null, null));
            assertTrue(client.cluster().putComponentTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::delete-component-template-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::delete-component-template-execute-listener

        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::delete-component-template-execute-async
        client.cluster().deleteComponentTemplateAsync(deleteRequest, RequestOptions.DEFAULT, listener); // <1>
        // end::delete-component-template-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }
}
