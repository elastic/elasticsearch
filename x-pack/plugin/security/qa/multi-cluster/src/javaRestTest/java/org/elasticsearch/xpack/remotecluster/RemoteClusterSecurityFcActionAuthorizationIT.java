/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionRequest;
import org.elasticsearch.xpack.ccr.action.repositories.GetCcrRestoreFileChunkAction;
import org.elasticsearch.xpack.ccr.action.repositories.GetCcrRestoreFileChunkRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.PASS;
import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.USER;
import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.createCrossClusterAccessApiKey;
import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.performRequestWithAdminUser;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RemoteClusterSecurityFcActionAuthorizationIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster testCluster = ElasticsearchCluster.local()
        .name("test-cluster")
        .module("analysis-common")
        .module("x-pack-ccr")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("remote_cluster_server.enabled", "true")
        .setting("remote_cluster.port", "0")
        .setting("xpack.security.remote_cluster_server.ssl.enabled", "false")
        .user(USER, PASS.toString())
        .build();

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    protected String getTestRestCluster() {
        return testCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static <Request extends ActionRequest, Response extends ActionResponse> Response executeRemote(
        RemoteClusterClient client,
        RemoteClusterActionType<Response> action,
        Request request
    ) throws Exception {
        final var future = new PlainActionFuture<Response>();
        client.execute(action, request, future);
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (ExceptionsHelper.unwrapCause(e.getCause()) instanceof Exception cause) {
                throw cause;
            }

            throw new AssertionError(e);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testIndicesPrivilegesAreEnforcedForCcrRestoreSessionActions() throws Exception {
        final Map<String, Object> crossClusterApiKeyMap = createCrossClusterAccessApiKey(adminClient(), """
            {
              "replication": [
                {
                   "names": ["leader-index*"]
                }
              ]
            }""");

        final String leaderIndex1UUID;
        final String leaderIndex2UUID;
        final String privateIndexUUID;

        // Create indices on the leader cluster
        {
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "leader-index-1" } }
                { "name": "doc-1" }
                { "index": { "_index": "leader-index-2" } }
                { "name": "doc-2" }
                { "index": { "_index": "private-index" } }
                { "name": "doc-3" }
                """));
            assertOK(adminClient().performRequest(bulkRequest));

            final ObjectPath indexSettings = assertOKAndCreateObjectPath(
                adminClient().performRequest(new Request("GET", "/leader-index*,private-index/_settings"))
            );
            leaderIndex1UUID = indexSettings.evaluate("leader-index-1.settings.index.uuid");
            leaderIndex2UUID = indexSettings.evaluate("leader-index-2.settings.index.uuid");
            privateIndexUUID = indexSettings.evaluate("private-index.settings.index.uuid");
        }

        // Simulate QC behaviours by directly connecting to the FC using a transport service
        final String apiKey = (String) crossClusterApiKeyMap.get("encoded");
        final boolean skipUnavailable = randomBoolean();
        try (MockTransportService service = startTransport("node", threadPool, apiKey, skipUnavailable)) {
            final RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            final List<RemoteConnectionInfo> remoteConnectionInfos = remoteClusterService.getRemoteConnectionInfos().toList();
            assertThat(remoteConnectionInfos, hasSize(1));
            assertThat(remoteConnectionInfos.get(0).isConnected(), is(true));

            Executor responseExecutor = threadPool.generic();
            final var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                "my_remote_cluster",
                responseExecutor,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
            );

            // Creating a restore session fails if index is not accessible
            final ShardId privateShardId = new ShardId("private-index", privateIndexUUID, 0);
            final PutCcrRestoreSessionRequest request = new PutCcrRestoreSessionRequest(UUIDs.randomBase64UUID(), privateShardId);
            final ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> executeRemote(remoteClusterClient, PutCcrRestoreSessionAction.REMOTE_TYPE, request)
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "action [indices:internal/admin/ccr/restore/session/put] towards remote cluster is unauthorized "
                        + "for user [_system] with assigned roles [] authenticated by API key id ["
                        + crossClusterApiKeyMap.get("id")
                        + "] of user [test_user] on indices [private-index], this action is granted by the index privileges "
                        + "[cross_cluster_replication_internal,all]"
                )
            );

            // Creating restore sessions succeed when indices are accessible
            final String sessionUUID1 = UUIDs.randomBase64UUID();
            final ShardId shardId1 = new ShardId("leader-index-1", leaderIndex1UUID, 0);
            final PutCcrRestoreSessionRequest request1 = new PutCcrRestoreSessionRequest(sessionUUID1, shardId1);
            final PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse response1 = executeRemote(
                remoteClusterClient,
                PutCcrRestoreSessionAction.REMOTE_TYPE,
                request1
            );
            assertThat(response1.getStoreFileMetadata().fileMetadataMap().keySet(), hasSize(greaterThanOrEqualTo(1)));
            final String leaderIndex1FileName = response1.getStoreFileMetadata().fileMetadataMap().keySet().iterator().next();

            final String sessionUUID2 = UUIDs.randomBase64UUID();
            final ShardId shardId2 = new ShardId("leader-index-2", leaderIndex2UUID, 0);
            final PutCcrRestoreSessionRequest request2 = new PutCcrRestoreSessionRequest(sessionUUID2, shardId2);
            final PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse response2 = executeRemote(
                remoteClusterClient,
                PutCcrRestoreSessionAction.REMOTE_TYPE,
                request2
            );
            assertThat(response2.getStoreFileMetadata().fileMetadataMap().keySet(), hasSize(greaterThanOrEqualTo(1)));
            final String leaderIndex2FileName = response2.getStoreFileMetadata().fileMetadataMap().keySet().iterator().next();

            // Get file chuck fails if requested index is not authorized
            final var e1 = expectThrows(
                ElasticsearchSecurityException.class,
                () -> executeRemote(
                    remoteClusterClient,
                    GetCcrRestoreFileChunkAction.REMOTE_TYPE,
                    new GetCcrRestoreFileChunkRequest(response1.getNode(), sessionUUID1, leaderIndex1FileName, 1, privateShardId)
                )
            );
            assertThat(
                e1.getMessage(),
                containsString("action [indices:internal/admin/ccr/restore/file_chunk/get] towards remote cluster is unauthorized")
            );

            // Get file chunk fails if requested index does not match session index
            final var e2 = expectThrows(
                IllegalArgumentException.class,
                () -> executeRemote(
                    remoteClusterClient,
                    GetCcrRestoreFileChunkAction.REMOTE_TYPE,
                    new GetCcrRestoreFileChunkRequest(response1.getNode(), sessionUUID1, leaderIndex1FileName, 1, shardId2)
                )
            );
            assertThat(e2.getMessage(), containsString("does not match requested shardId"));

            // Get file chunk fails if requested file is not part of the session
            final var e3 = expectThrows(
                IllegalArgumentException.class,
                () -> executeRemote(
                    remoteClusterClient,
                    GetCcrRestoreFileChunkAction.REMOTE_TYPE,
                    new GetCcrRestoreFileChunkRequest(
                        response1.getNode(),
                        sessionUUID1,
                        randomValueOtherThan(leaderIndex1FileName, () -> randomAlphaOfLengthBetween(3, 20)),
                        1,
                        shardId1
                    )
                )
            );
            assertThat(e3.getMessage(), containsString("invalid file name"));

            // Get file chunk succeeds
            final GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse getChunkResponse = executeRemote(
                remoteClusterClient,
                GetCcrRestoreFileChunkAction.REMOTE_TYPE,
                new GetCcrRestoreFileChunkRequest(response2.getNode(), sessionUUID2, leaderIndex2FileName, 1, shardId2)
            );
            assertFalse(getChunkResponse.getChunk().hasReferences());

            // Clear restore session fails if index is unauthorized
            final var e4 = expectThrows(
                ElasticsearchSecurityException.class,
                () -> executeRemote(
                    remoteClusterClient,
                    ClearCcrRestoreSessionAction.REMOTE_TYPE,
                    new ClearCcrRestoreSessionRequest(sessionUUID1, response1.getNode(), privateShardId)
                )
            );
            assertThat(
                e4.getMessage(),
                containsString("action [indices:internal/admin/ccr/restore/session/clear] towards remote cluster is unauthorized")
            );

            // Clear restore session fails if requested index does not match session index
            final var e5 = expectThrows(
                IllegalArgumentException.class,
                () -> executeRemote(
                    remoteClusterClient,
                    ClearCcrRestoreSessionAction.REMOTE_TYPE,
                    new ClearCcrRestoreSessionRequest(sessionUUID1, response1.getNode(), shardId2)
                )
            );
            assertThat(e5.getMessage(), containsString("does not match requested shardId"));

            // Clear restore sessions succeed
            executeRemote(
                remoteClusterClient,
                ClearCcrRestoreSessionAction.REMOTE_TYPE,
                new ClearCcrRestoreSessionRequest(sessionUUID1, response1.getNode(), shardId1)
            );
            executeRemote(
                remoteClusterClient,
                ClearCcrRestoreSessionAction.REMOTE_TYPE,
                new ClearCcrRestoreSessionRequest(sessionUUID2, response2.getNode(), shardId2)
            );
        }
    }

    public void testRestApiKeyIsNotAllowedOnRemoteClusterPort() throws IOException {
        final var createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "rest_api_key"
            }""");
        final Response createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
        assertOK(createApiKeyResponse);
        final Map<String, Object> apiKeyMap = responseAsMap(createApiKeyResponse);
        final String apiKey = (String) apiKeyMap.get("encoded");
        final boolean skipUnavailable = randomBoolean();
        try (MockTransportService service = startTransport("node", threadPool, apiKey, skipUnavailable)) {
            final RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            final var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                "my_remote_cluster",
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
            );
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(
                    remoteClusterClient,
                    RemoteClusterNodesAction.REMOTE_TYPE,
                    RemoteClusterNodesAction.Request.REMOTE_CLUSTER_SERVER_NODES
                )
            );
            if (skipUnavailable) {
                assertThat(e, instanceOf(ConnectTransportException.class));
                assertThat(e.getMessage(), containsString("Unable to connect to [my_remote_cluster]"));
            } else {
                assertThat(e, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    e.getMessage(),
                    containsString(
                        "authentication expected API key type of [cross_cluster], but API key [" + apiKeyMap.get("id") + "] has type [rest]"
                    )
                );
            }
        }
    }

    public void testUpdateCrossClusterApiKey() throws Exception {
        final Map<String, Object> crossClusterApiKeyMap = createCrossClusterAccessApiKey(adminClient(), """
            {
              "search": [
                {
                   "names": ["other-index"]
                }
              ]
            }""");
        final String apiKeyId = (String) crossClusterApiKeyMap.get("id");

        // Create indices on the leader cluster
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "index" } }
            { "name": "doc-1" }
            """));
        assertOK(adminClient().performRequest(bulkRequest));

        // End user subjectInfo
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo = new CrossClusterAccessSubjectInfo(
            Authentication.newRealmAuthentication(
                new User("foo", "role"),
                new Authentication.RealmRef(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), "node")
            ),
            new RoleDescriptorsIntersection(
                new RoleDescriptor(
                    "cross_cluster",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("index").privileges("read", "read_cross_cluster").build() },
                    null
                )
            )
        );
        // Field cap request to test
        final FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices("index").fields("name");

        // Perform cross-cluster requests
        boolean skipUnavailable = randomBoolean();
        try (
            MockTransportService service = startTransport(
                "node",
                threadPool,
                (String) crossClusterApiKeyMap.get("encoded"),
                Map.of(TransportFieldCapabilitiesAction.NAME, crossClusterAccessSubjectInfo),
                skipUnavailable
            )
        ) {
            final RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            final List<RemoteConnectionInfo> remoteConnectionInfos = remoteClusterService.getRemoteConnectionInfos().toList();
            assertThat(remoteConnectionInfos, hasSize(1));
            assertThat(remoteConnectionInfos.get(0).isConnected(), is(true));
            final var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                "my_remote_cluster",
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
            );

            // 1. Not accessible because API key does not grant the access
            final ElasticsearchSecurityException e1 = expectThrows(
                ElasticsearchSecurityException.class,
                () -> executeRemote(remoteClusterClient, TransportFieldCapabilitiesAction.REMOTE_TYPE, request)
            );
            assertThat(
                e1.getMessage(),
                containsString(
                    "action [indices:data/read/field_caps] towards remote cluster is unauthorized "
                        + "for user [foo] with assigned roles [role] authenticated by API key id ["
                        + apiKeyId
                        + "] of user [test_user] on indices [index], this action is granted by the index privileges "
                        + "[view_index_metadata,manage,read,all]"
                )
            );

            // 2. Update the API key to grant access
            final Request updateApiKeyRequest = new Request("PUT", "/_security/cross_cluster/api_key/" + apiKeyId);
            updateApiKeyRequest.setJsonEntity("""
                {
                  "access": {
                    "search": [
                      {
                        "names": ["index"]
                      }
                    ]
                  }
                }""");
            assertOK(performRequestWithAdminUser(adminClient(), updateApiKeyRequest));
            final FieldCapabilitiesResponse fieldCapabilitiesResponse = executeRemote(
                remoteClusterClient,
                TransportFieldCapabilitiesAction.REMOTE_TYPE,
                request
            );
            assertThat(fieldCapabilitiesResponse.getIndices(), arrayContaining("index"));

            // 3. Update the API key again to remove access
            updateApiKeyRequest.setJsonEntity("""
                {
                  "access": {
                    "replication": [
                      {
                        "names": ["index"]
                      }
                    ]
                  },
                  "metadata": { "tag": 42 }
                }""");
            assertOK(performRequestWithAdminUser(adminClient(), updateApiKeyRequest));
            final ElasticsearchSecurityException e2 = expectThrows(
                ElasticsearchSecurityException.class,
                () -> executeRemote(remoteClusterClient, TransportFieldCapabilitiesAction.REMOTE_TYPE, request)
            );
            assertThat(
                e2.getMessage(),
                containsString(
                    "action [indices:data/read/field_caps] towards remote cluster is unauthorized "
                        + "for user [foo] with assigned roles [role] authenticated by API key id ["
                        + apiKeyId
                        + "] of user [test_user] on indices [index], this action is granted by the index privileges "
                        + "[view_index_metadata,manage,read,all]"
                )
            );
        }
    }

    public void testMalformedShardLevelActionIsRejected() throws Exception {
        final Map<String, Object> crossClusterApiKeyMap = createCrossClusterAccessApiKey(adminClient(), """
            {
              "search": [
                {
                   "names": ["idx-a", "idx-b"]
                }
              ]
            }""");

        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "idx-a", "_id": "1" } }
            { "name": "doc-1" }
            { "index": { "_index": "idx-b", "_id": "1" } }
            { "name": "doc-1" }
            """));
        assertOK(adminClient().performRequest(bulkRequest));

        final Request getIndexSettingsRequest = new Request("GET", "/idx-b/_settings");
        getIndexSettingsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        final ObjectPath indexSettings = assertOKAndCreateObjectPath(adminClient().performRequest(getIndexSettingsRequest));
        String otherIndexId = indexSettings.evaluate(".idx-b.settings.index.uuid");

        // Create the malformed request with a mismatch between with the request index and shard ID
        String indexA = "idx-a";

        try (
            MockTransportService service = startTransport(
                "node",
                threadPool,
                (String) crossClusterApiKeyMap.get("encoded"),
                Map.of(TransportGetAction.TYPE.name() + "[s]", buildCrossClusterAccessSubjectInfo(indexA)),
                randomBoolean()
            )
        ) {
            final RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            final List<RemoteConnectionInfo> remoteConnectionInfos = remoteClusterService.getRemoteConnectionInfos().toList();
            assertThat(remoteConnectionInfos, hasSize(1));
            assertThat(remoteConnectionInfos.get(0).isConnected(), is(true));

            MalformedGetRequest malformedGetRequest = new MalformedGetRequest(otherIndexId);
            malformedGetRequest.assertParsesAsGetRequest();
            final ElasticsearchSecurityException e = expectThrows(
                ElasticsearchSecurityException.class,
                () -> executeRemote(
                    remoteClusterService.getRemoteClusterClient(
                        "my_remote_cluster",
                        threadPool.generic(),
                        RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
                    ),
                    new RemoteClusterActionType<>(TransportGetAction.TYPE.name() + "[s]", GetResponse::new),
                    malformedGetRequest
                )
            );
            assertThat(e.getMessage(), containsString("is unauthorized"));
        }

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
    }

    private static CrossClusterAccessSubjectInfo buildCrossClusterAccessSubjectInfo(String... indices) throws IOException {
        return new CrossClusterAccessSubjectInfo(
            Authentication.newRealmAuthentication(new User("query-user", "role"), new Authentication.RealmRef("file", "file", "node")),
            new RoleDescriptorsIntersection(
                new RoleDescriptor(
                    "cross_cluster",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(indices).privileges("read", "read_cross_cluster").build() },
                    null
                )
            )
        );
    }

    private static MockTransportService startTransport(
        final String nodeName,
        final ThreadPool threadPool,
        String encodedApiKey,
        boolean skipUnavailable
    ) {
        return startTransport(nodeName, threadPool, encodedApiKey, Map.of(), skipUnavailable);
    }

    private static MockTransportService startTransport(
        final String nodeName,
        final ThreadPool threadPool,
        String encodedApiKey,
        Map<String, CrossClusterAccessSubjectInfo> subjectInfoLookup,
        boolean skipUnavailable
    ) {
        final String remoteClusterServerEndpoint = testCluster.getRemoteClusterServerEndpoint(0);

        final Settings.Builder builder = Settings.builder()
            .put("node.name", nodeName)
            .put("xpack.security.remote_cluster_client.ssl.enabled", "false");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote_cluster.credentials", encodedApiKey);
        builder.setSecureSettings(secureSettings);
        if (randomBoolean()) {
            builder.put("cluster.remote.my_remote_cluster.mode", "sniff")
                .put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(skipUnavailable))
                .put("cluster.remote.my_remote_cluster.seeds", remoteClusterServerEndpoint);
        } else {
            builder.put("cluster.remote.my_remote_cluster.mode", "proxy")
                .put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(skipUnavailable))
                .put("cluster.remote.my_remote_cluster.proxy_address", remoteClusterServerEndpoint);
        }

        final MockTransportService service = MockTransportService.createNewService(
            builder.build(),
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool,
            null
        );
        boolean success = false;
        try {
            service.addSendBehavior((connection, requestId, action, request, options) -> {
                final ThreadContext threadContext = threadPool.getThreadContext();
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    new CrossClusterAccessHeaders(
                        "ApiKey " + encodedApiKey,
                        subjectInfoLookup.getOrDefault(
                            action,
                            SystemUser.crossClusterAccessSubjectInfo(TransportVersion.current(), nodeName)
                        )
                    ).writeToContext(threadContext);
                    connection.sendRequest(requestId, action, request, options);
                }
            });
            service.start();
            success = true;
        } finally {
            if (success == false) {
                service.close();
            }
        }
        return service;
    }

    private static class MalformedGetRequest extends LegacyActionRequest {
        private final String otherIndexId;

        MalformedGetRequest(String otherIndexId) {
            this.otherIndexId = otherIndexId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null; // this space intentionally left blank
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // This is a manually-written malformed get request, since it's intentionally difficult to form this kind of
            // request with production code.
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeOptionalWriteable(new ShardId("idx-b", otherIndexId, 0)); // InternalShardId
            out.writeOptionalString("idx-a"); // index name
            out.writeString("1"); // doc id
            out.writeOptionalString(null); // routing
            out.writeOptionalString(null); // preference

            out.writeBoolean(true); // refresh
            out.writeOptionalStringArray(null); // stored fields
            out.writeBoolean(true); // realtime
            out.writeByte(VersionType.INTERNAL.getValue()); // version type
            out.writeLong(Versions.MATCH_ANY); // version
            out.writeOptionalWriteable(null); // fetch source context
            out.writeBoolean(false); // force synthetic source
        }

        /**
         * Checks that this fake request can actually be parsed as a get request. If this assertion fails,
         * check that the above writeTo method matches GetRequest's streaming methods.
         */
        public void assertParsesAsGetRequest() throws Exception {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            this.writeTo(out);
            InputStreamStreamInput inputStreamStreamInput = new InputStreamStreamInput(new ByteArrayInputStream(outBuffer.toByteArray()));
            GetRequest parsedRequest = new GetRequest(inputStreamStreamInput);
            assertEquals("idx-a", parsedRequest.index());
            assertEquals("1", parsedRequest.id());
            assertEquals("idx-b", parsedRequest.shards().get(0).getIndexName());
        }
    }
}
