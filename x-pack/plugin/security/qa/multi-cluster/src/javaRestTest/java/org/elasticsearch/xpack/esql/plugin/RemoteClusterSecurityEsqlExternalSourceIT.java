/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Datasets are local to the cluster that registered them. A holder of a cross-cluster API key reaches the fulfilling
 * cluster's compute actions over the remote-cluster server port, so it can send what a querying cluster sends, a
 * {@link ClusterComputeRequest}, or a {@link DataNodeRequest} directly, and either can carry a plan with an
 * {@link ExternalSourceExec} that the key's {@code search} grant would cover by name. This test sends both over a real
 * remote-cluster connection authenticated with such a key and asserts the fulfilling cluster refuses them before it
 * plans anything, while a permitted cross-cluster ES|QL query over an index with the same key keeps working.
 */
public class RemoteClusterSecurityEsqlExternalSourceIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final String INDEX = "employees";
    private static final String REMOTE_USER = "remote_esql_user";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "false")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "false")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> (String) crossClusterApiKey().get("encoded"))
            .build();
    }

    @ClassRule
    // The fulfilling cluster must be up before the query cluster asks for the API key it is configured with.
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @After
    public void terminateThreadPool() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /** A key whose {@code search} grant covers every name, dataset names included. */
    private static Map<String, Object> crossClusterApiKey() {
        if (API_KEY_MAP_REF.get() == null) {
            API_KEY_MAP_REF.set(createCrossClusterAccessApiKey("""
                {
                  "search": [
                    {
                       "names": ["*"]
                    }
                  ]
                }"""));
        }
        return API_KEY_MAP_REF.get();
    }

    public void testExternalSourceIsRefusedOverTheRemoteClusterConnection() throws Exception {
        configureRemoteCluster();
        populateData();

        // The positive control: the same key serves a permitted cross-cluster query over an index.
        Request query = new Request("POST", "_query");
        query.setJsonEntity("{\"query\": \"FROM my_remote_cluster:" + INDEX + " | STATS c = COUNT(*) | LIMIT 1\"}");
        query.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_USER, PASS)));
        Response response = client().performRequest(query);
        assertOK(response);
        List<List<Object>> values = ObjectPath.createFromResponse(response).evaluate("values");
        assertThat(values, hasSize(1));
        assertThat(values.get(0), equalTo(List.of(2)));

        // A subject the fulfilling cluster would grant read on every name; what matters is that the refusal does not
        // depend on the grant.
        CrossClusterAccessSubjectInfo subjectInfo = new CrossClusterAccessSubjectInfo(
            Authentication.newRealmAuthentication(new User("query-user", "role"), new Authentication.RealmRef("file", "file", "node")),
            new RoleDescriptorsIntersection(
                new RoleDescriptor(
                    "cross_cluster",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("read").build() },
                    null
                )
            )
        );
        String apiKey = (String) crossClusterApiKey().get("encoded");
        Map<String, CrossClusterAccessSubjectInfo> subjectInfoByAction = Map.of(
            ComputeService.CLUSTER_ACTION_NAME,
            subjectInfo,
            ComputeService.DATA_ACTION_NAME,
            subjectInfo
        );
        try (MockTransportService service = startTransport("node", threadPool, apiKey, subjectInfoByAction)) {
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            assertThat(remoteClusterService.getRemoteConnectionInfos().toList(), hasSize(1));
            // The connection is established lazily; wait for it rather than take it.
            PlainActionFuture<Transport.Connection> connected = new PlainActionFuture<>();
            remoteClusterService.maybeEnsureConnectedAndGetConnection(REMOTE_CLUSTER_ALIAS, true, connected);
            Transport.Connection connection = connected.actionGet(TimeValue.timeValueSeconds(30));
            ThreadContext threadContext = threadPool.getThreadContext();

            ExternalSourceExec exec = externalSource("secret_dataset");
            PhysicalPlan plan = new ExchangeSinkExec(Source.EMPTY, exec.output(), false, exec);
            String[] indices = new String[] { INDEX };
            Configuration configuration = configuration();

            // What a querying cluster sends: refused as a remote-cluster compute carrying an external source.
            ClusterComputeRequest clusterRequest = new ClusterComputeRequest(
                "querying-cluster",
                "external-cluster-" + randomAlphaOfLength(8),
                configuration,
                new RemoteClusterPlan(plan, indices, new OriginalIndices(indices, IndicesOptions.strictExpandOpen()))
            );
            ElasticsearchStatusException clusterRefusal = expectRefusal(
                service,
                connection,
                ComputeService.CLUSTER_ACTION_NAME,
                clusterRequest,
                in -> new ComputeResponse(in, threadContext)
            );
            assertThat(clusterRefusal.getMessage(), containsString("local to the cluster that registered them"));

            // What a data node receives from its own coordinator, sent directly: refused under the cross-cluster
            // authentication before the dataset name is even looked at.
            DataNodeRequest dataNodeRequest = new DataNodeRequest(
                "external-data-" + randomAlphaOfLength(8),
                configuration,
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                List.of(),
                Map.of(),
                plan,
                new String[0],
                IndicesOptions.strictExpandOpen(),
                false,
                false,
                false,
                exec.splits()
            );
            ElasticsearchStatusException dataNodeRefusal = expectRefusal(
                service,
                connection,
                ComputeService.DATA_ACTION_NAME,
                dataNodeRequest,
                in -> new DataNodeComputeResponse(in, threadContext)
            );
            assertThat(dataNodeRefusal.getMessage(), containsString("local to the cluster that registered them"));
        }
    }

    private void populateData() throws IOException {
        Request createIndex = new Request("PUT", INDEX);
        createIndex.setJsonEntity("""
            { "mappings": { "properties": { "emp_id": { "type": "keyword" } } } }""");
        assertOK(performRequestAgainstFulfillingCluster(createIndex));
        Request bulk = new Request("POST", "/_bulk?refresh=true");
        bulk.setJsonEntity("""
            { "index": { "_index": "employees" } }
            { "emp_id": "1" }
            { "index": { "_index": "employees" } }
            { "emp_id": "2" }
            """);
        assertOK(performRequestAgainstFulfillingCluster(bulk));

        Request putRole = new Request("PUT", "/_security/role/remote_esql");
        putRole.setJsonEntity("""
            {
              "indices": [ { "names": [""], "privileges": ["read"] } ],
              "remote_indices": [ { "names": ["employees"], "privileges": ["read"], "clusters": ["my_remote_cluster"] } ]
            }""");
        assertOK(adminClient().performRequest(putRole));
        Request putUser = new Request("PUT", "/_security/user/" + REMOTE_USER);
        putUser.setJsonEntity("""
            { "password": "x-pack-test-password", "roles": ["remote_esql"] }""");
        assertOK(adminClient().performRequest(putUser));
    }

    private <T extends TransportResponse> ElasticsearchStatusException expectRefusal(
        MockTransportService service,
        Transport.Connection connection,
        String action,
        AbstractTransportRequest request,
        Writeable.Reader<T> reader
    ) {
        request.setParentTask(new TaskId("querying-node", randomNonNegativeLong()));
        PlainActionFuture<T> future = new PlainActionFuture<>();
        service.sendRequest(
            connection,
            action,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(future, reader, threadPool.generic())
        );
        Exception e = expectThrows(Exception.class, () -> future.actionGet(TimeValue.timeValueSeconds(30)));
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause.toString(), cause, instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException refusal = (ElasticsearchStatusException) cause;
        assertThat(refusal.status(), is(RestStatus.BAD_REQUEST));
        return refusal;
    }

    /** An external source naming {@code dataset}, with the config and split a crafted plan would carry. */
    private static ExternalSourceExec externalSource(String dataset) {
        List<Attribute> output = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://attacker/whatever/*.csv",
            "csv",
            output,
            Map.of("format", "csv", "auth", "federated_identity", "role_arn", "arn:aws:iam::123456789012:role/attacker"),
            Map.of(),
            null,
            10
        );
        List<ExternalSplit> splits = List.of(
            new FileSplit("csv", StoragePath.of("s3://attacker/whatever/rows.csv"), 0, 10, "csv", Map.of(), Map.of())
        );
        return exec.withDatasetName(dataset).withSplits(splits);
    }

    private static Configuration configuration() {
        return new Configuration(
            Instant.now(),
            Locale.US,
            null,
            null,
            new QueryPragmas(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            "crafted",
            false,
            Map.of(),
            System.nanoTime(),
            false,
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            ResolvedSettings.EMPTY,
            Map.of()
        );
    }

    /**
     * A transport service of its own, connected to the fulfilling cluster's remote-cluster server port with the API key,
     * that stamps every request with the cross-cluster access headers a querying cluster would send: the subject in
     * {@code subjectInfoByAction} for the actions under test, the system user's for the connection's own actions.
     */
    private static MockTransportService startTransport(
        String nodeName,
        ThreadPool threadPool,
        String encodedApiKey,
        Map<String, CrossClusterAccessSubjectInfo> subjectInfoByAction
    ) {
        String remoteClusterServerEndpoint = fulfillingCluster.getRemoteClusterServerEndpoint(0);
        Settings.Builder builder = Settings.builder()
            .put("node.name", nodeName)
            .put("xpack.security.remote_cluster_client.ssl.enabled", "false")
            .put("cluster.remote.my_remote_cluster.mode", "proxy")
            .put("cluster.remote.my_remote_cluster.proxy_address", remoteClusterServerEndpoint);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote_cluster.credentials", encodedApiKey);
        builder.setSecureSettings(secureSettings);

        MockTransportService service = MockTransportService.createNewService(
            builder.build(),
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool,
            null
        );
        boolean success = false;
        try {
            service.addSendBehavior((connection, requestId, action, request, options) -> {
                ThreadContext threadContext = threadPool.getThreadContext();
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    CrossClusterAccessSubjectInfo subjectInfo = subjectInfoByAction.getOrDefault(
                        action,
                        SystemUser.crossClusterAccessSubjectInfo(TransportVersion.current(), nodeName)
                    );
                    new CrossClusterAccessHeaders("ApiKey " + encodedApiKey, subjectInfo).writeToContext(threadContext, null);
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
}
