/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * A {@link DataNodeRequest} is authorized by the security filter on the {@code indices()} its sender chose, which says
 * nothing about the dataset the {@link ExternalSourceExec} inside its plan names. The data node therefore decides for
 * itself, from the plan, whether the request's credential may read that dataset, before it plans or rebinds anything
 * ({@code DataNodeComputeHandler.messageReceived}). These tests send crafted requests straight to the data node action
 * under users whose only difference is which dataset their role grants {@code read} on, and assert that the dataset
 * is read when and only when the grant covers it, in both request shapes a coordinator sends (external splits, and
 * shards with a plan that also reads a dataset).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class DataNodeRequestDatasetAuthorizationIT extends SecurityIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final SecureString PASSWORD = new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray());

    /** {@code read} on {@link #DATASET_A} and on {@link #INDEX}; nothing on {@link #DATASET_B}. */
    private static final String READER = "ds_reader";
    /** {@code read} on {@code pattern_*}, which covers {@link #DATASET_PATTERN} only by wildcard. */
    private static final String PATTERN_READER = "ds_pattern_reader";
    /** {@code read} on {@link #DATASET_A} under a DLS query. */
    private static final String DLS_READER = "ds_dls_reader";
    /** {@code read} on {@link #OTHER_INDEX} only: a credential allowed to send the compute action, with no dataset. */
    private static final String INDEX_ONLY_READER = "ds_index_only_reader";

    private static final String DATA_SOURCE = "authz_ds";
    private static final String DATASET_A = "authz_a";
    private static final String DATASET_B = "authz_b";
    private static final String DATASET_PATTERN = "pattern_authz";
    private static final String INDEX = "authz_idx";
    private static final String OTHER_INDEX = "authz_other_idx";

    private static Path fileA;
    private static Path fileB;
    private static Path filePattern;

    @Override
    protected String configUsers() {
        StringBuilder users = new StringBuilder(super.configUsers());
        for (String user : List.of(READER, PATTERN_READER, DLS_READER, INDEX_ONLY_READER)) {
            users.append(user).append(':').append(SecuritySettingsSource.TEST_PASSWORD_HASHED).append('\n');
        }
        return users.toString();
    }

    @Override
    protected String configUsersRoles() {
        StringBuilder usersRoles = new StringBuilder(super.configUsersRoles());
        for (String user : List.of(READER, PATTERN_READER, DLS_READER, INDEX_ONLY_READER)) {
            usersRoles.append(user).append(':').append(user).append('\n');
        }
        return usersRoles.toString();
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + """
            ds_reader:
              indices:
                - names: [ 'authz_a', 'authz_idx' ]
                  privileges: [ 'read' ]
            ds_pattern_reader:
              indices:
                - names: [ 'pattern_*' ]
                  privileges: [ 'read' ]
            ds_dls_reader:
              indices:
                - names: [ 'authz_a' ]
                  privileges: [ 'read' ]
                  query: '{"match_all": {}}'
            ds_index_only_reader:
              indices:
                - names: [ 'authz_other_idx' ]
                  privileges: [ 'read' ]
            """;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // The same wiring as AbstractExternalDataSourceIT, on top of the security plugins the base class installs.
        plugins.add(TestEncryptionServicePlugin.class);
        plugins.add(AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(AbstractExternalDataSourceIT.TestDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey(), false)
            .put(Federation.FEDERATION_ENABLED.getKey(), true)
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    /**
     * Three datasets over three one-column CSV files, registered once with the superuser-like test user; the index
     * the mixed-shape requests and queries read is recreated per test, as the cluster wipe drops it.
     */
    @Before
    public void setupFixtures() throws Exception {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        if (fileA == null) {
            Path dir = createTempDir();
            fileA = Files.writeString(dir.resolve("a.csv"), "a\na1\na2\n", StandardCharsets.UTF_8);
            fileB = Files.writeString(dir.resolve("b.csv"), "a\nb1\n", StandardCharsets.UTF_8);
            filePattern = Files.writeString(dir.resolve("pattern.csv"), "a\np1\n", StandardCharsets.UTF_8);
        }
        registerDataSource();
        registerDataset(DATASET_A, fileA);
        registerDataset(DATASET_B, fileB);
        registerDataset(DATASET_PATTERN, filePattern);
        for (String index : List.of(INDEX, OTHER_INDEX)) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping("a", "type=keyword")
            );
        }
        client().prepareIndex(INDEX).setId("1").setSource("a", "idx1").get();
        client().admin().indices().prepareRefresh(INDEX).get();
        ensureGreen(INDEX, OTHER_INDEX);
    }

    public void testExternalRequestNamingAnUnauthorizedDatasetIsRefused() throws Exception {
        assertRefused(sendExternal(READER, exec(DATASET_B, fileB)), DATASET_B);
    }

    public void testExternalRequestNamingAnAuthorizedDatasetReadsItsRegisteredDefinition() throws Exception {
        // The exec carries a hostile config and another path; what is read is the registered definition of DATASET_A.
        assertThat(sendExternal(READER, exec(DATASET_A, fileA)).expectRows(), contains("a1", "a2"));
    }

    public void testUnregisteredDatasetIsRefusedLikeAnUnauthorizedOne() throws Exception {
        Path never = createTempDir().resolve("never.csv");
        Files.writeString(never, "a\nn1\n", StandardCharsets.UTF_8);
        assertRefused(sendExternal(READER, exec("authz_never_registered", never)), "authz_never_registered");
    }

    public void testCredentialWithReadOnAnIndexButNotTheDatasetIsRefused() throws Exception {
        // Allowed to send the compute action (read on an index), so the security filter lets the request through.
        assertRefused(sendExternal(INDEX_ONLY_READER, exec(DATASET_A, fileA)), DATASET_A);
    }

    public void testRolePatternCoveringTheDatasetNameAuthorizesIt() throws Exception {
        assertThat(sendExternal(PATTERN_READER, exec(DATASET_PATTERN, filePattern)).expectRows(), contains("p1"));
        assertRefused(sendExternal(PATTERN_READER, exec(DATASET_A, fileA)), DATASET_A);
    }

    public void testDlsOnTheDatasetIsRefused() throws Exception {
        assertRefused(sendExternal(DLS_READER, exec(DATASET_A, fileA)), DATASET_A);
    }

    public void testShardRequestCarryingAnExternalSourceIsAuthorizedByTheDataset() throws Exception {
        // READER may read INDEX, so the shard request passes the security filter in both cases; only the dataset differs.
        assertThat(sendShard(READER, exec(DATASET_A, fileA)).expectRows(), contains("a1", "a2"));
        assertRefused(sendShard(READER, exec(DATASET_B, fileB)), DATASET_B);
    }

    /**
     * The shape the shard path receives from a coordinator for {@code FROM idx, dataset}: the dataset is still a
     * logical {@link ExternalRelation} inside the fragment. The name is found there too, and the accepted request
     * reads the registered definition: the relation carries no splits, so the node lists the registered resource.
     */
    public void testShardRequestCarryingAnExternalRelationIsAuthorizedByTheDataset() throws Exception {
        assertRefused(sendShard(READER, fragment(DATASET_B)), DATASET_B);
        assertThat(sendShard(READER, fragment(DATASET_A)).expectRows(), contains("a1", "a2"));
    }

    public void testCrossClusterAccessAuthenticationIsRefused() throws Exception {
        // A cross-cluster credential whose grants cover the dataset name by pattern would otherwise be authorized.
        Outcome outcome = send(crossClusterAccessAuthentication(), externalRequest(exec(DATASET_A, fileA)));
        ElasticsearchStatusException refusal = outcome.refusal();
        assertThat(refusal.getMessage(), containsString("local to the cluster that registered them"));
    }

    public void testMalformedDatasetNamesAreRefused() throws Exception {
        for (String name : List.of("", "*", "-" + DATASET_A, DATASET_A + ",*", "remote:" + DATASET_A)) {
            assertRefused(sendExternal(READER, exec(name, fileA)), name);
        }
    }

    public void testQueryingTheDatasetThroughTheCoordinatorStillWorks() throws Exception {
        // Regression check that the coordinator's gate and the receiver's agree: the coordinator is the single data
        // node, so its own data node request also passes through the receiver gate on the way to the rows. The refusal
        // for INDEX_ONLY_READER is the coordinator's (Unknown index); the receiver gate is proven by the crafted cases.
        try (EsqlQueryResponse response = query(READER, "FROM " + DATASET_A + " | SORT a")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a1"), List.of("a2"))));
        }
        try (EsqlQueryResponse response = query(READER, "FROM " + INDEX + ", " + DATASET_A + " | STATS c = COUNT(*)")) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        Exception e = expectThrows(Exception.class, () -> query(INDEX_ONLY_READER, "FROM " + DATASET_A).close());
        assertThat(ExceptionsHelper.unwrapCause(e).getMessage(), containsString("Unknown index [" + DATASET_A + "]"));
    }

    /** What a request answered with: the rows it delivered to the exchange, or the refusal it ended with. */
    private record Outcome(List<String> rows, Exception failure) {
        ElasticsearchStatusException refusal() {
            assertNotNull("expected a refusal but got rows " + rows, failure);
            Throwable cause = ExceptionsHelper.unwrapCause(failure);
            assertThat(cause, instanceOf(ElasticsearchStatusException.class));
            ElasticsearchStatusException refusal = (ElasticsearchStatusException) cause;
            assertThat(refusal.status(), is(RestStatus.BAD_REQUEST));
            return refusal;
        }

        List<String> expectRows() {
            if (failure != null) {
                throw new AssertionError("expected rows but the request failed", failure);
            }
            return rows;
        }
    }

    private static void assertRefused(Outcome outcome, String dataset) {
        assertThat(outcome.refusal().getMessage(), containsString("external dataset [" + dataset + "] is not registered on this node"));
    }

    private Outcome sendExternal(String user, ExternalSourceExec exec) {
        return send(user, externalRequest(exec));
    }

    private Outcome sendShard(String user, ExternalSourceExec exec) {
        return send(user, shardRequest(new ExchangeSinkExec(Source.EMPTY, exec.output(), false, exec)));
    }

    private Outcome sendShard(String user, FragmentExec fragment) {
        return send(user, shardRequest(new ExchangeSinkExec(Source.EMPTY, fragment.output(), false, fragment)));
    }

    /**
     * A transport request between nodes carries the authentication the coordinator established, not the credentials;
     * the security sender interceptor insists on one being in the context. Each test user is a file-realm user whose
     * single role has the user's name, so the authentication written here is what a coordinator would have forwarded.
     */
    private Outcome send(String user, DataNodeRequest request) {
        Authentication authentication = Authentication.newRealmAuthentication(
            new User(user, user),
            new Authentication.RealmRef("file", "file", "coordinator")
        );
        return send(authentication, request);
    }

    private Outcome send(Authentication authentication, DataNodeRequest request) {
        return send(threadContext -> authentication.writeToContext(threadContext), request);
    }

    /**
     * Sends {@code request} to the data node's own action, as a coordinator would, with the credential {@code credential}
     * puts in the thread context, and drains the exchange the request writes to.
     */
    private Outcome send(CheckedConsumer<ThreadContext> credential, DataNodeRequest request) {
        TransportService transportService = internalCluster().getDataNodeInstances(TransportService.class).iterator().next();
        ExchangeService exchange = internalCluster().getInstance(ExchangeService.class, transportService.getLocalNode().getName());
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        String sessionId = request.sessionId();
        // The coordinator opens the sink before it sends the request; without one the handler answers with an empty result.
        ExchangeSinkHandler sink = exchange.createSinkHandler(sessionId, randomIntBetween(1, 4));
        try {
            request.setParentTask(new TaskId(transportService.getLocalNode().getId(), randomNonNegativeLong()));
            PlainActionFuture<DataNodeComputeResponse> future = new PlainActionFuture<>();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                credential.accept(threadContext);
                transportService.sendRequest(
                    transportService.getLocalNode(),
                    ComputeService.DATA_ACTION_NAME,
                    request,
                    new ActionListenerResponseHandler<>(
                        future,
                        in -> new DataNodeComputeResponse(in, threadContext),
                        EsExecutors.DIRECT_EXECUTOR_SERVICE
                    )
                );
            } catch (Exception e) {
                return new Outcome(List.of(), e);
            }
            List<String> rows = new ArrayList<>();
            Exception drainFailure = null;
            try {
                drain(sink, rows);
            } catch (Exception e) {
                // A refused request fails its sink; the failure the request answers with is the one that matters.
                drainFailure = e;
            }
            try {
                DataNodeComputeResponse response = future.actionGet(TIMEOUT);
                assertThat("no shard-level failure expected", response.shardLevelFailures().values(), hasSize(0));
                if (drainFailure != null) {
                    throw new AssertionError("the request succeeded but its pages could not be fetched", drainFailure);
                }
                return new Outcome(rows, null);
            } catch (Exception e) {
                return new Outcome(rows, e);
            }
        } finally {
            // A refused request has already failed and removed its sink; one that never reached the handler has not.
            exchange.finishSinkHandler(sessionId, sink.isFinished() ? null : new IllegalStateException("test finished"));
        }
    }

    /** What one fetch from the sink delivered; the page is taken inside the fetch callback, before the sink releases the response. */
    private record Fetched(@Nullable Page page, boolean finished) {}

    /** Fetches pages the way a coordinator's exchange source does, until the sink reports it has no more. */
    private static void drain(ExchangeSinkHandler sink, List<String> rows) {
        while (true) {
            PlainActionFuture<Fetched> fetched = new PlainActionFuture<>();
            sink.fetchPageAsync(false, fetched.map(response -> new Fetched(response.takePage(), response.finished())));
            Fetched result = fetched.actionGet(TIMEOUT);
            Page page = result.page();
            if (page != null) {
                try {
                    BytesRefBlock block = page.getBlock(0);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        rows.add(block.getBytesRef(block.getFirstValueIndex(p), new BytesRef()).utf8ToString());
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
            if (result.finished()) {
                return;
            }
        }
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T value) throws Exception;
    }

    private static DataNodeRequest externalRequest(ExternalSourceExec exec) {
        return new DataNodeRequest(
            "authz-external-" + randomAlphaOfLength(8),
            TEST_CFG,
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            List.of(),
            Map.of(),
            new ExchangeSinkExec(Source.EMPTY, exec.output(), false, exec),
            new String[0],
            IndicesOptions.strictExpandOpen(),
            false,
            false,
            false,
            exec.splits()
        );
    }

    private static DataNodeRequest shardRequest(ExchangeSinkExec plan) {
        return new DataNodeRequest(
            "authz-shard-" + randomAlphaOfLength(8),
            TEST_CFG,
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            List.of(new DataNodeRequest.Shard(new ShardId(resolveIndex(INDEX), 0), SplitShardCountSummary.UNSET)),
            Map.of(),
            plan,
            new String[] { INDEX },
            IndicesOptions.strictExpandOpen(),
            false,
            false,
            false,
            List.of()
        );
    }

    /** The logical shape of {@code dataset} as a coordinator ships it inside a fragment, under the same hostile config. */
    private static FragmentExec fragment(String dataset) {
        List<Attribute> output = output();
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(
            output,
            "csv",
            "s3://attacker/whatever/*.csv",
            null,
            null,
            Map.of(),
            config()
        );
        return new FragmentExec(
            new ExternalRelation(Source.EMPTY, "s3://attacker/whatever/*.csv", metadata, output, FileList.UNRESOLVED, Map.of(), dataset)
        );
    }

    private static List<Attribute> output() {
        return List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
    }

    private static Map<String, Object> config() {
        return Map.of(
            "format",
            "csv",
            "auth",
            "federated_identity",
            "role_arn",
            "arn:aws:iam::123456789012:role/attacker",
            "endpoint",
            "https://attacker.example"
        );
    }

    /** An exec naming {@code dataset} but carrying another identity's storage config, and a split over {@code file}. */
    private static ExternalSourceExec exec(String dataset, Path file) throws Exception {
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://attacker/whatever/*.csv",
            "csv",
            output(),
            config(),
            Map.of(),
            null,
            10
        );
        List<ExternalSplit> splits = List.of(
            new FileSplit("csv", StoragePath.ofLocalPath(file), 0, Files.size(file), "csv", Map.of(), Map.of())
        );
        return exec.withDatasetName(dataset).withSplits(splits);
    }

    /**
     * What a fulfilling cluster sees for a request a querying cluster sent with a cross-cluster API key: an API key
     * subject whose grants, and whose querying user's grants, both cover every index name.
     */
    private static Authentication crossClusterAccessAuthentication() throws Exception {
        Map<String, Object> apiKeyMetadata = Map.of(
            AuthenticationField.API_KEY_ID_KEY,
            "cross-cluster-key-id",
            AuthenticationField.API_KEY_NAME_KEY,
            "cross-cluster-key",
            AuthenticationField.API_KEY_TYPE_KEY,
            "cross_cluster",
            AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
            new BytesArray("""
                {
                  "cross_cluster": {
                    "cluster": ["cross_cluster_search"],
                    "indices": [ { "names": ["*"], "privileges": ["read", "read_cross_cluster", "view_index_metadata"] } ]
                  }
                }"""),
            AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
            new BytesArray("{}")
        );
        Authentication apiKey = Authentication.newApiKeyAuthentication(
            AuthenticationResult.success(new User("cross-cluster-key-user"), apiKeyMetadata),
            "fulfilling-node"
        );
        Authentication queryingUser = Authentication.newRealmAuthentication(
            new User("querying-user", "querying-role"),
            new Authentication.RealmRef("file", "file", "querying-node")
        );
        RoleDescriptor queryingGrant = new RoleDescriptor(
            "querying-role",
            null,
            new RoleDescriptor.IndicesPrivileges[] { RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("read").build() },
            null
        );
        return apiKey.toCrossClusterAccess(new CrossClusterAccessSubjectInfo(queryingUser, new RoleDescriptorsIntersection(queryingGrant)));
    }

    private EsqlQueryResponse query(String user, String query) {
        return client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(user, PASSWORD)))
            .execute(EsqlQueryAction.INSTANCE, EsqlQueryRequest.syncEsqlQueryRequest(query))
            .actionGet(TIMEOUT);
    }

    /** A data source PUT is an upsert, so repeating it across the suite's tests is fine. */
    private void registerDataSource() {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATA_SOURCE, "test", null, new HashMap<>())
            )
        );
    }

    private void registerDataset(String name, Path file) {
        try {
            assertAcked(
                client().execute(
                    PutDatasetAction.INSTANCE,
                    new PutDatasetAction.Request(
                        TIMEOUT,
                        TIMEOUT,
                        name,
                        DATA_SOURCE,
                        StoragePath.fileUri(file),
                        null,
                        new HashMap<>(Map.of("format", "csv"))
                    )
                )
            );
        } catch (ResourceAlreadyExistsException e) {
            // registered by an earlier test of this suite
        }
    }
}
