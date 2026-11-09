/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * A {@link DataNodeRequest} is accepted from any holder of a transport credential, and the {@link ExternalSourceExec}
 * it carries names its storage config, source path and splits. Both entry points that run such a plan,
 * {@code DataNodeComputeHandler.handleExternalSourceRequest} for a request made of splits and
 * {@code ComputeService.runCompute} for one made of shards, must rebind the exec to the dataset registered in this
 * node's cluster state before anything reads the plan's config. These tests send crafted requests straight to the
 * data node action, naming a registered dataset but carrying someone else's credentials, and assert the refusal
 * only the rebind produces: the plan's config is never what gets read.
 */
public class DataNodeRequestDatasetRebindIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /**
     * Splits in the request and no shards: the external request path. The plan carries a definition version the
     * registry does not have, so the rebind, and only the rebind, refuses the request before it starts.
     */
    public void testExternalRequestIsReboundToTheRegisteredDataset() throws Exception {
        Path csv = createTempDir().resolve("rows.csv");
        Files.writeString(csv, "a\n1\n2\n", StandardCharsets.UTF_8);
        String dataset = registerDataset("rebind_external", StoragePath.fileUri(csv), Map.of("format", "csv"));
        String node = randomFrom(internalCluster().getNodeNames());
        String sessionId = "rebind-external-" + randomAlphaOfLength(8);
        ExchangeService exchange = internalCluster().getInstance(ExchangeService.class, node);
        // The coordinator opens the sink before it sends the request; without one the handler answers with an empty result.
        exchange.createSinkHandler(sessionId, 1);
        try {
            ExternalSourceExec exec = hostileExec(dataset, csv, Map.of(DefinitionVersion.CONFIG_KEY, "not-the-registered-version"));
            DataNodeRequest request = request(sessionId, exec, List.of(), new String[0], exec.splits());

            Exception e = expectThrows(Exception.class, () -> send(node, request));
            assertThat(
                ExceptionsHelper.unwrapCause(e).getMessage(),
                containsString("external dataset [" + dataset + "] definition changed while the query was planned")
            );
        } finally {
            exchange.finishSinkHandler(sessionId, null);
        }
    }

    /**
     * A shard in the request and the exec inside its plan: the shard path through {@code runCompute}. The split names
     * a file the registered dataset does not cover, so the rebind refuses it; the failure lands on the shard, as any
     * compute failure on this path does.
     */
    public void testShardRequestCarryingAnExternalSourceIsReboundToTheRegisteredDataset() throws Exception {
        Path csv = createTempDir().resolve("rows.csv");
        Files.writeString(csv, "a\n1\n2\n", StandardCharsets.UTF_8);
        Path outside = createTempDir().resolve("secret.csv");
        Files.writeString(outside, "a\n3\n", StandardCharsets.UTF_8);
        String dataset = registerDataset("rebind_shard", StoragePath.fileUri(csv), Map.of("format", "csv"));
        // The index is pinned to the node the request goes to, so that node must be able to hold its shard.
        String node = internalCluster().getRandomDataNodeName();
        String index = "rebind_shard_idx";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", node)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("a", "type=keyword")
        );
        ensureGreen(index);
        String sessionId = "rebind-shard-" + randomAlphaOfLength(8);
        ExchangeService exchange = internalCluster().getInstance(ExchangeService.class, node);
        exchange.createSinkHandler(sessionId, 1);
        try {
            ExternalSourceExec exec = hostileExec(dataset, outside, Map.of());
            DataNodeRequest request = request(
                sessionId,
                exec,
                List.of(new DataNodeRequest.Shard(new ShardId(resolveIndex(index), 0), SplitShardCountSummary.UNSET)),
                new String[] { index },
                List.of()
            );

            String failure;
            try {
                DataNodeComputeResponse response = send(node, request);
                assertThat("the refusal is reported on the shard", response.shardLevelFailures().values(), hasSize(1));
                failure = ExceptionsHelper.unwrapCause(response.shardLevelFailures().values().iterator().next()).getMessage();
            } catch (Exception e) {
                failure = ExceptionsHelper.unwrapCause(e).getMessage();
            }
            assertThat(
                failure,
                containsString("external dataset [" + dataset + "] does not cover split [" + StoragePath.fileUri(outside) + "]")
            );
        } finally {
            exchange.finishSinkHandler(sessionId, null);
        }
    }

    /** An exec naming {@code dataset} but carrying another identity's storage config and a split of its own choosing. */
    private static ExternalSourceExec hostileExec(String dataset, Path file, Map<String, Object> extraConfig) throws Exception {
        Map<String, Object> config = new HashMap<>(
            Map.of(
                "format",
                "csv",
                "auth",
                "federated_identity",
                "role_arn",
                "arn:aws:iam::123456789012:role/attacker",
                "endpoint",
                "https://attacker.example"
            )
        );
        config.putAll(extraConfig);
        List<Attribute> output = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://attacker/whatever/*.csv",
            "csv",
            output,
            config,
            Map.of(),
            null,
            10
        );
        List<ExternalSplit> splits = List.of(
            new FileSplit("csv", StoragePath.ofLocalPath(file), 0, Files.size(file), "csv", Map.of(), Map.of())
        );
        return exec.withDatasetName(dataset).withSplits(splits);
    }

    private static DataNodeRequest request(
        String sessionId,
        ExternalSourceExec exec,
        List<DataNodeRequest.Shard> shards,
        String[] indices,
        List<ExternalSplit> externalSplits
    ) {
        Configuration configuration = TEST_CFG;
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, exec.output(), false, exec);
        return new DataNodeRequest(
            sessionId,
            configuration,
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            shards,
            Map.of(),
            plan,
            indices,
            IndicesOptions.strictExpandOpen(),
            false,
            false,
            false,
            externalSplits
        );
    }

    /** Sends the request to {@code node}'s own data node action, as a coordinator would, and waits for its answer. */
    private DataNodeComputeResponse send(String node, DataNodeRequest request) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, node);
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        // The data node action insists on a parent task; a coordinator's query task would be it.
        request.setParentTask(new TaskId(transportService.getLocalNode().getId(), randomNonNegativeLong()));
        PlainActionFuture<DataNodeComputeResponse> future = new PlainActionFuture<>();
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
        return future.actionGet(TIMEOUT);
    }
}
