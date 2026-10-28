/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * A plan carrying an external source is accepted only in the request shapes a coordinator sends. These tests send the
 * shapes no coordinator produces straight to the transport actions and assert they are refused before any planning,
 * whatever the dataset: a data node request with neither shards nor splits, a remote-cluster compute (datasets are
 * local to the cluster that registered them), and a request naming a dataset this node never registered. They run
 * without security, where the authorization round-trip resolves against the registry alone.
 */
public class ExternalSourceRequestShapeIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testExternalNodeWithNeitherShardsNorSplitsIsRefused() throws Exception {
        Path csv = createTempDir().resolve("rows.csv");
        Files.writeString(csv, "a\n1\n2\n", StandardCharsets.UTF_8);
        String dataset = registerDataset("shape_empty", StoragePath.fileUri(csv), Map.of("format", "csv"));
        String node = randomFrom(internalCluster().getNodeNames());
        ExternalSourceExec exec = exec(dataset, csv);
        DataNodeRequest request = new DataNodeRequest(
            "shape-empty-" + randomAlphaOfLength(8),
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
            List.of()
        );

        ElasticsearchStatusException e = expectRefusal(node, ComputeService.DATA_ACTION_NAME, request, in -> {
            throw new AssertionError("no response expected");
        });
        assertThat(e.getMessage(), containsString("cannot be read by a data node request with neither shards nor external splits"));
    }

    public void testRemoteClusterComputeWithAnExternalNodeIsRefused() throws Exception {
        Path csv = createTempDir().resolve("rows.csv");
        Files.writeString(csv, "a\n1\n2\n", StandardCharsets.UTF_8);
        String dataset = registerDataset("shape_remote", StoragePath.fileUri(csv), Map.of("format", "csv"));
        String node = randomFrom(internalCluster().getNodeNames());
        ExternalSourceExec exec = exec(dataset, csv);
        String[] indices = new String[] { "some_index" };
        ClusterComputeRequest request = new ClusterComputeRequest(
            "querying_cluster",
            "shape-remote-" + randomAlphaOfLength(8),
            TEST_CFG,
            new RemoteClusterPlan(
                new ExchangeSinkExec(Source.EMPTY, exec.output(), false, exec),
                indices,
                new OriginalIndices(indices, IndicesOptions.strictExpandOpen())
            )
        );

        ElasticsearchStatusException e = expectRefusal(node, ComputeService.CLUSTER_ACTION_NAME, request, in -> {
            throw new AssertionError("no response expected");
        });
        assertThat(e.getMessage(), containsString("local to the cluster that registered them"));
    }

    public void testUnregisteredDatasetIsRefusedBeforePlanning() throws Exception {
        Path csv = createTempDir().resolve("rows.csv");
        Files.writeString(csv, "a\n1\n2\n", StandardCharsets.UTF_8);
        String node = randomFrom(internalCluster().getNodeNames());
        ExternalSourceExec exec = exec("shape_never_registered", csv);
        DataNodeRequest request = new DataNodeRequest(
            "shape-unregistered-" + randomAlphaOfLength(8),
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

        ElasticsearchStatusException e = expectRefusal(node, ComputeService.DATA_ACTION_NAME, request, in -> {
            throw new AssertionError("no response expected");
        });
        assertThat(e.getMessage(), containsString("external dataset [shape_never_registered] is not registered on this node"));
    }

    private static ExternalSourceExec exec(String dataset, Path file) throws Exception {
        List<Attribute> output = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            StoragePath.fileUri(file),
            "csv",
            output,
            Map.of("format", "csv"),
            Map.of(),
            null,
            10
        );
        List<ExternalSplit> splits = List.of(
            new FileSplit("csv", StoragePath.ofLocalPath(file), 0, Files.size(file), "csv", Map.of(), Map.of())
        );
        return exec.withDatasetName(dataset).withSplits(splits);
    }

    /** Sends {@code request} to {@code node}'s own handler, as a coordinator would, and returns the refusal it answers with. */
    private <T extends TransportResponse> ElasticsearchStatusException expectRefusal(
        String node,
        String action,
        AbstractTransportRequest request,
        Writeable.Reader<T> reader
    ) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, node);
        request.setParentTask(new TaskId(transportService.getLocalNode().getId(), randomNonNegativeLong()));
        PlainActionFuture<T> future = new PlainActionFuture<>();
        transportService.sendRequest(
            transportService.getLocalNode(),
            action,
            request,
            new ActionListenerResponseHandler<>(future, reader, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        Exception e = expectThrows(Exception.class, () -> future.actionGet(TIMEOUT));
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertThat(cause, instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException refusal = (ElasticsearchStatusException) cause;
        assertThat(refusal.status(), is(RestStatus.BAD_REQUEST));
        return refusal;
    }
}
