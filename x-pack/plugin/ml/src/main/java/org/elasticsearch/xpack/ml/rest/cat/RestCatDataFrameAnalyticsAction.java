/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.cat;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.xpack.core.common.table.TableColumnAttributeBuilder;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response.Stats;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCatDataFrameAnalyticsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "_cat/ml/data_frame/analytics/{" + DataFrameAnalyticsConfig.ID + "}"),
            new Route(GET, "_cat/ml/data_frame/analytics")
        );
    }

    @Override
    public String getName() {
        return "cat_ml_get_data_frame_analytics_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest restRequest, NodeClient client) {
        String dataFrameAnalyticsId = restRequest.param(DataFrameAnalyticsConfig.ID.getPreferredName());
        if (Strings.isNullOrEmpty(dataFrameAnalyticsId)) {
            dataFrameAnalyticsId = Metadata.ALL;
        }

        GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request(dataFrameAnalyticsId);
        getRequest.setAllowNoResources(
            restRequest.paramAsBoolean(
                GetDataFrameAnalyticsAction.Request.ALLOW_NO_MATCH.getPreferredName(),
                getRequest.isAllowNoResources()
            )
        );

        GetDataFrameAnalyticsStatsAction.Request getStatsRequest = new GetDataFrameAnalyticsStatsAction.Request(dataFrameAnalyticsId);
        getStatsRequest.setAllowNoMatch(true);

        return channel -> client.execute(GetDataFrameAnalyticsAction.INSTANCE, getRequest, new RestActionListener<>(channel) {
            @Override
            public void processResponse(GetDataFrameAnalyticsAction.Response getResponse) {
                client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, getStatsRequest, new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(GetDataFrameAnalyticsStatsAction.Response getStatsResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(getResponse, getStatsResponse), channel);
                    }
                });
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/ml/data_frame/analytics\n");
        sb.append("/_cat/ml/data_frame/analytics/{").append(DataFrameAnalyticsConfig.ID.getPreferredName()).append("}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest unused) {
        return getTableWithHeader();
    }

    private static Table getTableWithHeader() {
        return new Table().startHeaders()
            // DFA config info
            .addCell("id", TableColumnAttributeBuilder.builder("the id").build())
            .addCell("type", TableColumnAttributeBuilder.builder("analysis type").setAliases("t").build())
            .addCell("create_time", TableColumnAttributeBuilder.builder("job creation time").setAliases("ct", "createTime").build())
            .addCell(
                "version",
                TableColumnAttributeBuilder.builder("the version of Elasticsearch when the analytics was created", false)
                    .setAliases("v")
                    .build()
            )
            .addCell("source_index", TableColumnAttributeBuilder.builder("source index", false).setAliases("si", "sourceIndex").build())
            .addCell("dest_index", TableColumnAttributeBuilder.builder("destination index", false).setAliases("di", "destIndex").build())
            .addCell("description", TableColumnAttributeBuilder.builder("description", false).setAliases("d").build())
            .addCell(
                "model_memory_limit",
                TableColumnAttributeBuilder.builder("model memory limit", false).setAliases("mml", "modelMemoryLimit").build()
            )
            // DFA stats info
            .addCell(
                "state",
                TableColumnAttributeBuilder.builder("job state")
                    .setAliases("s")
                    .setTextAlignment(TableColumnAttributeBuilder.TextAlign.RIGHT)
                    .build()
            )
            .addCell(
                "failure_reason",
                TableColumnAttributeBuilder.builder("failure reason", false).setAliases("fr", "failureReason").build()
            )
            .addCell("progress", TableColumnAttributeBuilder.builder("progress", false).setAliases("p").build())
            .addCell(
                "assignment_explanation",
                TableColumnAttributeBuilder.builder("why the job is or is not assigned to a node", false)
                    .setAliases("ae", "assignmentExplanation")
                    .build()
            )
            // Node info
            .addCell("node.id", TableColumnAttributeBuilder.builder("id of the assigned node", false).setAliases("ni", "nodeId").build())
            .addCell(
                "node.name",
                TableColumnAttributeBuilder.builder("name of the assigned node", false).setAliases("nn", "nodeName").build()
            )
            .addCell(
                "node.ephemeral_id",
                TableColumnAttributeBuilder.builder("ephemeral id of the assigned node", false).setAliases("ne", "nodeEphemeralId").build()
            )
            .addCell(
                "node.address",
                TableColumnAttributeBuilder.builder("network address of the assigned node", false).setAliases("na", "nodeAddress").build()
            )
            .endHeaders();
    }

    private static Table buildTable(
        GetDataFrameAnalyticsAction.Response getResponse,
        GetDataFrameAnalyticsStatsAction.Response getStatsResponse
    ) {
        Map<String, Stats> statsById = getStatsResponse.getResponse().results().stream().collect(toMap(Stats::getId, Function.identity()));
        Table table = getTableWithHeader();
        for (DataFrameAnalyticsConfig config : getResponse.getResources().results()) {
            Stats stats = statsById.get(config.getId());
            DiscoveryNode node = stats == null ? null : stats.getNode();
            table.startRow()
                .addCell(config.getId())
                .addCell(config.getAnalysis().getWriteableName())
                .addCell(config.getCreateTime())
                .addCell(config.getVersion())
                .addCell(String.join(",", config.getSource().getIndex()))
                .addCell(config.getDest().getIndex())
                .addCell(config.getDescription())
                .addCell(config.getModelMemoryLimit())
                .addCell(stats == null ? null : stats.getState())
                .addCell(stats == null ? null : stats.getFailureReason())
                .addCell(stats == null ? null : progressToString(stats.getProgress()))
                .addCell(stats == null ? null : stats.getAssignmentExplanation())
                .addCell(node == null ? null : node.getId())
                .addCell(node == null ? null : node.getName())
                .addCell(node == null ? null : node.getEphemeralId())
                .addCell(node == null ? null : node.getAddress().toString())
                .endRow();
        }
        return table;
    }

    private static String progressToString(List<PhaseProgress> phases) {
        return phases.stream().map(p -> p.getPhase() + ":" + p.getProgressPercent()).collect(joining(","));
    }
}
