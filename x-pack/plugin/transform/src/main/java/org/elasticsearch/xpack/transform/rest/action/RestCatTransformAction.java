/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.common.table.TableColumnAttributeBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.transform.Transform;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.transform.TransformField.ALLOW_NO_MATCH;

@ServerlessScope(Scope.PUBLIC)
public class RestCatTransformAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_cat/transforms"), new Route(GET, "_cat/transforms/{" + TransformField.TRANSFORM_ID + "}"));
    }

    @Override
    public String getName() {
        return "cat_transform_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(TransformField.TRANSFORM_ID);
        if (Strings.isNullOrEmpty(id)) {
            id = Metadata.ALL;
        }

        GetTransformAction.Request request = new GetTransformAction.Request(id);
        request.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), true));

        GetTransformStatsAction.Request statsRequest = new GetTransformStatsAction.Request(id, null);
        statsRequest.setAllowNoMatch(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(), true));

        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            PageParams pageParams = new PageParams(
                restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)
            );
            request.setPageParams(pageParams);
            statsRequest.setPageParams(pageParams);
        }

        return channel -> client.execute(GetTransformAction.INSTANCE, request, new RestActionListener<>(channel) {
            @Override
            public void processResponse(GetTransformAction.Response response) {
                client.execute(GetTransformStatsAction.INSTANCE, statsRequest, new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(GetTransformStatsAction.Response statsResponse) throws Exception {
                        return RestTable.buildResponse(buildTable(response, statsResponse), channel);
                    }
                });
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/transforms\n");
        sb.append("/_cat/transforms/{" + TransformField.TRANSFORM_ID + "}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest unused) {
        return getTableWithHeader();
    }

    private static Table getTableWithHeader() {
        return new Table().startHeaders()
            // default columns
            .addCell("id", TableColumnAttributeBuilder.builder("the id").build())
            .addCell(
                "state",
                TableColumnAttributeBuilder.builder("transform state")
                    .setAliases("s")
                    .setTextAlignment(TableColumnAttributeBuilder.TextAlign.RIGHT)
                    .build()
            )
            .addCell("checkpoint", TableColumnAttributeBuilder.builder("checkpoint").setAliases("c").build())
            .addCell(
                "documents_processed",
                TableColumnAttributeBuilder.builder("the number of documents read from source indices and processed")
                    .setAliases("docp", "documentsProcessed")
                    .build()
            )
            .addCell(
                "checkpoint_progress",
                TableColumnAttributeBuilder.builder("progress of the checkpoint").setAliases("cp", "checkpointProgress").build()
            )
            .addCell(
                "last_search_time",
                TableColumnAttributeBuilder.builder("last time transform searched for updates").setAliases("lst", "lastSearchTime").build()
            )
            .addCell(
                "changes_last_detection_time",
                TableColumnAttributeBuilder.builder("changes last detected time").setAliases("cldt").build()
            )

            // optional columns
            .addCell(
                "create_time",
                TableColumnAttributeBuilder.builder("transform creation time", false).setAliases("ct", "createTime").build()
            )
            .addCell(
                "version",
                TableColumnAttributeBuilder.builder("the version of Elasticsearch when the transform was created", false)
                    .setAliases("v")
                    .build()
            )
            .addCell("source_index", TableColumnAttributeBuilder.builder("source index", false).setAliases("si", "sourceIndex").build())
            .addCell("dest_index", TableColumnAttributeBuilder.builder("destination index", false).setAliases("di", "destIndex").build())
            .addCell("pipeline", TableColumnAttributeBuilder.builder("transform pipeline", false).setAliases("p").build())
            .addCell("description", TableColumnAttributeBuilder.builder("description", false).setAliases("d").build())
            .addCell("transform_type", TableColumnAttributeBuilder.builder("batch or continuous transform", false).setAliases("tt").build())
            .addCell("frequency", TableColumnAttributeBuilder.builder("frequency of transform", false).setAliases("f").build())
            .addCell("max_page_search_size", TableColumnAttributeBuilder.builder("max page search size", false).setAliases("mpsz").build())
            .addCell("docs_per_second", TableColumnAttributeBuilder.builder("docs per second", false).setAliases("dps").build())

            .addCell("reason", TableColumnAttributeBuilder.builder("reason for the current state", false).setAliases("r", "reason").build())

            .addCell("search_total", TableColumnAttributeBuilder.builder("total number of search phases", false).setAliases("st").build())
            .addCell(
                "search_failure",
                TableColumnAttributeBuilder.builder("total number of search failures", false).setAliases("sf").build()
            )
            .addCell("search_time", TableColumnAttributeBuilder.builder("total search time", false).setAliases("stime").build())
            .addCell(
                "index_total",
                TableColumnAttributeBuilder.builder("total number of index phases done by the transform", false).setAliases("it").build()
            )
            .addCell("index_failure", TableColumnAttributeBuilder.builder("total number of index failures", false).setAliases("if").build())
            .addCell(
                "index_time",
                TableColumnAttributeBuilder.builder("total time spent indexing documents", false).setAliases("itime").build()
            )
            .addCell(
                "documents_indexed",
                TableColumnAttributeBuilder.builder("the number of documents written to the destination index", false)
                    .setAliases("doci")
                    .build()
            )
            .addCell(
                "delete_time",
                TableColumnAttributeBuilder.builder("total time spent deleting documents", false).setAliases("dtime").build()
            )
            .addCell(
                "documents_deleted",
                TableColumnAttributeBuilder.builder("the number of documents deleted from the destination index", false)
                    .setAliases("docd")
                    .build()
            )
            .addCell(
                "trigger_count",
                TableColumnAttributeBuilder.builder("the number of times the transform has been triggered", false).setAliases("tc").build()
            )
            .addCell(
                "pages_processed",
                TableColumnAttributeBuilder.builder("the number of pages processed", false).setAliases("pp").build()
            )
            .addCell(
                "processing_time",
                TableColumnAttributeBuilder.builder("the total time spent processing documents", false).setAliases("pt").build()
            )
            .addCell(
                "checkpoint_duration_time_exp_avg",
                TableColumnAttributeBuilder.builder("exponential average checkpoint processing time (milliseconds)", false)
                    .setAliases("cdtea", "checkpointTimeExpAvg")
                    .build()
            )
            .addCell(
                "indexed_documents_exp_avg",
                TableColumnAttributeBuilder.builder("exponential average number of documents indexed", false).setAliases("idea").build()
            )
            .addCell(
                "processed_documents_exp_avg",
                TableColumnAttributeBuilder.builder("exponential average number of documents processed", false).setAliases("pdea").build()
            )
            .endHeaders();
    }

    private Table buildTable(GetTransformAction.Response response, GetTransformStatsAction.Response statsResponse) {
        Table table = getTableWithHeader();
        Map<String, TransformStats> statsById = statsResponse.getTransformsStats()
            .stream()
            .collect(Collectors.toMap(TransformStats::getId, Function.identity()));
        response.getTransformConfigurations().forEach(config -> {
            TransformStats stats = statsById.get(config.getId());
            TransformCheckpointingInfo checkpointingInfo = null;
            TransformIndexerStats transformIndexerStats = null;

            if (stats != null) {
                checkpointingInfo = stats.getCheckpointingInfo();
                transformIndexerStats = stats.getIndexerStats();
            }

            Integer maxPageSearchSize = config.getSettings() == null || config.getSettings().getMaxPageSearchSize() == null
                ? config.getPivotConfig() == null || config.getPivotConfig().getMaxPageSearchSize() == null
                    ? Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE
                    : config.getPivotConfig().getMaxPageSearchSize()
                : config.getSettings().getMaxPageSearchSize();

            Double progress = checkpointingInfo == null ? null
                : checkpointingInfo.getNext().getCheckpointProgress() == null ? null
                : checkpointingInfo.getNext().getCheckpointProgress().getPercentComplete();

            table.startRow()
                // default columns
                .addCell(config.getId())
                .addCell(stats == null ? null : stats.getState().toString())
                .addCell(checkpointingInfo == null ? null : checkpointingInfo.getLast().getCheckpoint())
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getNumDocuments())
                .addCell(progress == null ? null : String.format(Locale.ROOT, "%.2f", progress))
                .addCell(
                    checkpointingInfo == null ? null
                        : checkpointingInfo.getLastSearchTime() == null ? null
                        : Date.from(checkpointingInfo.getLastSearchTime())
                )
                .addCell(
                    checkpointingInfo == null ? null
                        : checkpointingInfo.getChangesLastDetectedAt() == null ? null
                        : Date.from(checkpointingInfo.getChangesLastDetectedAt())
                )

                // optional columns
                .addCell(config.getCreateTime() == null ? null : Date.from(config.getCreateTime()))
                .addCell(config.getVersion())
                .addCell(String.join(",", config.getSource().getIndex()))
                .addCell(config.getDestination().getIndex())
                .addCell(config.getDestination().getPipeline())
                .addCell(config.getDescription())
                .addCell(config.getSyncConfig() == null ? "batch" : "continuous")
                .addCell(config.getFrequency() == null ? Transform.DEFAULT_TRANSFORM_FREQUENCY : config.getFrequency())
                .addCell(maxPageSearchSize)
                .addCell(
                    config.getSettings() == null || config.getSettings().getDocsPerSecond() == null
                        ? "-"
                        : config.getSettings().getDocsPerSecond()
                )
                .addCell(stats == null ? null : stats.getReason())
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getSearchTotal())
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getSearchFailures())
                .addCell(transformIndexerStats == null ? null : TimeValue.timeValueMillis(transformIndexerStats.getSearchTime()))

                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getIndexTotal())
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getIndexFailures())
                .addCell(transformIndexerStats == null ? null : TimeValue.timeValueMillis(transformIndexerStats.getIndexTime()))

                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getOutputDocuments())
                .addCell(transformIndexerStats == null ? null : TimeValue.timeValueMillis(transformIndexerStats.getDeleteTime()))
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getNumDeletedDocuments())
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getNumInvocations())
                .addCell(transformIndexerStats == null ? null : transformIndexerStats.getNumPages())
                .addCell(transformIndexerStats == null ? null : TimeValue.timeValueMillis(transformIndexerStats.getProcessingTime()))

                .addCell(
                    transformIndexerStats == null
                        ? null
                        : String.format(Locale.ROOT, "%.2f", transformIndexerStats.getExpAvgCheckpointDurationMs())
                )
                .addCell(
                    transformIndexerStats == null
                        ? null
                        : String.format(Locale.ROOT, "%.2f", transformIndexerStats.getExpAvgDocumentsIndexed())
                )
                .addCell(
                    transformIndexerStats == null
                        ? null
                        : String.format(Locale.ROOT, "%.2f", transformIndexerStats.getExpAvgDocumentsProcessed())
                )
                .endRow();
        });

        return table;
    }
}
