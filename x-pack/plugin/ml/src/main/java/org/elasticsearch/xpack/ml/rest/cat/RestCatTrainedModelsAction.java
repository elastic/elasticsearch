/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.xpack.core.common.table.TableColumnAttributeBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.security.user.XPackUser;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request.ALLOW_NO_MATCH;

public class RestCatTrainedModelsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "_cat/ml/trained_models"),
            new Route(GET, "_cat/ml/trained_models/{" + TrainedModelConfig.MODEL_ID.getPreferredName() + "}"));
    }

    @Override
    public String getName() {
        return "cat_ml_get_trained_models_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest restRequest, NodeClient client) {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        if (Strings.isNullOrEmpty(modelId)) {
            modelId = Metadata.ALL;
        }
        GetTrainedModelsStatsAction.Request statsRequest = new GetTrainedModelsStatsAction.Request(modelId);
        GetTrainedModelsAction.Request modelsAction = new GetTrainedModelsAction.Request(modelId, false, null);
        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            statsRequest.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
            modelsAction.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }
        statsRequest.setAllowNoResources(true);
        modelsAction.setAllowNoResources(restRequest.paramAsBoolean(ALLOW_NO_MATCH.getPreferredName(),
            statsRequest.isAllowNoResources()));

        return channel -> {
            final ActionListener<Table> listener = ActionListener.notifyOnce(new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(final Table table) throws Exception {
                    return RestTable.buildResponse(table, channel);
                }
            });

            client.execute(GetTrainedModelsAction.INSTANCE, modelsAction, ActionListener.wrap(
                trainedModels -> {
                    final List<TrainedModelConfig> trainedModelConfigs = trainedModels.getResources().results();

                    Set<String> potentialAnalyticsIds = new HashSet<>();
                    // Analytics Configs are created by the XPackUser
                    trainedModelConfigs.stream()
                        .filter(c -> XPackUser.NAME.equals(c.getCreatedBy()))
                        .forEach(c -> potentialAnalyticsIds.addAll(c.getTags()));


                    // Find the related DataFrameAnalyticsConfigs
                    String requestIdPattern = Strings.collectionToDelimitedString(potentialAnalyticsIds, "*,") + "*";

                    final GroupedActionListener<ActionResponse> groupedListener = createGroupedListener(restRequest,
                        2,
                        trainedModels.getResources().results(),
                        listener);

                    client.execute(GetTrainedModelsStatsAction.INSTANCE,
                        statsRequest,
                        ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure));

                    GetDataFrameAnalyticsAction.Request dataFrameAnalyticsRequest =
                        new GetDataFrameAnalyticsAction.Request(requestIdPattern);
                    dataFrameAnalyticsRequest.setAllowNoResources(true);
                    dataFrameAnalyticsRequest.setPageParams(new PageParams(0, potentialAnalyticsIds.size()));
                    client.execute(GetDataFrameAnalyticsAction.INSTANCE,
                        dataFrameAnalyticsRequest,
                        ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure));
                },
                listener::onFailure
            ));
        };
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/ml/trained_models\n");
        sb.append("/_cat/ml/trained_models/{model_id}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();

        // Trained Model Info
        table.addCell("id", TableColumnAttributeBuilder.builder("the trained model id").build());
        table.addCell("created_by", TableColumnAttributeBuilder.builder("who created the model", false)
            .setAliases("c", "createdBy")
            .setTextAlignment(TableColumnAttributeBuilder.TextAlign.RIGHT)
            .build());
        table.addCell("heap_size", TableColumnAttributeBuilder.builder("the estimated heap size to keep the model in memory")
            .setAliases("hs","modelHeapSize")
            .build());
        table.addCell("operations", TableColumnAttributeBuilder.builder("the estimated number of operations to use the model")
            .setAliases("o", "modelOperations")
            .build());
        table.addCell("license", TableColumnAttributeBuilder.builder("The license level of the model", false)
            .setAliases("l")
            .build());
        table.addCell("create_time", TableColumnAttributeBuilder.builder("The time the model was created")
            .setAliases("ct")
            .build());
        table.addCell("version", TableColumnAttributeBuilder.builder("The version of Elasticsearch when the model was created", false)
            .setAliases("v")
            .build());
        table.addCell("description", TableColumnAttributeBuilder.builder("The model description", false)
            .setAliases("d")
            .build());

        // Trained Model Stats
        table.addCell("ingest.pipelines", TableColumnAttributeBuilder.builder("The number of pipelines referencing the model")
            .setAliases("ip", "ingestPipelines")
            .build());
        table.addCell("ingest.count", TableColumnAttributeBuilder.builder("The total number of docs processed by the model", false)
            .setAliases("ic", "ingestCount")
            .build());
        table.addCell("ingest.time", TableColumnAttributeBuilder.builder(
            "The total time spent processing docs with this model",
            false)
            .setAliases("it", "ingestTime")
            .build());
        table.addCell("ingest.current", TableColumnAttributeBuilder.builder(
            "The total documents currently being handled by the model",
            false)
            .setAliases("icurr", "ingestCurrent")
            .build());
        table.addCell("ingest.failed", TableColumnAttributeBuilder.builder(
            "The total count of failed ingest attempts with this model",
            false)
            .setAliases("if", "ingestFailed")
            .build());

        table.addCell("data_frame.id", TableColumnAttributeBuilder.builder(
            "The data frame analytics config id that created the model (if still available)")
            .setAliases("dfid", "dataFrameAnalytics")
            .build());
        table.addCell("data_frame.create_time", TableColumnAttributeBuilder.builder(
            "The time the data frame analytics config was created",
            false)
            .setAliases("dft", "dataFrameAnalyticsTime")
            .build());
        table.addCell("data_frame.source_index", TableColumnAttributeBuilder.builder(
            "The source index used to train in the data frame analysis",
            false)
            .setAliases("dfsi", "dataFrameAnalyticsSrcIndex")
            .build());
        table.addCell("data_frame.analysis", TableColumnAttributeBuilder.builder(
            "The analysis used by the data frame to build the model",
            false)
            .setAliases("dfa", "dataFrameAnalyticsAnalysis")
            .build());

        table.endHeaders();
        return table;
    }

    private GroupedActionListener<ActionResponse> createGroupedListener(final RestRequest request,
                                                                        final int size,
                                                                        final List<TrainedModelConfig> configs,
                                                                        final ActionListener<Table> listener) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(final Collection<ActionResponse> responses) {
                GetTrainedModelsStatsAction.Response statsResponse = extractResponse(responses, GetTrainedModelsStatsAction.Response.class);
                GetDataFrameAnalyticsAction.Response analytics = extractResponse(responses, GetDataFrameAnalyticsAction.Response.class);
                listener.onResponse(buildTable(request,
                    statsResponse.getResources().results(),
                    configs,
                    analytics == null ? Collections.emptyList() : analytics.getResources().results()));
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        }, size);
    }


    private Table buildTable(RestRequest request,
                             List<GetTrainedModelsStatsAction.Response.TrainedModelStats> stats,
                             List<TrainedModelConfig> configs,
                             List<DataFrameAnalyticsConfig> analyticsConfigs) {
        Table table = getTableWithHeader(request);
        assert configs.size() == stats.size();

        Map<String, DataFrameAnalyticsConfig> analyticsMap = analyticsConfigs.stream()
            .collect(Collectors.toMap(DataFrameAnalyticsConfig::getId, Function.identity()));
        Map<String, GetTrainedModelsStatsAction.Response.TrainedModelStats> statsMap = stats.stream()
            .collect(Collectors.toMap(GetTrainedModelsStatsAction.Response.TrainedModelStats::getModelId, Function.identity()));

        configs.forEach(config -> {
            table.startRow();
            // Trained Model Info
            table.addCell(config.getModelId());
            table.addCell(config.getCreatedBy());
            table.addCell(new ByteSizeValue(config.getEstimatedHeapMemory()));
            table.addCell(config.getEstimatedOperations());
            table.addCell(config.getLicenseLevel());
            table.addCell(config.getCreateTime());
            table.addCell(config.getVersion().toString());
            table.addCell(config.getDescription());

            GetTrainedModelsStatsAction.Response.TrainedModelStats modelStats = statsMap.get(config.getModelId());
            table.addCell(modelStats.getPipelineCount());
            boolean hasIngestStats = modelStats != null && modelStats.getIngestStats() != null;
            table.addCell(hasIngestStats ? modelStats.getIngestStats().getTotalStats().getIngestCount() : 0);
            table.addCell(hasIngestStats ?
                TimeValue.timeValueMillis(modelStats.getIngestStats().getTotalStats().getIngestTimeInMillis()) :
                TimeValue.timeValueMillis(0));
            table.addCell(hasIngestStats ? modelStats.getIngestStats().getTotalStats().getIngestCurrent() : 0);
            table.addCell(hasIngestStats ? modelStats.getIngestStats().getTotalStats().getIngestFailedCount() : 0);

            DataFrameAnalyticsConfig dataFrameAnalyticsConfig = config.getTags()
                .stream()
                .filter(analyticsMap::containsKey)
                .map(analyticsMap::get)
                .findFirst()
                .orElse(null);
            table.addCell(dataFrameAnalyticsConfig == null ? "__none__" : dataFrameAnalyticsConfig.getId());
            table.addCell(dataFrameAnalyticsConfig == null ? null : dataFrameAnalyticsConfig.getCreateTime());
            table.addCell(dataFrameAnalyticsConfig == null ?
                null :
                Strings.arrayToCommaDelimitedString(dataFrameAnalyticsConfig.getSource().getIndex()));
            DataFrameAnalysis analysis = dataFrameAnalyticsConfig == null ? null : dataFrameAnalyticsConfig.getAnalysis();
            table.addCell(analysis == null ? null : analysis.getWriteableName());

            table.endRow();
        });
        return table;
    }

    @SuppressWarnings("unchecked")
    private static <A extends ActionResponse> A extractResponse(final Collection<? extends ActionResponse> responses, Class<A> c) {
        return (A) responses.stream().filter(c::isInstance).findFirst().get();
    }
}
