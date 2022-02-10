/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.downsample.DownsampleDateHistogramConfig;
import org.elasticsearch.xpack.core.downsample.action.DownSampleAction;
import org.elasticsearch.xpack.core.downsample.action.DownSampleAction.Request;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig.CalendarInterval;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig.FixedInterval;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;

public class TransportDownSampleAction extends AcknowledgedTransportMasterNodeAction<DownSampleAction.Request> {
    private final Client client;

    @Inject
    public TransportDownSampleAction(
        Client client,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RollupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DownSampleAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        IndexMetadata originalIndexMetadata = state.getMetadata().index(request.getSourceIndex());
        if (originalIndexMetadata == null) {
            throw new ResourceNotFoundException("source index [{" + request.getSourceIndex() + "}] not found");
        }

        if (IndexSettings.MODE.get(originalIndexMetadata.getSettings()) != IndexMode.TIME_SERIES) {
            throw new UnsupportedOperationException("downsample is support for time_series index");
        }

        RollupActionConfig rollupActionConfig = buildRollupActionConfig(request, originalIndexMetadata);
        RollupAction.Request rollupRequest = new RollupAction.Request(
            request.getSourceIndex(),
            request.getDownsampleIndex(),
            rollupActionConfig
        );
        client.execute(RollupAction.INSTANCE, rollupRequest, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private RollupActionConfig buildRollupActionConfig(Request request, IndexMetadata indexMetadata) {
        Map<String, Object> mapping = indexMetadata.mapping().sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        if (properties == null) {
            throw new IllegalArgumentException("no dimension and metrics fields exception");
        }

        Map<String, Object> fields = flattenFields(properties);
        List<String> dimensions = new ArrayList<>();
        List<MetricConfig> metricsConfig = new ArrayList<>();
        fields.forEach((key, value) -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> fieldProperty = (Map<String, Object>) value;
            if (fieldProperty.containsKey(TIME_SERIES_DIMENSION_PARAM)
                && String.valueOf(fieldProperty.get(TIME_SERIES_DIMENSION_PARAM)).equals("true")) {
                dimensions.add(key);
            } else if (fieldProperty.containsKey(TIME_SERIES_METRIC_PARAM)) {
                @SuppressWarnings("unchecked")
                MetricType metricType = MetricType.valueOf((String) fieldProperty.get(TIME_SERIES_DIMENSION_PARAM));
                // TODO make sure the right metrics list settings
                switch (metricType) {
                    case counter -> metricsConfig.add(new MetricConfig(key, List.of("max")));
                    case gauge -> metricsConfig.add(new MetricConfig(key, List.of("max", "min", "sum", "value_count")));
                    case summary -> metricsConfig.add(new MetricConfig(key, List.of("max", "min", "sum", "value_count")));
                    case histogram -> {
                        // TODO to support histogram downsample
                        throw new IllegalArgumentException("downsample does not support histogram type");
                    }
                }
            }
        });

        DownsampleDateHistogramConfig downsampleConfig = request.getDownsampleConfig();
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig;
        if (request.getDownsampleConfig().getCalendarInterval() != null) {
            dateHistogramGroupConfig = new CalendarInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                downsampleConfig.getFixedInterval(),
                downsampleConfig.getTimeZone()
            );
        } else {
            dateHistogramGroupConfig = new FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                downsampleConfig.getFixedInterval(),
                downsampleConfig.getTimeZone()
            );
        }

        return new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig(dimensions.toArray(Strings.EMPTY_ARRAY))),
            metricsConfig
        );
    }

    static Map<String, Object> flattenFields(Map<String, Object> properties) {
        return flattenFields(properties, null);

    }

    private static Map<String, Object> flattenFields(Map<String, Object> map, String parentPath) {
        Map<String, Object> flatMap = new HashMap<>();
        String prefix = parentPath != null ? parentPath + "." : "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                String type = (String) value.get("type");
                if (type == null || type.equals("object")) {
                    if (value.containsKey("properties")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> properties = (Map<String, Object>) value.get("properties");
                        flatMap.putAll(flattenFields(properties, prefix + entry.getKey()));
                    }
                } else {
                    flatMap.put(prefix + entry.getKey(), entry.getValue());
                }
            }
        }
        return flatMap;
    }
}
