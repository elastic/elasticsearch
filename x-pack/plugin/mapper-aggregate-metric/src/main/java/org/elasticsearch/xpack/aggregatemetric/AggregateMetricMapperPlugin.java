/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.aggregatemetric;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricFieldMapper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class AggregateMetricMapperPlugin extends Plugin implements MapperPlugin, ActionPlugin {

    protected final boolean enabled;

    public AggregateMetricMapperPlugin(Settings settings) {
        this.enabled = XPackSettings.AGGREGATE_METRIC_ENABLED.get(settings);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        if (enabled == false) {
            return emptyMap();
        }
        return singletonMap(AggregateMetricFieldMapper.CONTENT_TYPE, new AggregateMetricFieldMapper.TypeParser());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.AGGREGATE_METRIC, AggregateMetricUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.AGGREGATE_METRIC, AggregateMetricInfoTransportAction.class));
    }

}
