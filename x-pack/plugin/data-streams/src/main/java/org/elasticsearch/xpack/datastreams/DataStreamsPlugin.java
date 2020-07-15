/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.datastreams.mapper.DataStreamTimestampFieldMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataStreamsPlugin extends Plugin implements ActionPlugin, MapperPlugin {

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Map.of(DataStreamTimestampFieldMapper.NAME, new DataStreamTimestampFieldMapper.TypeParser());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var dsUsageAction = new ActionHandler<>(XPackUsageFeatureAction.DATA_STREAMS, DataStreamUsageTransportAction.class);
        var dsInfoAction = new ActionHandler<>(XPackInfoFeatureAction.DATA_STREAMS, DataStreamInfoTransportAction.class);
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(dsUsageAction);
        actions.add(dsInfoAction);
        return actions;
    }
}
