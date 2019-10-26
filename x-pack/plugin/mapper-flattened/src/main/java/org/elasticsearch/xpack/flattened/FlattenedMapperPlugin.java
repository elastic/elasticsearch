/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class FlattenedMapperPlugin extends Plugin implements MapperPlugin, ActionPlugin {

    protected final boolean enabled;

    public FlattenedMapperPlugin(Settings settings) {
        this.enabled = XPackSettings.FLATTENED_ENABLED.get(settings);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        if (enabled == false) {
            return emptyMap();
        }
        return singletonMap(FlatObjectFieldMapper.CONTENT_TYPE, new FlatObjectFieldMapper.TypeParser());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.FLATTENED, FlattenedUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.FLATTENED, FlattenedInfoTransportAction.class));
    }
}
