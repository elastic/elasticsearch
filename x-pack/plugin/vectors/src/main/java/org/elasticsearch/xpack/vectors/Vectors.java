/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors;

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
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.SparseVectorFieldMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class Vectors extends Plugin implements MapperPlugin, ActionPlugin {

    protected final boolean enabled;

    public Vectors(Settings settings) {
        this.enabled = XPackSettings.VECTORS_ENABLED.get(settings);
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.VECTORS, VectorsUsageTransportAction.class),
            new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.VECTORS, VectorsInfoTransportAction.class));
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        if (enabled == false) {
            return emptyMap();
        }
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(DenseVectorFieldMapper.CONTENT_TYPE, new DenseVectorFieldMapper.TypeParser());
        mappers.put(SparseVectorFieldMapper.CONTENT_TYPE, new SparseVectorFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }
}
