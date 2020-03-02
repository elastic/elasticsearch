/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class ConstantKeywordMapperPlugin extends Plugin implements MapperPlugin, ActionPlugin {

    public ConstantKeywordMapperPlugin(Settings settings) {}

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return singletonMap(ConstantKeywordFieldMapper.CONTENT_TYPE, new ConstantKeywordFieldMapper.TypeParser());
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(b -> {
            XPackPlugin.bindFeatureSet(b, ConstantKeywordFeatureSet.class);
        });
    }

}
