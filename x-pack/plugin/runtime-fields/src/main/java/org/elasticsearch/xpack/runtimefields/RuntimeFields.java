/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DynamicRuntimeFieldsBuilder;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.runtimefields.mapper.BooleanFieldScript;
import org.elasticsearch.xpack.runtimefields.mapper.BooleanScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.DateFieldScript;
import org.elasticsearch.xpack.runtimefields.mapper.DateScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.DoubleFieldScript;
import org.elasticsearch.xpack.runtimefields.mapper.DoubleScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.GeoPointFieldScript;
import org.elasticsearch.xpack.runtimefields.mapper.GeoPointScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.IpFieldScript;
import org.elasticsearch.xpack.runtimefields.mapper.IpScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.KeywordScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.LongFieldScript;
import org.elasticsearch.xpack.runtimefields.mapper.LongScriptFieldType;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class RuntimeFields extends Plugin implements MapperPlugin, ScriptPlugin {

    @Override
    public Map<String, RuntimeFieldType.Parser> getRuntimeFieldTypes() {
        return org.elasticsearch.common.collect.Map.ofEntries(
            org.elasticsearch.common.collect.Map.entry(BooleanFieldMapper.CONTENT_TYPE, BooleanScriptFieldType.PARSER),
            org.elasticsearch.common.collect.Map.entry(NumberFieldMapper.NumberType.LONG.typeName(), LongScriptFieldType.PARSER),
            org.elasticsearch.common.collect.Map.entry(NumberFieldMapper.NumberType.DOUBLE.typeName(), DoubleScriptFieldType.PARSER),
            org.elasticsearch.common.collect.Map.entry(IpFieldMapper.CONTENT_TYPE, IpScriptFieldType.PARSER),
            org.elasticsearch.common.collect.Map.entry(DateFieldMapper.CONTENT_TYPE, DateScriptFieldType.PARSER),
            org.elasticsearch.common.collect.Map.entry(KeywordFieldMapper.CONTENT_TYPE, KeywordScriptFieldType.PARSER),
            org.elasticsearch.common.collect.Map.entry(GeoPointFieldMapper.CONTENT_TYPE, GeoPointScriptFieldType.PARSER)
        );
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return org.elasticsearch.common.collect.List.of(
            BooleanFieldScript.CONTEXT,
            DateFieldScript.CONTEXT,
            DoubleFieldScript.CONTEXT,
            GeoPointFieldScript.CONTEXT,
            IpFieldScript.CONTEXT,
            LongFieldScript.CONTEXT,
            StringFieldScript.CONTEXT
        );
    }

    @Override
    public DynamicRuntimeFieldsBuilder getDynamicRuntimeFieldsBuilder() {
        return org.elasticsearch.xpack.runtimefields.mapper.DynamicRuntimeFieldsBuilder.INSTANCE;
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(b -> XPackPlugin.bindFeatureSet(b, RuntimeFieldsFeatureSet.class));
    }
}
