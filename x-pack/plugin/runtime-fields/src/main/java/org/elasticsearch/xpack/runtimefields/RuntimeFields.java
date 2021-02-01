/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DynamicRuntimeFieldsBuilder;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
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
import org.elasticsearch.xpack.runtimefields.mapper.NamedGroupExtractor;
import org.elasticsearch.xpack.runtimefields.mapper.NamedGroupExtractor.GrokHelper;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class RuntimeFields extends Plugin implements MapperPlugin, ScriptPlugin, ActionPlugin {

    static final Setting<TimeValue> GROK_WATCHDOG_INTERVAL = Setting.timeSetting(
        "runtime_fields.grok.watchdog.interval",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    static final Setting<TimeValue> GROK_WATCHDOG_MAX_EXECUTION_TIME = Setting.timeSetting(
        "runtime_fields.grok.watchdog.max_execution_time",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private final NamedGroupExtractor.GrokHelper grokHelper;

    public RuntimeFields(Settings settings) {
        grokHelper = new NamedGroupExtractor.GrokHelper(
            GROK_WATCHDOG_INTERVAL.get(settings),
            GROK_WATCHDOG_MAX_EXECUTION_TIME.get(settings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(GROK_WATCHDOG_INTERVAL, GROK_WATCHDOG_MAX_EXECUTION_TIME);
    }

    @Override
    public Map<String, RuntimeFieldType.Parser> getRuntimeFieldTypes() {
        return Map.ofEntries(
            Map.entry(BooleanFieldMapper.CONTENT_TYPE, BooleanScriptFieldType.PARSER),
            Map.entry(NumberFieldMapper.NumberType.LONG.typeName(), LongScriptFieldType.PARSER),
            Map.entry(NumberFieldMapper.NumberType.DOUBLE.typeName(), DoubleScriptFieldType.PARSER),
            Map.entry(IpFieldMapper.CONTENT_TYPE, IpScriptFieldType.PARSER),
            Map.entry(DateFieldMapper.CONTENT_TYPE, DateScriptFieldType.PARSER),
            Map.entry(KeywordFieldMapper.CONTENT_TYPE, KeywordScriptFieldType.PARSER),
            Map.entry(GeoPointFieldMapper.CONTENT_TYPE, GeoPointScriptFieldType.PARSER)
        );
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return List.of(
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

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.RUNTIME_FIELDS, RuntimeFieldsUsageTransportAction.class),
            new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.RUNTIME_FIELDS, RuntimeFieldsInfoTransportAction.class)
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        grokHelper.finishInitializing(threadPool);
        return List.of();
    }

    public GrokHelper grokHelper() {
        return grokHelper;
    }
}
