/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.watcher.actions.WatcherActionModule;
import org.elasticsearch.xpack.watcher.client.WatcherClientModule;
import org.elasticsearch.xpack.watcher.condition.ConditionModule;
import org.elasticsearch.xpack.watcher.execution.ExecutionModule;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.InternalWatchExecutor;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.history.HistoryModule;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.input.InputModule;
import org.elasticsearch.xpack.watcher.rest.action.RestAckWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestActivateWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestDeleteWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestExecuteWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestGetWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestHijackOperationAction;
import org.elasticsearch.xpack.watcher.rest.action.RestPutWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestWatchServiceAction;
import org.elasticsearch.xpack.watcher.rest.action.RestWatcherStatsAction;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry.TemplateConfig;
import org.elasticsearch.xpack.watcher.transform.TransformModule;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.ack.TransportAckWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.activate.TransportActivateWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.delete.TransportDeleteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.execute.TransportExecuteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.get.TransportGetWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.put.TransportPutWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.service.TransportWatcherServiceAction;
import org.elasticsearch.xpack.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.watcher.transport.actions.stats.TransportWatcherStatsAction;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.watcher.trigger.TriggerModule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleModule;
import org.elasticsearch.xpack.watcher.watch.WatchModule;
import org.elasticsearch.xpack.watcher.watch.WatchStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class Watcher implements ActionPlugin, ScriptPlugin {

    public static final Setting<String> INDEX_WATCHER_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.plugin.version", "", Function.identity(), Setting.Property.IndexScope);
    public static final Setting<String> INDEX_WATCHER_TEMPLATE_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.template.version", "", Function.identity(), Setting.Property.IndexScope);
    public static final Setting<Boolean> ENCRYPT_SENSITIVE_DATA_SETTING =
            Setting.boolSetting("xpack.watcher.encrypt_sensitive_data", false, Setting.Property.NodeScope);
    public static final Setting<TimeValue> MAX_STOP_TIMEOUT_SETTING =
        Setting.timeSetting("xpack.watcher.stop.timeout", TimeValue.timeValueSeconds(30), Setting.Property.NodeScope);

    private static final ScriptContext.Plugin SCRIPT_PLUGIN = new ScriptContext.Plugin("xpack", "watch");
    public static final ScriptContext SCRIPT_CONTEXT = SCRIPT_PLUGIN::getKey;

    private static final Logger logger = Loggers.getLogger(XPackPlugin.class);

    static {
        MetaData.registerPrototype(WatcherMetaData.TYPE, WatcherMetaData.PROTO);
    }

    protected final Settings settings;
    protected final boolean transportClient;
    protected final boolean enabled;

    public Watcher(Settings settings) {
        this.settings = settings;
        transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);
        validAutoCreateIndex(settings);
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(new WatcherModule(enabled, transportClient));
        if (enabled && transportClient == false) {
            modules.add(new WatchModule());
            modules.add(new WatcherClientModule());
            modules.add(new TransformModule());
            modules.add(new TriggerModule(settings));
            modules.add(new ScheduleModule());
            modules.add(new ConditionModule());
            modules.add(new InputModule());
            modules.add(new WatcherActionModule());
            modules.add(new HistoryModule());
            modules.add(new ExecutionModule());
        }
        return modules;
    }

    public Settings additionalSettings() {
        return Settings.EMPTY;
    }


    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        for (TemplateConfig templateConfig : WatcherIndexTemplateRegistry.TEMPLATE_CONFIGS) {
            settings.add(templateConfig.getSetting());
        }
        settings.add(INDEX_WATCHER_VERSION_SETTING);
        settings.add(INDEX_WATCHER_TEMPLATE_VERSION_SETTING);
        settings.add(MAX_STOP_TIMEOUT_SETTING);
        settings.add(ExecutionService.DEFAULT_THROTTLE_PERIOD_SETTING);
        settings.add(Setting.intSetting("xpack.watcher.execution.scroll.size", 0, Setting.Property.NodeScope));
        settings.add(Setting.intSetting("xpack.watcher.watch.scroll.size", 0, Setting.Property.NodeScope));
        settings.add(ENCRYPT_SENSITIVE_DATA_SETTING);

        settings.add(Setting.simpleString("xpack.watcher.internal.ops.search.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.internal.ops.bulk.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.internal.ops.index.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.actions.index.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.index.rest.direct_access", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.trigger.schedule.engine", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.input.search.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.transform.search.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.trigger.schedule.ticker.tick_interval", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.execution.scroll.timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.start_immediately", Setting.Property.NodeScope));
        return settings;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled) {
            final FixedExecutorBuilder builder =
                    new FixedExecutorBuilder(
                            settings,
                            InternalWatchExecutor.THREAD_POOL_NAME,
                            5 * EsExecutors.numberOfProcessors(settings),
                            1000,
                            "xpack.watcher.thread_pool");
            return Collections.singletonList(builder);
        }
        return Collections.emptyList();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        if (false == enabled) {
            return emptyList();
        }
        return Arrays.asList(new ActionHandler<>(PutWatchAction.INSTANCE, TransportPutWatchAction.class),
                new ActionHandler<>(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class),
                new ActionHandler<>(GetWatchAction.INSTANCE, TransportGetWatchAction.class),
                new ActionHandler<>(WatcherStatsAction.INSTANCE, TransportWatcherStatsAction.class),
                new ActionHandler<>(AckWatchAction.INSTANCE, TransportAckWatchAction.class),
                new ActionHandler<>(ActivateWatchAction.INSTANCE, TransportActivateWatchAction.class),
                new ActionHandler<>(WatcherServiceAction.INSTANCE, TransportWatcherServiceAction.class),
                new ActionHandler<>(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (false == enabled) {
            return emptyList();
        }
        return Arrays.asList(RestPutWatchAction.class,
                RestDeleteWatchAction.class,
                RestWatcherStatsAction.class,
                RestGetWatchAction.class,
                RestWatchServiceAction.class,
                RestAckWatchAction.class,
                RestActivateWatchAction.class,
                RestExecuteWatchAction.class,
                RestHijackOperationAction.class);
    }

    @Override
    public ScriptContext.Plugin getCustomScriptContexts() {
        return SCRIPT_PLUGIN;
    }

    static void validAutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too" +
                " restrictive. disable [action.auto_create_index] or set it to " +
                "[{}, {}, {}*]", (Object) value, WatchStore.INDEX, TriggeredWatchStore.INDEX_NAME, HistoryStore.INDEX_PREFIX);
        if (Booleans.isExplicitFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (Booleans.isExplicitTrue(value)) {
            return;
        }

        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>();
        indices.add(".watches");
        indices.add(".triggered_watches");
        DateTime now = new DateTime(DateTimeZone.UTC);
        indices.add(HistoryStore.getHistoryIndexNameForTime(now));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusDays(1)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(1)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(2)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(3)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(4)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(5)));
        indices.add(HistoryStore.getHistoryIndexNameForTime(now.plusMonths(6)));
        for (String index : indices) {
            boolean matched = false;
            for (String match : matches) {
                char c = match.charAt(0);
                if (c == '-') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                } else if (c == '+') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        matched = true;
                        break;
                    }
                } else {
                    if (Regex.simpleMatch(match, index)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
        logger.warn("the [action.auto_create_index] setting is configured to be restrictive [{}]. " +
                " for the next 6 months daily history indices are allowed to be created, but please make sure" +
                " that any future history indices after 6 months with the pattern " +
                "[.watcher-history-YYYY.MM.dd] are allowed to be created", value);
    }


}
