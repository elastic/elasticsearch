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
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachmentsParser;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.notification.jira.JiraService;
import org.elasticsearch.xpack.notification.pagerduty.PagerDutyService;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.actions.ActionRegistry;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.email.EmailActionFactory;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatActionFactory;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexActionFactory;
import org.elasticsearch.xpack.watcher.actions.jira.JiraAction;
import org.elasticsearch.xpack.watcher.actions.jira.JiraActionFactory;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingActionFactory;
import org.elasticsearch.xpack.watcher.actions.pagerduty.PagerDutyAction;
import org.elasticsearch.xpack.watcher.actions.pagerduty.PagerDutyActionFactory;
import org.elasticsearch.xpack.watcher.actions.slack.SlackAction;
import org.elasticsearch.xpack.watcher.actions.slack.SlackActionFactory;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookActionFactory;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.ArrayCompareCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.ConditionFactory;
import org.elasticsearch.xpack.watcher.condition.ConditionRegistry;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.execution.AsyncTriggerListener;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.InternalWatchExecutor;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatch;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.execution.WatchExecutor;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.input.InputFactory;
import org.elasticsearch.xpack.watcher.input.InputRegistry;
import org.elasticsearch.xpack.watcher.input.chain.ChainInput;
import org.elasticsearch.xpack.watcher.input.chain.ChainInputFactory;
import org.elasticsearch.xpack.watcher.input.http.HttpInput;
import org.elasticsearch.xpack.watcher.input.http.HttpInputFactory;
import org.elasticsearch.xpack.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.watcher.input.none.NoneInputFactory;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInputFactory;
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
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransformFactory;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;
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
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.CronSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.DailySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.HourlySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.MonthlySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.WeeklySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.YearlySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.engine.SchedulerScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.engine.TickerScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.watcher.support.Exceptions.illegalState;

public class Watcher implements ActionPlugin, ScriptPlugin {

    public static final Setting<String> INDEX_WATCHER_TEMPLATE_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.template.version", "", Function.identity(), Setting.Property.IndexScope);
    public static final Setting<Boolean> ENCRYPT_SENSITIVE_DATA_SETTING =
            Setting.boolSetting("xpack.watcher.encrypt_sensitive_data", false, Setting.Property.NodeScope);
    public static final Setting<TimeValue> MAX_STOP_TIMEOUT_SETTING =
            Setting.timeSetting("xpack.watcher.stop.timeout", TimeValue.timeValueSeconds(30), Setting.Property.NodeScope);
    public static final Setting<String> TRIGGER_SCHEDULE_ENGINE_SETTING =
            new Setting<>("xpack.watcher.trigger.schedule.engine", "ticker", s -> {
                switch (s) {
                    case "ticker":
                    case "scheduler":
                        return s;
                    default:
                        throw new IllegalArgumentException("Can't parse [xpack.watcher.trigger.schedule.engine] must be one of [ticker, " +
                                "scheduler], was [" + s + "]");
                }

            }, Setting.Property.NodeScope);


    private static final ScriptContext.Plugin SCRIPT_PLUGIN = new ScriptContext.Plugin("xpack", "watch");
    public static final ScriptContext SCRIPT_CONTEXT = SCRIPT_PLUGIN::getKey;

    private static final Logger logger = Loggers.getLogger(XPackPlugin.class);

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, WatcherMetaData.TYPE, WatcherMetaData::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, WatcherMetaData.TYPE, WatcherMetaData::readDiffFrom));
        return entries;
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(WatcherMetaData.TYPE),
                WatcherMetaData::fromXContent));
        return entries;
    }

    protected final Settings settings;
    protected final boolean transportClient;
    protected final boolean enabled;
    private final boolean transportClientMode;

    public Watcher(Settings settings) {
        this.settings = settings;
        transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        validAutoCreateIndex(settings);
    }

    public Collection<Object> createComponents(Clock clock, ScriptService scriptService, InternalClient internalClient,
                                               XPackLicenseState licenseState,
                                               HttpClient httpClient, HttpRequestTemplate.Parser httpTemplateParser,
                                               ThreadPool threadPool, ClusterService clusterService, CryptoService cryptoService,
                                               NamedXContentRegistry xContentRegistry, Collection<Object> components) {
        if (enabled == false) {
            return Collections.emptyList();
        }

        final Map<String, ConditionFactory> parsers = new HashMap<>();
        parsers.put(AlwaysCondition.TYPE, (c, id, p) -> AlwaysCondition.parse(id, p));
        parsers.put(NeverCondition.TYPE, (c, id, p) -> NeverCondition.parse(id, p));
        parsers.put(ArrayCompareCondition.TYPE, (c, id, p) -> ArrayCompareCondition.parse(c, id, p));
        parsers.put(CompareCondition.TYPE, (c, id, p) -> CompareCondition.parse(c, id, p));
        parsers.put(ScriptCondition.TYPE, (c, id, p) -> ScriptCondition.parse(scriptService, id, p));

        final ConditionRegistry conditionRegistry = new ConditionRegistry(Collections.unmodifiableMap(parsers), clock);
        final Map<String, TransformFactory> transformFactories = new HashMap<>();
        transformFactories.put(ScriptTransform.TYPE, new ScriptTransformFactory(settings, scriptService));
        transformFactories.put(SearchTransform.TYPE, new SearchTransformFactory(settings, internalClient, xContentRegistry, scriptService));
        final TransformRegistry transformRegistry = new TransformRegistry(settings, Collections.unmodifiableMap(transformFactories));

        final Map<String, ActionFactory> actionFactoryMap = new HashMap<>();
        TextTemplateEngine templateEngine = getService(TextTemplateEngine.class, components);
        actionFactoryMap.put(EmailAction.TYPE, new EmailActionFactory(settings, getService(EmailService.class, components), templateEngine,
                getService(EmailAttachmentsParser.class, components)));
        actionFactoryMap.put(WebhookAction.TYPE, new WebhookActionFactory(settings, httpClient,
                getService(HttpRequestTemplate.Parser.class, components), templateEngine));
        actionFactoryMap.put(IndexAction.TYPE, new IndexActionFactory(settings, internalClient));
        actionFactoryMap.put(LoggingAction.TYPE, new LoggingActionFactory(settings, templateEngine));
        actionFactoryMap.put(HipChatAction.TYPE, new HipChatActionFactory(settings, templateEngine,
                getService(HipChatService.class, components)));
        actionFactoryMap.put(JiraAction.TYPE, new JiraActionFactory(settings, templateEngine,
                getService(JiraService.class, components)));
        actionFactoryMap.put(SlackAction.TYPE, new SlackActionFactory(settings, templateEngine,
                getService(SlackService.class, components)));
        actionFactoryMap.put(PagerDutyAction.TYPE, new PagerDutyActionFactory(settings, templateEngine,
                getService(PagerDutyService.class, components)));
        final ActionRegistry registry = new ActionRegistry(actionFactoryMap, conditionRegistry, transformRegistry, clock, licenseState);

        final Map<String, InputFactory> inputFactories = new HashMap<>();
        inputFactories.put(SearchInput.TYPE,
                new SearchInputFactory(settings, internalClient, xContentRegistry, scriptService));
        inputFactories.put(SimpleInput.TYPE, new SimpleInputFactory(settings));
        inputFactories.put(HttpInput.TYPE, new HttpInputFactory(settings, httpClient, templateEngine, httpTemplateParser));
        inputFactories.put(NoneInput.TYPE, new NoneInputFactory(settings));
        final InputRegistry inputRegistry = new InputRegistry(settings, inputFactories);
        inputFactories.put(ChainInput.TYPE, new ChainInputFactory(settings, inputRegistry));

        final WatcherClientProxy watcherClientProxy = new WatcherClientProxy(settings, internalClient);
        final WatcherClient watcherClient = new WatcherClient(internalClient);

        final HistoryStore historyStore = new HistoryStore(settings, watcherClientProxy);
        final Set<Schedule.Parser> scheduleParsers = new HashSet<>();
        scheduleParsers.add(new CronSchedule.Parser());
        scheduleParsers.add(new DailySchedule.Parser());
        scheduleParsers.add(new HourlySchedule.Parser());
        scheduleParsers.add(new IntervalSchedule.Parser());
        scheduleParsers.add(new MonthlySchedule.Parser());
        scheduleParsers.add(new WeeklySchedule.Parser());
        scheduleParsers.add(new YearlySchedule.Parser());
        final ScheduleRegistry scheduleRegistry = new ScheduleRegistry(scheduleParsers);

        TriggerEngine manualTriggerEngine = new ManualTriggerEngine();
        TriggerEngine configuredTriggerEngine = getTriggerEngine(clock, scheduleRegistry);

        final Set<TriggerEngine> triggerEngines = new HashSet<>();
        triggerEngines.add(manualTriggerEngine);
        triggerEngines.add(configuredTriggerEngine);
        final TriggerService triggerService = new TriggerService(settings, triggerEngines);

        final TriggeredWatch.Parser triggeredWatchParser = new TriggeredWatch.Parser(settings, triggerService);
        final TriggeredWatchStore triggeredWatchStore = new TriggeredWatchStore(settings, watcherClientProxy, triggeredWatchParser);

        final WatcherSearchTemplateService watcherSearchTemplateService =
                new WatcherSearchTemplateService(settings, scriptService, xContentRegistry);
        final WatchExecutor watchExecutor = getWatchExecutor(threadPool);
        final Watch.Parser watchParser = new Watch.Parser(settings, triggerService, registry, inputRegistry, cryptoService, clock);

        final ExecutionService executionService = new ExecutionService(settings, historyStore, triggeredWatchStore, watchExecutor,
                clock, threadPool, watchParser, watcherClientProxy);

        final TriggerEngine.Listener triggerEngineListener = getTriggerEngineListener(executionService);
        triggerService.register(triggerEngineListener);

        final WatcherIndexTemplateRegistry watcherIndexTemplateRegistry = new WatcherIndexTemplateRegistry(settings,
                clusterService.getClusterSettings(), clusterService, threadPool, internalClient);

        final WatcherService watcherService = new WatcherService(settings, triggerService, executionService,
                watcherIndexTemplateRegistry, watchParser, watcherClientProxy);

        final WatcherLifeCycleService watcherLifeCycleService =
                new WatcherLifeCycleService(settings, threadPool, clusterService, watcherService);

        return Arrays.asList(registry, watcherClient, inputRegistry, historyStore, triggerService, triggeredWatchParser,
                watcherLifeCycleService, executionService, triggerEngineListener, watcherService, watchParser,
                configuredTriggerEngine, triggeredWatchStore, watcherSearchTemplateService, watcherClientProxy);
    }

    protected TriggerEngine getTriggerEngine(Clock clock, ScheduleRegistry scheduleRegistry) {
        String engine = TRIGGER_SCHEDULE_ENGINE_SETTING.get(settings);
        switch (engine) {
            case "scheduler":
                return new SchedulerScheduleTriggerEngine(settings, scheduleRegistry, clock);
            case "ticker":
                return new TickerScheduleTriggerEngine(settings, scheduleRegistry, clock);
            default: // should never happen, as the setting is already parsing for scheduler/ticker
                throw illegalState("schedule engine must be either set to [scheduler] or [ticker], but was []", engine);
        }
    }

    protected WatchExecutor getWatchExecutor(ThreadPool threadPool) {
        return new InternalWatchExecutor(threadPool);
    }

    protected TriggerEngine.Listener getTriggerEngineListener(ExecutionService executionService) {
        return new AsyncTriggerListener(settings, executionService);
    }

    private <T> T getService(Class<T> serviceClass, Collection<Object> services) {
        List<Object> collect = services.stream().filter(o -> o.getClass() == serviceClass).collect(Collectors.toList());
        if (collect.isEmpty()) {
            throw new IllegalArgumentException("no service for class " + serviceClass.getName());
        } else if (collect.size() > 1) {
            throw new IllegalArgumentException("more than one service for class " + serviceClass.getName());
        }
        return (T) collect.get(0);
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, WatcherFeatureSet.class);
            if (transportClientMode || enabled == false) {
                b.bind(WatcherService.class).toProvider(Providers.of(null));
            }
        });

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
        settings.add(TRIGGER_SCHEDULE_ENGINE_SETTING);
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
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
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
                "[{}, {}, {}*]", (Object) value, Watch.INDEX, TriggeredWatchStore.INDEX_NAME, HistoryStore.INDEX_PREFIX);
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
