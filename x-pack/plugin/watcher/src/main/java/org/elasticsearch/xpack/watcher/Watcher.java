/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.core.watcher.actions.ActionRegistry;
import org.elasticsearch.xpack.core.watcher.condition.ConditionFactory;
import org.elasticsearch.xpack.core.watcher.condition.ConditionRegistry;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.core.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
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
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpSettings;
import org.elasticsearch.xpack.watcher.common.http.auth.HttpAuthFactory;
import org.elasticsearch.xpack.watcher.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.watcher.common.http.auth.basic.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.condition.ArrayCompareCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.execution.AsyncTriggerEventConsumer;
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
import org.elasticsearch.xpack.watcher.input.none.NoneInputFactory;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInputFactory;
import org.elasticsearch.xpack.watcher.input.transform.TransformInput;
import org.elasticsearch.xpack.watcher.input.transform.TransformInputFactory;
import org.elasticsearch.xpack.watcher.notification.email.Account;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.watcher.notification.email.attachment.DataAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentsParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.HttpEmailAttachementParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.ReportingAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.support.BodyPartSource;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;
import org.elasticsearch.xpack.watcher.notification.pagerduty.PagerDutyService;
import org.elasticsearch.xpack.watcher.notification.slack.SlackService;
import org.elasticsearch.xpack.watcher.rest.action.RestAckWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestActivateWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestDeleteWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestExecuteWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestGetWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestPutWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestWatchServiceAction;
import org.elasticsearch.xpack.watcher.rest.action.RestWatcherStatsAction;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransformFactory;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;
import org.elasticsearch.xpack.watcher.transport.actions.ack.TransportAckWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.activate.TransportActivateWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.delete.TransportDeleteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.execute.TransportExecuteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.get.TransportGetWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.put.TransportPutWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.service.TransportWatcherServiceAction;
import org.elasticsearch.xpack.watcher.transport.actions.stats.TransportWatcherStatsAction;
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
import org.elasticsearch.xpack.watcher.trigger.schedule.engine.TickerScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;

public class Watcher extends Plugin implements ActionPlugin, ScriptPlugin {

    // This setting is only here for backward compatibility reasons as 6.x indices made use of it. It can be removed in 8.x.
    @Deprecated
    public static final Setting<String> INDEX_WATCHER_TEMPLATE_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.template.version", "", Function.identity(), Setting.Property.IndexScope);
    public static final Setting<Boolean> ENCRYPT_SENSITIVE_DATA_SETTING =
            Setting.boolSetting("xpack.watcher.encrypt_sensitive_data", false, Setting.Property.NodeScope);
    public static final Setting<TimeValue> MAX_STOP_TIMEOUT_SETTING =
            Setting.timeSetting("xpack.watcher.stop.timeout", TimeValue.timeValueSeconds(30), Setting.Property.NodeScope);

    public static final ScriptContext<SearchScript.Factory> SCRIPT_SEARCH_CONTEXT =
        new ScriptContext<>("xpack", SearchScript.Factory.class);
    // TODO: remove this context when each xpack script use case has their own contexts
    public static final ScriptContext<ExecutableScript.Factory> SCRIPT_EXECUTABLE_CONTEXT
        = new ScriptContext<>("xpack_executable", ExecutableScript.Factory.class);
    public static final ScriptContext<TemplateScript.Factory> SCRIPT_TEMPLATE_CONTEXT
        = new ScriptContext<>("xpack_template", TemplateScript.Factory.class);

    private static final Logger logger = Loggers.getLogger(Watcher.class);
    private WatcherIndexingListener listener;
    private HttpClient httpClient;

    protected final Settings settings;
    protected final boolean transportClient;
    protected final boolean enabled;
    protected final Environment env;

    public Watcher(final Settings settings) {
        this.settings = settings;
        this.transportClient = XPackPlugin.transportClientMode(settings);
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);
        env = transportClient ? null : new Environment(settings, null);

        if (enabled && transportClient == false) {
            validAutoCreateIndex(settings, logger);
        }
    }

    // overridable by tests
    protected SSLService getSslService() { return XPackPlugin.getSharedSslService(); }
    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }
    protected Clock getClock() { return Clock.systemUTC(); }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        if (enabled == false) {
            return Collections.emptyList();
        }

        // only initialize these classes if Watcher is enabled, and only after the plugin security policy for Watcher is in place
        BodyPartSource.init();
        Account.init();

        final CryptoService cryptoService;
        try {
            cryptoService = ENCRYPT_SENSITIVE_DATA_SETTING.get(settings) ? new CryptoService(settings) : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        new WatcherIndexTemplateRegistry(settings, clusterService, threadPool, client);

        // http client
        Map<String, HttpAuthFactory> httpAuthFactories = new HashMap<>();
        httpAuthFactories.put(BasicAuth.TYPE, new BasicAuthFactory(cryptoService));
        // TODO: add more auth types, or remove this indirection
        HttpAuthRegistry httpAuthRegistry = new HttpAuthRegistry(httpAuthFactories);
        HttpRequestTemplate.Parser httpTemplateParser = new HttpRequestTemplate.Parser(httpAuthRegistry);
        httpClient = new HttpClient(settings, httpAuthRegistry, getSslService());

        // notification
        EmailService emailService = new EmailService(settings, cryptoService, clusterService.getClusterSettings());
        HipChatService hipChatService = new HipChatService(settings, httpClient, clusterService.getClusterSettings());
        JiraService jiraService = new JiraService(settings, httpClient, clusterService.getClusterSettings());
        SlackService slackService = new SlackService(settings, httpClient, clusterService.getClusterSettings());
        PagerDutyService pagerDutyService = new PagerDutyService(settings, httpClient, clusterService.getClusterSettings());

        TextTemplateEngine templateEngine = new TextTemplateEngine(settings, scriptService);
        Map<String, EmailAttachmentParser> emailAttachmentParsers = new HashMap<>();
        emailAttachmentParsers.put(HttpEmailAttachementParser.TYPE, new HttpEmailAttachementParser(httpClient, httpTemplateParser,
                templateEngine));
        emailAttachmentParsers.put(DataAttachmentParser.TYPE, new DataAttachmentParser());
        emailAttachmentParsers.put(ReportingAttachmentParser.TYPE, new ReportingAttachmentParser(settings, httpClient, templateEngine,
                httpAuthRegistry));
        EmailAttachmentsParser emailAttachmentsParser = new EmailAttachmentsParser(emailAttachmentParsers);

        // conditions
        final Map<String, ConditionFactory> parsers = new HashMap<>();
        parsers.put(InternalAlwaysCondition.TYPE, (c, id, p) -> InternalAlwaysCondition.parse(id, p));
        parsers.put(NeverCondition.TYPE, (c, id, p) -> NeverCondition.parse(id, p));
        parsers.put(ArrayCompareCondition.TYPE, ArrayCompareCondition::parse);
        parsers.put(CompareCondition.TYPE, CompareCondition::parse);
        parsers.put(ScriptCondition.TYPE, (c, id, p) -> ScriptCondition.parse(scriptService, id, p));

        final ConditionRegistry conditionRegistry = new ConditionRegistry(Collections.unmodifiableMap(parsers), getClock());
        final Map<String, TransformFactory> transformFactories = new HashMap<>();
        transformFactories.put(ScriptTransform.TYPE, new ScriptTransformFactory(settings, scriptService));
        transformFactories.put(SearchTransform.TYPE, new SearchTransformFactory(settings, client, xContentRegistry, scriptService));
        final TransformRegistry transformRegistry = new TransformRegistry(settings, Collections.unmodifiableMap(transformFactories));

        // actions
        final Map<String, ActionFactory> actionFactoryMap = new HashMap<>();
        actionFactoryMap.put(EmailAction.TYPE, new EmailActionFactory(settings, emailService, templateEngine, emailAttachmentsParser));
        actionFactoryMap.put(WebhookAction.TYPE, new WebhookActionFactory(settings, httpClient, httpTemplateParser, templateEngine));
        actionFactoryMap.put(IndexAction.TYPE, new IndexActionFactory(settings, client));
        actionFactoryMap.put(LoggingAction.TYPE, new LoggingActionFactory(settings, templateEngine));
        actionFactoryMap.put(HipChatAction.TYPE, new HipChatActionFactory(settings, templateEngine, hipChatService));
        actionFactoryMap.put(JiraAction.TYPE, new JiraActionFactory(settings, templateEngine, jiraService));
        actionFactoryMap.put(SlackAction.TYPE, new SlackActionFactory(settings, templateEngine, slackService));
        actionFactoryMap.put(PagerDutyAction.TYPE, new PagerDutyActionFactory(settings, templateEngine, pagerDutyService));
        final ActionRegistry registry = new ActionRegistry(actionFactoryMap, conditionRegistry, transformRegistry, getClock(),
            getLicenseState());

        // inputs
        final Map<String, InputFactory> inputFactories = new HashMap<>();
        inputFactories.put(SearchInput.TYPE, new SearchInputFactory(settings, client, xContentRegistry, scriptService));
        inputFactories.put(SimpleInput.TYPE, new SimpleInputFactory(settings));
        inputFactories.put(HttpInput.TYPE, new HttpInputFactory(settings, httpClient, templateEngine, httpTemplateParser));
        inputFactories.put(NoneInput.TYPE, new NoneInputFactory(settings));
        inputFactories.put(TransformInput.TYPE, new TransformInputFactory(settings, transformRegistry));
        final InputRegistry inputRegistry = new InputRegistry(settings, inputFactories);
        inputFactories.put(ChainInput.TYPE, new ChainInputFactory(settings, inputRegistry));

        final HistoryStore historyStore = new HistoryStore(settings, client);

        // schedulers
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
        final TriggerEngine configuredTriggerEngine = getTriggerEngine(getClock(), scheduleRegistry);

        final Set<TriggerEngine> triggerEngines = new HashSet<>();
        triggerEngines.add(manualTriggerEngine);
        triggerEngines.add(configuredTriggerEngine);
        final TriggerService triggerService = new TriggerService(settings, triggerEngines);

        final TriggeredWatch.Parser triggeredWatchParser = new TriggeredWatch.Parser(settings, triggerService);
        final TriggeredWatchStore triggeredWatchStore = new TriggeredWatchStore(settings, client, triggeredWatchParser);

        final WatcherSearchTemplateService watcherSearchTemplateService =
                new WatcherSearchTemplateService(settings, scriptService, xContentRegistry);
        final WatchExecutor watchExecutor = getWatchExecutor(threadPool);
        final WatchParser watchParser = new WatchParser(settings, triggerService, registry, inputRegistry, cryptoService, getClock());

        final ExecutionService executionService = new ExecutionService(settings, historyStore, triggeredWatchStore, watchExecutor,
                getClock(), watchParser, clusterService, client, threadPool.generic());

        final Consumer<Iterable<TriggerEvent>> triggerEngineListener = getTriggerEngineListener(executionService);
        triggerService.register(triggerEngineListener);

        WatcherService watcherService = new WatcherService(settings, triggerService, triggeredWatchStore, executionService,
                watchParser, client);

        final WatcherLifeCycleService watcherLifeCycleService =
                new WatcherLifeCycleService(settings, clusterService, watcherService);

        listener = new WatcherIndexingListener(settings, watchParser, getClock(), triggerService);
        clusterService.addListener(listener);

        return Arrays.asList(registry, inputRegistry, historyStore, triggerService, triggeredWatchParser,
                watcherLifeCycleService, executionService, triggerEngineListener, watcherService, watchParser,
                configuredTriggerEngine, triggeredWatchStore, watcherSearchTemplateService, slackService, pagerDutyService, hipChatService);
    }

    protected TriggerEngine getTriggerEngine(Clock clock, ScheduleRegistry scheduleRegistry) {
        return new TickerScheduleTriggerEngine(settings, scheduleRegistry, clock);
    }

    protected WatchExecutor getWatchExecutor(ThreadPool threadPool) {
        return new InternalWatchExecutor(threadPool);
    }

    protected Consumer<Iterable<TriggerEvent>> getTriggerEngineListener(ExecutionService executionService) {
        return new AsyncTriggerEventConsumer(settings, executionService);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();
        modules.add(b -> b.bind(Clock.class).toInstance(getClock())); //currently assuming the only place clock is bound
        modules.add(b -> {
            XPackPlugin.bindFeatureSet(b, WatcherFeatureSet.class);
            if (transportClient || enabled == false) {
                b.bind(WatcherService.class).toProvider(Providers.of(null));
            }
        });

        return modules;
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(INDEX_WATCHER_TEMPLATE_VERSION_SETTING);
        settings.add(MAX_STOP_TIMEOUT_SETTING);
        settings.add(ExecutionService.DEFAULT_THROTTLE_PERIOD_SETTING);
        settings.add(TickerScheduleTriggerEngine.TICKER_INTERVAL_SETTING);
        settings.add(Setting.intSetting("xpack.watcher.execution.scroll.size", 0, Setting.Property.NodeScope));
        settings.add(Setting.intSetting("xpack.watcher.watch.scroll.size", 0, Setting.Property.NodeScope));
        settings.add(ENCRYPT_SENSITIVE_DATA_SETTING);
        settings.add(WatcherField.ENCRYPTION_KEY_SETTING);

        settings.add(Setting.simpleString("xpack.watcher.internal.ops.search.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.internal.ops.bulk.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.internal.ops.index.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.actions.index.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.actions.bulk.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.index.rest.direct_access", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.input.search.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.transform.search.default_timeout", Setting.Property.NodeScope));
        settings.add(Setting.simpleString("xpack.watcher.execution.scroll.timeout", Setting.Property.NodeScope));
        settings.add(WatcherLifeCycleService.SETTING_REQUIRE_MANUAL_START);

        // notification services
        settings.addAll(SlackService.getSettings());
        settings.addAll(EmailService.getSettings());
        settings.addAll(HtmlSanitizer.getSettings());
        settings.addAll(HipChatService.getSettings());
        settings.addAll(JiraService.getSettings());
        settings.addAll(PagerDutyService.getSettings());
        settings.add(ReportingAttachmentParser.RETRIES_SETTING);
        settings.add(ReportingAttachmentParser.INTERVAL_SETTING);

        // http settings
        settings.addAll(HttpSettings.getSettings());

        // encryption settings
        CryptoService.addSettings(settings);
        return settings;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled) {
            final FixedExecutorBuilder builder =
                    new FixedExecutorBuilder(
                            settings,
                            InternalWatchExecutor.THREAD_POOL_NAME,
                            getWatcherThreadPoolSize(settings),
                            1000,
                            "xpack.watcher.thread_pool");
            return Collections.singletonList(builder);
        }
        return Collections.emptyList();
    }

    /**
     * A method to indicate the size of the watcher thread pool
     * As watches are primarily bound on I/O waiting and execute
     * synchronously, it makes sense to have a certain minimum of a
     * threadpool size. This means you should start with a fair number
     * of threads which is more than the number of CPUs, but you also need
     * to ensure that this number does not go crazy high if you have really
     * beefy machines. This can still be configured manually.
     *
     * Calculation is as follows:
     * Use five times the number of processors up until 50, then stick with the
     * number of processors.
     *
     * If the node is not a data node, we will never need so much threads, so we
     * just return 1 here, which still allows to execute a watch locally, but
     * there is no need of managing any more threads here
     *
     * @param settings The current settings
     * @return A number between 5 and the number of processors
     */
    static int getWatcherThreadPoolSize(Settings settings) {
        boolean isDataNode = Node.NODE_DATA_SETTING.get(settings);
        if (isDataNode) {
            int numberOfProcessors = EsExecutors.numberOfProcessors(settings);
            long size = Math.max(Math.min(5 * numberOfProcessors, 50), numberOfProcessors);
            return Math.toIntExact(size);
        } else {
            return 1;
        }
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
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (false == enabled) {
            return emptyList();
        }
        return Arrays.asList(
                new RestPutWatchAction(settings, restController),
                new RestDeleteWatchAction(settings, restController),
                new RestWatcherStatsAction(settings, restController),
                new RestGetWatchAction(settings, restController),
                new RestWatchServiceAction(settings, restController),
                new RestAckWatchAction(settings, restController),
                new RestActivateWatchAction(settings, restController),
                new RestExecuteWatchAction(settings, restController));
    }

    @Override
    public void onIndexModule(IndexModule module) {
        if (enabled == false || transportClient) {
            return;
        }

        assert listener != null;
        // for now, we only add this index operation listener to indices starting with .watches
        // this also means, that aliases pointing to this index have to follow this notation
        if (module.getIndex().getName().startsWith(Watch.INDEX)) {
            module.addIndexOperationListener(listener);
        }
    }

    static void validAutoCreateIndex(Settings settings, Logger logger) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too" +
                " restrictive. disable [action.auto_create_index] or set it to " +
                "[{}, {}, {}*]", (Object) value, Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME, HistoryStoreField.INDEX_PREFIX);
        if (Booleans.isFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }

        if (Booleans.isTrue(value)) {
            return;
        }

        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>();
        indices.add(".watches");
        indices.add(".triggered_watches");
        DateTime now = new DateTime(DateTimeZone.UTC);
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusDays(1)));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusMonths(1)));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusMonths(2)));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusMonths(3)));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusMonths(4)));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusMonths(5)));
        indices.add(HistoryStoreField.getHistoryIndexNameForTime(now.plusMonths(6)));
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

    // These are all old templates from pre 6.0 era, that need to be deleted
    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return map -> {
            map.keySet().removeIf(name -> name.startsWith("watch_history_"));
            return map;
        };
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        return Collections.singletonList(new EncryptSensitiveDataBootstrapCheck(env));
    }

    @Override
    public List<ScriptContext> getContexts() {
        return Arrays.asList(Watcher.SCRIPT_SEARCH_CONTEXT, Watcher.SCRIPT_EXECUTABLE_CONTEXT, Watcher.SCRIPT_TEMPLATE_CONTEXT);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(httpClient);
    }
}
