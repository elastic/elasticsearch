/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.core.watcher.actions.ActionRegistry;
import org.elasticsearch.xpack.core.watcher.condition.ConditionRegistry;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
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
import org.elasticsearch.xpack.watcher.common.http.HttpSettings;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.condition.ArrayCompareCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.condition.WatcherConditionScript;
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
import org.elasticsearch.xpack.watcher.notification.NotificationService;
import org.elasticsearch.xpack.watcher.notification.email.Account;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.watcher.notification.email.attachment.DataAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentsParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.HttpEmailAttachementParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.ReportingAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.support.BodyPartSource;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;
import org.elasticsearch.xpack.watcher.notification.pagerduty.PagerDutyService;
import org.elasticsearch.xpack.watcher.notification.slack.SlackService;
import org.elasticsearch.xpack.watcher.rest.action.RestAckWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestActivateWatchAction;
import org.elasticsearch.xpack.watcher.rest.action.RestActivateWatchAction.DeactivateRestHandler;
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
import org.elasticsearch.xpack.watcher.transform.script.WatcherTransformScript;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class Watcher extends Plugin implements SystemIndexPlugin, ScriptPlugin, ReloadablePlugin {

    // This setting is only here for backward compatibility reasons as 6.x indices made use of it. It can be removed in 8.x.
    @Deprecated
    public static final Setting<String> INDEX_WATCHER_TEMPLATE_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.template.version", "", Function.identity(), Setting.Property.IndexScope);
    public static final Setting<Boolean> ENCRYPT_SENSITIVE_DATA_SETTING =
            Setting.boolSetting("xpack.watcher.encrypt_sensitive_data", false, Setting.Property.NodeScope);
    public static final Setting<TimeValue> MAX_STOP_TIMEOUT_SETTING =
            Setting.timeSetting("xpack.watcher.stop.timeout", TimeValue.timeValueSeconds(30), Setting.Property.NodeScope);
    private static final Setting<Integer> SETTING_BULK_ACTIONS =
        Setting.intSetting("xpack.watcher.bulk.actions", 1, 1, 10000, NodeScope);
    private static final Setting<Integer> SETTING_BULK_CONCURRENT_REQUESTS =
        Setting.intSetting("xpack.watcher.bulk.concurrent_requests", 0, 0, 20, NodeScope);
    private static final Setting<TimeValue> SETTING_BULK_FLUSH_INTERVAL =
        Setting.timeSetting("xpack.watcher.bulk.flush_interval", TimeValue.timeValueSeconds(1), NodeScope);
    private static final Setting<ByteSizeValue> SETTING_BULK_SIZE =
        Setting.byteSizeSetting("xpack.watcher.bulk.size", new ByteSizeValue(1, ByteSizeUnit.MB),
            new ByteSizeValue(1, ByteSizeUnit.MB), new ByteSizeValue(10, ByteSizeUnit.MB), NodeScope);

    public static final ScriptContext<TemplateScript.Factory> SCRIPT_TEMPLATE_CONTEXT
        = new ScriptContext<>("xpack_template", TemplateScript.Factory.class);

    private static final Logger logger = LogManager.getLogger(Watcher.class);
    private WatcherIndexingListener listener;
    private HttpClient httpClient;
    private BulkProcessor bulkProcessor;

    protected final Settings settings;
    protected final boolean enabled;
    protected List<NotificationService> reloadableServices = new ArrayList<>();

    public Watcher(final Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);

        if (enabled) {
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

        new WatcherIndexTemplateRegistry(environment.settings(), clusterService, threadPool, client, xContentRegistry);

        final SSLService sslService = getSslService();
        // http client
        httpClient = new HttpClient(settings, sslService, cryptoService, clusterService);

        // notification
        EmailService emailService = new EmailService(settings, cryptoService, sslService, clusterService.getClusterSettings());
        JiraService jiraService = new JiraService(settings, httpClient, clusterService.getClusterSettings());
        SlackService slackService = new SlackService(settings, httpClient, clusterService.getClusterSettings());
        PagerDutyService pagerDutyService = new PagerDutyService(settings, httpClient, clusterService.getClusterSettings());

        reloadableServices.add(emailService);
        reloadableServices.add(jiraService);
        reloadableServices.add(slackService);
        reloadableServices.add(pagerDutyService);

        TextTemplateEngine templateEngine = new TextTemplateEngine(scriptService);
        Map<String, EmailAttachmentParser> emailAttachmentParsers = new HashMap<>();
        emailAttachmentParsers.put(HttpEmailAttachementParser.TYPE, new HttpEmailAttachementParser(httpClient, templateEngine));
        emailAttachmentParsers.put(DataAttachmentParser.TYPE, new DataAttachmentParser());
        emailAttachmentParsers.put(ReportingAttachmentParser.TYPE,
            new ReportingAttachmentParser(settings, httpClient, templateEngine, clusterService.getClusterSettings()));
        EmailAttachmentsParser emailAttachmentsParser = new EmailAttachmentsParser(emailAttachmentParsers);

        // conditions

        final ConditionRegistry conditionRegistry = new ConditionRegistry(
                Map.of(
                        InternalAlwaysCondition.TYPE, (c, id, p) -> InternalAlwaysCondition.parse(id, p),
                        NeverCondition.TYPE, (c, id, p) -> NeverCondition.parse(id, p),
                        ArrayCompareCondition.TYPE, ArrayCompareCondition::parse,
                        CompareCondition.TYPE, CompareCondition::parse,
                        ScriptCondition.TYPE, (c, id, p) -> ScriptCondition.parse(scriptService, id, p)),
                getClock());
        final TransformRegistry transformRegistry = new TransformRegistry(Map.of(
                ScriptTransform.TYPE, new ScriptTransformFactory(scriptService),
                SearchTransform.TYPE, new SearchTransformFactory(settings, client, xContentRegistry, scriptService)));

        // actions
        final Map<String, ActionFactory> actionFactoryMap = new HashMap<>();
        actionFactoryMap.put(EmailAction.TYPE, new EmailActionFactory(settings, emailService, templateEngine, emailAttachmentsParser));
        actionFactoryMap.put(WebhookAction.TYPE, new WebhookActionFactory(httpClient, templateEngine));
        actionFactoryMap.put(IndexAction.TYPE, new IndexActionFactory(settings, client));
        actionFactoryMap.put(LoggingAction.TYPE, new LoggingActionFactory(templateEngine));
        actionFactoryMap.put(JiraAction.TYPE, new JiraActionFactory(templateEngine, jiraService));
        actionFactoryMap.put(SlackAction.TYPE, new SlackActionFactory(templateEngine, slackService));
        actionFactoryMap.put(PagerDutyAction.TYPE, new PagerDutyActionFactory(templateEngine, pagerDutyService));
        final ActionRegistry registry = new ActionRegistry(actionFactoryMap, conditionRegistry, transformRegistry, getClock(),
            getLicenseState());

        // inputs
        final Map<String, InputFactory> inputFactories = new HashMap<>();
        inputFactories.put(SearchInput.TYPE, new SearchInputFactory(settings, client, xContentRegistry, scriptService));
        inputFactories.put(SimpleInput.TYPE, new SimpleInputFactory());
        inputFactories.put(HttpInput.TYPE, new HttpInputFactory(settings, httpClient, templateEngine));
        inputFactories.put(NoneInput.TYPE, new NoneInputFactory());
        inputFactories.put(TransformInput.TYPE, new TransformInputFactory(transformRegistry));
        final InputRegistry inputRegistry = new InputRegistry(inputFactories);
        inputFactories.put(ChainInput.TYPE, new ChainInputFactory(inputRegistry));

        bulkProcessor = BulkProcessor.builder(new OriginSettingClient(client, WATCHER_ORIGIN)::bulk, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    Map<String, String> triggeredFailures = Arrays.stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .filter(r -> r.getIndex().startsWith(TriggeredWatchStoreField.INDEX_NAME))
                        .collect(Collectors.toMap(BulkItemResponse::getId, BulkItemResponse::getFailureMessage));
                    Map<String, String> historyFailures = Arrays.stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .filter(r -> r.getIndex().startsWith(HistoryStoreField.INDEX_PREFIX))
                        .collect(Collectors.toMap(BulkItemResponse::getId, BulkItemResponse::getFailureMessage));
                    if (triggeredFailures.isEmpty() == false) {
                        String failure = triggeredFailures.values().stream().collect(Collectors.joining(", "));
                        logger.error("triggered watches could not be deleted {}, failure [{}]",
                            triggeredFailures.keySet(), Strings.substring(failure, 0, 2000));
                    }
                    if (historyFailures.isEmpty() == false) {
                        String failure = historyFailures.values().stream().collect(Collectors.joining(", "));
                        logger.error("watch history could not be written {}, failure [{}]",
                            historyFailures.keySet(), Strings.substring(failure, 0, 2000));
                    }

                    Map<String, String> overwrittenIds = Arrays.stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .filter(r -> r.getIndex().startsWith(HistoryStoreField.INDEX_PREFIX))
                        .filter(r -> r.getVersion() > 1)
                        .collect(Collectors.toMap(BulkItemResponse::getId, BulkItemResponse::getFailureMessage));
                    if (overwrittenIds.isEmpty() == false) {
                        String failure = overwrittenIds.values().stream().collect(Collectors.joining(", "));
                        logger.info("overwrote watch history entries {}, possible second execution of a triggered watch, failure [{}]",
                            overwrittenIds.keySet(), Strings.substring(failure, 0, 2000));
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("error executing bulk", failure);
            }
        })
            .setFlushInterval(SETTING_BULK_FLUSH_INTERVAL.get(settings))
            .setBulkActions(SETTING_BULK_ACTIONS.get(settings))
            .setBulkSize(SETTING_BULK_SIZE.get(settings))
            .setConcurrentRequests(SETTING_BULK_CONCURRENT_REQUESTS.get(settings))
            .build();

        HistoryStore historyStore = new HistoryStore(bulkProcessor);

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
        final TriggerService triggerService = new TriggerService(triggerEngines);

        final TriggeredWatch.Parser triggeredWatchParser = new TriggeredWatch.Parser(triggerService);
        final TriggeredWatchStore triggeredWatchStore = new TriggeredWatchStore(settings, client, triggeredWatchParser, bulkProcessor);

        final WatcherSearchTemplateService watcherSearchTemplateService =
                new WatcherSearchTemplateService(scriptService, xContentRegistry);
        final WatchExecutor watchExecutor = getWatchExecutor(threadPool);
        final WatchParser watchParser = new WatchParser(triggerService, registry, inputRegistry, cryptoService, getClock());

        final ExecutionService executionService = new ExecutionService(settings, historyStore, triggeredWatchStore, watchExecutor,
                getClock(), watchParser, clusterService, client, threadPool.generic());

        final Consumer<Iterable<TriggerEvent>> triggerEngineListener = getTriggerEngineListener(executionService);
        triggerService.register(triggerEngineListener);

        WatcherService watcherService = new WatcherService(settings, triggerService, triggeredWatchStore, executionService,
                watchParser, client);

        final WatcherLifeCycleService watcherLifeCycleService =
                new WatcherLifeCycleService(clusterService, watcherService);

        listener = new WatcherIndexingListener(watchParser, getClock(), triggerService);
        clusterService.addListener(listener);

        // note: clock is needed here until actions can be constructed directly instead of by guice
        return Arrays.asList(new ClockHolder(getClock()), registry, inputRegistry, historyStore, triggerService, triggeredWatchParser,
                watcherLifeCycleService, executionService, triggerEngineListener, watcherService, watchParser,
                configuredTriggerEngine, triggeredWatchStore, watcherSearchTemplateService, slackService, pagerDutyService);
    }

    protected TriggerEngine getTriggerEngine(Clock clock, ScheduleRegistry scheduleRegistry) {
        return new TickerScheduleTriggerEngine(settings, scheduleRegistry, clock);
    }

    protected WatchExecutor getWatchExecutor(ThreadPool threadPool) {
        return new InternalWatchExecutor(threadPool);
    }

    protected Consumer<Iterable<TriggerEvent>> getTriggerEngineListener(ExecutionService executionService) {
        return new AsyncTriggerEventConsumer(executionService);
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

        // bulk processor configuration
        settings.add(SETTING_BULK_ACTIONS);
        settings.add(SETTING_BULK_CONCURRENT_REQUESTS);
        settings.add(SETTING_BULK_FLUSH_INTERVAL);
        settings.add(SETTING_BULK_SIZE);

        // notification services
        settings.addAll(SlackService.getSettings());
        settings.addAll(EmailService.getSettings());
        settings.addAll(HtmlSanitizer.getSettings());
        settings.addAll(JiraService.getSettings());
        settings.addAll(PagerDutyService.getSettings());
        settings.addAll(ReportingAttachmentParser.getSettings());

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
    static int getWatcherThreadPoolSize(final Settings settings) {
        return getWatcherThreadPoolSize(Node.NODE_DATA_SETTING.get(settings), EsExecutors.numberOfProcessors(settings));
    }

    static int getWatcherThreadPoolSize(final boolean isDataNode, final int numberOfProcessors) {
        if (isDataNode) {
            final long size = Math.max(Math.min(5 * numberOfProcessors, 50), numberOfProcessors);
            return Math.toIntExact(size);
        } else {
            return 1;
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.WATCHER, WatcherUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.WATCHER, WatcherInfoTransportAction.class);
        if (false == enabled) {
            return Arrays.asList(usageAction, infoAction);
        }
        return Arrays.asList(new ActionHandler<>(PutWatchAction.INSTANCE, TransportPutWatchAction.class),
                new ActionHandler<>(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class),
                new ActionHandler<>(GetWatchAction.INSTANCE, TransportGetWatchAction.class),
                new ActionHandler<>(WatcherStatsAction.INSTANCE, TransportWatcherStatsAction.class),
                new ActionHandler<>(AckWatchAction.INSTANCE, TransportAckWatchAction.class),
                new ActionHandler<>(ActivateWatchAction.INSTANCE, TransportActivateWatchAction.class),
                new ActionHandler<>(WatcherServiceAction.INSTANCE, TransportWatcherServiceAction.class),
                new ActionHandler<>(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class),
                usageAction,
                infoAction);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (false == enabled) {
            return emptyList();
        }
        return Arrays.asList(
                new RestPutWatchAction(),
                new RestDeleteWatchAction(),
                new RestWatcherStatsAction(),
                new RestGetWatchAction(),
                new RestWatchServiceAction(),
                new RestWatchServiceAction.StopRestHandler(),
                new RestAckWatchAction(),
                new RestActivateWatchAction(),
                new DeactivateRestHandler(),
                new RestExecuteWatchAction());
    }

    @Override
    public void onIndexModule(IndexModule module) {
        if (enabled == false) {
            return;
        }

        assert listener != null;
        // Attach a listener to every index so that we can react to alias changes.
        // This listener will be a no-op except on the index pointed to by .watches
        module.addIndexOperationListener(listener);
    }

    static void validAutoCreateIndex(Settings settings, Logger logger) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }

        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too" +
                " restrictive. disable [action.auto_create_index] or set it to " +
                "[{},{},{}*]", (Object) value, Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME, HistoryStoreField.INDEX_PREFIX);
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
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
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
                "[.watcher-history-yyyy.MM.dd] are allowed to be created", value);
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
        return Collections.singletonList(new EncryptSensitiveDataBootstrapCheck());
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Arrays.asList(WatcherTransformScript.CONTEXT, WatcherConditionScript.CONTEXT, Watcher.SCRIPT_TEMPLATE_CONTEXT);
    }

    @Override
    public void close() throws IOException {
        if (enabled) {
            bulkProcessor.flush();
        }
        IOUtils.closeWhileHandlingException(httpClient);
        try {
            if (enabled && bulkProcessor.awaitClose(10, TimeUnit.SECONDS) == false) {
                logger.warn("failed to properly close watcher bulk processor");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Reloads all the reloadable services in watcher.
     */
    @Override
    public void reload(Settings settings) {
        if (enabled == false) {
            return;
        }
        reloadableServices.forEach(s -> s.reload(settings));
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
            new SystemIndexDescriptor(Watch.INDEX, "Contains Watch definitions"),
            new SystemIndexDescriptor(TriggeredWatchStoreField.INDEX_NAME, "Used to track current and queued Watch execution")
        ));
    }
}
