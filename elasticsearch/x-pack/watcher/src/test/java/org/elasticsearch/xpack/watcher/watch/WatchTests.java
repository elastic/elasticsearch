/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpMethod;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuthFactory;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.email.DataAttachment;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.EmailTemplate;
import org.elasticsearch.xpack.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.notification.email.Profile;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachments;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachmentsParser;
import org.elasticsearch.xpack.support.clock.Clock;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.support.clock.SystemClock;
import org.elasticsearch.xpack.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.actions.ActionRegistry;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.actions.ExecutableActions;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.email.EmailActionFactory;
import org.elasticsearch.xpack.watcher.actions.email.ExecutableEmailAction;
import org.elasticsearch.xpack.watcher.actions.index.ExecutableIndexAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexActionFactory;
import org.elasticsearch.xpack.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.xpack.watcher.actions.webhook.ExecutableWebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookActionFactory;
import org.elasticsearch.xpack.watcher.condition.ConditionFactory;
import org.elasticsearch.xpack.watcher.condition.ConditionRegistry;
import org.elasticsearch.xpack.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.always.AlwaysConditionFactory;
import org.elasticsearch.xpack.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.compare.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.compare.CompareCondition.Op;
import org.elasticsearch.xpack.watcher.condition.compare.CompareConditionFactory;
import org.elasticsearch.xpack.watcher.condition.compare.ExecutableCompareCondition;
import org.elasticsearch.xpack.watcher.condition.compare.array.ArrayCompareCondition;
import org.elasticsearch.xpack.watcher.condition.compare.array.ArrayCompareConditionFactory;
import org.elasticsearch.xpack.watcher.condition.compare.array.ExecutableArrayCompareCondition;
import org.elasticsearch.xpack.watcher.condition.script.ExecutableScriptCondition;
import org.elasticsearch.xpack.watcher.condition.script.ScriptCondition;
import org.elasticsearch.xpack.watcher.condition.script.ScriptConditionFactory;
import org.elasticsearch.xpack.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.watcher.input.InputBuilders;
import org.elasticsearch.xpack.watcher.input.InputFactory;
import org.elasticsearch.xpack.watcher.input.InputRegistry;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInputFactory;
import org.elasticsearch.xpack.watcher.support.WatcherScript;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.watcher.transform.chain.ChainTransform;
import org.elasticsearch.xpack.watcher.transform.chain.ExecutableChainTransform;
import org.elasticsearch.xpack.watcher.transform.script.ExecutableScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransformFactory;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;
import org.elasticsearch.xpack.watcher.trigger.Trigger;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.trigger.schedule.CronSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.DailySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.HourlySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.MonthlySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.WeeklySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.YearlySchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.DayOfWeek;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.Month;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.MonthTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.WeekTimes;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.YearTimes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.matchAllRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Mockito.mock;

public class WatchTests extends ESTestCase {
    private ScriptService scriptService;
    private WatcherClientProxy client;
    private HttpClient httpClient;
    private EmailService emailService;
    private TextTemplateEngine templateEngine;
    private HtmlSanitizer htmlSanitizer;
    private HttpAuthRegistry authRegistry;
    private XPackLicenseState licenseState;
    private ESLogger logger;
    private Settings settings = Settings.EMPTY;
    private WatcherSearchTemplateService searchTemplateService;

    @Before
    public void init() throws Exception {
        scriptService = mock(ScriptService.class);
        client = mock(WatcherClientProxy.class);
        httpClient = mock(HttpClient.class);
        emailService = mock(EmailService.class);
        templateEngine = mock(TextTemplateEngine.class);
        htmlSanitizer = mock(HtmlSanitizer.class);
        licenseState = mock(XPackLicenseState.class);
        authRegistry = new HttpAuthRegistry(singletonMap("basic", new BasicAuthFactory(null)));
        logger = Loggers.getLogger(WatchTests.class);
        searchTemplateService = mock(WatcherSearchTemplateService.class);
    }

    public void testParserSelfGenerated() throws Exception {
        DateTime now = new DateTime(UTC);
        ClockMock clock = new ClockMock();
        clock.setTime(now);
        TransformRegistry transformRegistry = transformRegistry();
        boolean includeStatus = randomBoolean();
        Schedule schedule = randomSchedule();
        Trigger trigger = new ScheduleTrigger(schedule);
        ScheduleRegistry scheduleRegistry = registry(schedule);
        TriggerEngine triggerEngine = new ParseOnlyScheduleTriggerEngine(Settings.EMPTY, scheduleRegistry, clock);
        TriggerService triggerService = new TriggerService(Settings.EMPTY, singleton(triggerEngine));

        ExecutableInput input = randomInput();
        InputRegistry inputRegistry = registry(input);

        ExecutableCondition condition = randomCondition();
        ConditionRegistry conditionRegistry = registry(condition);

        ExecutableTransform transform = randomTransform();

        ExecutableActions actions = randomActions();
        ActionRegistry actionRegistry = registry(actions, transformRegistry);

        Map<String, Object> metadata = singletonMap("_key", "_val");

        Map<String, ActionStatus> actionsStatuses = new HashMap<>();
        for (ActionWrapper action : actions) {
            actionsStatuses.put(action.id(), new ActionStatus(now));
        }
        WatchStatus watchStatus = new WatchStatus(clock.nowUTC(), unmodifiableMap(actionsStatuses));

        TimeValue throttlePeriod = randomBoolean() ? null : TimeValue.timeValueSeconds(randomIntBetween(5, 10));

        Watch watch = new Watch("_name", trigger, input, condition, transform, throttlePeriod, actions, metadata, watchStatus);

        BytesReference bytes = XContentFactory.jsonBuilder().value(watch).bytes();
        logger.info("{}", bytes.utf8ToString());
        Watch.Parser watchParser = new Watch.Parser(settings, conditionRegistry, triggerService, transformRegistry, actionRegistry,
                inputRegistry, null, clock);

        Watch parsedWatch = watchParser.parse("_name", includeStatus, bytes);

        if (includeStatus) {
            assertThat(parsedWatch.status(), equalTo(watchStatus));
        }
        assertThat(parsedWatch.trigger(), equalTo(trigger));
        assertThat(parsedWatch.input(), equalTo(input));
        assertThat(parsedWatch.condition(), equalTo(condition));
        if (throttlePeriod != null) {
            assertThat(parsedWatch.throttlePeriod().millis(), equalTo(throttlePeriod.millis()));
        }
        assertThat(parsedWatch.metadata(), equalTo(metadata));
        assertThat(parsedWatch.actions(), equalTo(actions));
    }

    public void testParserBadActions() throws Exception {
        ClockMock clock = new ClockMock();
        ScheduleRegistry scheduleRegistry = registry(randomSchedule());
        TriggerEngine triggerEngine = new ParseOnlyScheduleTriggerEngine(Settings.EMPTY, scheduleRegistry, clock);
        TriggerService triggerService = new TriggerService(Settings.EMPTY, singleton(triggerEngine));
        ExecutableCondition condition = randomCondition();
        ConditionRegistry conditionRegistry = registry(condition);
        ExecutableInput input = randomInput();
        InputRegistry inputRegistry = registry(input);

        TransformRegistry transformRegistry = transformRegistry();

        ExecutableActions actions = randomActions();
        ActionRegistry actionRegistry = registry(actions, transformRegistry);


        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startArray("actions").endArray()
                .endObject();
        Watch.Parser watchParser = new Watch.Parser(settings, conditionRegistry, triggerService, transformRegistry, actionRegistry,
                inputRegistry, null, clock);
        try {
            watchParser.parse("failure", false, jsonBuilder.bytes());
            fail("This watch should fail to parse as actions is an array");
        } catch (ElasticsearchParseException pe) {
            assertThat(pe.getMessage().contains("could not parse actions for watch [failure]"), is(true));
        }
    }

    public void testParserDefaults() throws Exception {
        Schedule schedule = randomSchedule();
        ScheduleRegistry scheduleRegistry = registry(schedule);
        TriggerEngine triggerEngine = new ParseOnlyScheduleTriggerEngine(Settings.EMPTY, scheduleRegistry, SystemClock.INSTANCE);
        TriggerService triggerService = new TriggerService(Settings.EMPTY, singleton(triggerEngine));

        ConditionRegistry conditionRegistry = registry(new ExecutableAlwaysCondition(logger));
        InputRegistry inputRegistry = registry(new ExecutableNoneInput(logger));
        TransformRegistry transformRegistry = transformRegistry();
        ExecutableActions actions =  new ExecutableActions(Collections.emptyList());
        ActionRegistry actionRegistry = registry(actions, transformRegistry);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(Watch.Field.TRIGGER.getPreferredName())
                .field(ScheduleTrigger.TYPE, schedule(schedule).build())
                .endObject();
        builder.endObject();
        Watch.Parser watchParser = new Watch.Parser(settings, conditionRegistry, triggerService, transformRegistry, actionRegistry,
                inputRegistry, null, SystemClock.INSTANCE);
        Watch watch = watchParser.parse("failure", false, builder.bytes());
        assertThat(watch, notNullValue());
        assertThat(watch.trigger(), instanceOf(ScheduleTrigger.class));
        assertThat(watch.input(), instanceOf(ExecutableNoneInput.class));
        assertThat(watch.condition(), instanceOf(ExecutableAlwaysCondition.class));
        assertThat(watch.transform(), nullValue());
        assertThat(watch.actions(), notNullValue());
        assertThat(watch.actions().count(), is(0));
    }

    private static Schedule randomSchedule() {
        String type = randomFrom(CronSchedule.TYPE, HourlySchedule.TYPE, DailySchedule.TYPE, WeeklySchedule.TYPE, MonthlySchedule.TYPE,
                YearlySchedule.TYPE, IntervalSchedule.TYPE);
        switch (type) {
            case CronSchedule.TYPE:
                return new CronSchedule("0/5 * * * * ? *");
            case HourlySchedule.TYPE:
                return HourlySchedule.builder().minutes(30).build();
            case DailySchedule.TYPE:
                return DailySchedule.builder().atNoon().build();
            case WeeklySchedule.TYPE:
                return WeeklySchedule.builder().time(WeekTimes.builder().on(DayOfWeek.FRIDAY).atMidnight()).build();
            case MonthlySchedule.TYPE:
                return MonthlySchedule.builder().time(MonthTimes.builder().on(1).atNoon()).build();
            case YearlySchedule.TYPE:
                return YearlySchedule.builder().time(YearTimes.builder().in(Month.JANUARY).on(1).atMidnight()).build();
            default:
                return new IntervalSchedule(IntervalSchedule.Interval.seconds(5));
        }
    }

    private static ScheduleRegistry registry(Schedule schedule) {
        Map<String, Schedule.Parser> parsers = new HashMap<>();
        switch (schedule.type()) {
            case CronSchedule.TYPE:
                parsers.put(CronSchedule.TYPE, new CronSchedule.Parser());
                return new ScheduleRegistry(parsers);
            case HourlySchedule.TYPE:
                parsers.put(HourlySchedule.TYPE, new HourlySchedule.Parser());
                return new ScheduleRegistry(parsers);
            case DailySchedule.TYPE:
                parsers.put(DailySchedule.TYPE, new DailySchedule.Parser());
                return new ScheduleRegistry(parsers);
            case WeeklySchedule.TYPE:
                parsers.put(WeeklySchedule.TYPE, new WeeklySchedule.Parser());
                return new ScheduleRegistry(parsers);
            case MonthlySchedule.TYPE:
                parsers.put(MonthlySchedule.TYPE, new MonthlySchedule.Parser());
                return new ScheduleRegistry(parsers);
            case YearlySchedule.TYPE:
                parsers.put(YearlySchedule.TYPE, new YearlySchedule.Parser());
                return new ScheduleRegistry(parsers);
            case IntervalSchedule.TYPE:
                parsers.put(IntervalSchedule.TYPE, new IntervalSchedule.Parser());
                return new ScheduleRegistry(parsers);
            default:
                throw new IllegalArgumentException("unknown schedule [" + schedule + "]");
        }
    }

    private ExecutableInput randomInput() {
        String type = randomFrom(SearchInput.TYPE, SimpleInput.TYPE);
        switch (type) {
            case SearchInput.TYPE:
                SearchInput searchInput = searchInput(WatcherTestUtils.newInputSearchRequest("idx")).build();
                return new ExecutableSearchInput(searchInput, logger, client, searchTemplateService, null);
            default:
                SimpleInput simpleInput = InputBuilders.simpleInput(singletonMap("_key", "_val")).build();
                return new ExecutableSimpleInput(simpleInput, logger);
        }
    }

    private InputRegistry registry(ExecutableInput input) {
        Map<String, InputFactory> parsers = new HashMap<>();
        switch (input.type()) {
            case SearchInput.TYPE:
                IndicesQueriesRegistry queryRegistry = new IndicesQueriesRegistry();
                QueryParser<MatchAllQueryBuilder> queryParser = MatchAllQueryBuilder::fromXContent;
                queryRegistry.register(queryParser, MatchAllQueryBuilder.NAME);
                parsers.put(SearchInput.TYPE, new SearchInputFactory(settings, client, queryRegistry, null, null, scriptService));
                return new InputRegistry(Settings.EMPTY, parsers);
            default:
                parsers.put(SimpleInput.TYPE, new SimpleInputFactory(settings));
                return new InputRegistry(Settings.EMPTY, parsers);
        }
    }

    private ExecutableCondition randomCondition() {
        String type = randomFrom(ScriptCondition.TYPE, AlwaysCondition.TYPE, CompareCondition.TYPE, ArrayCompareCondition.TYPE);
        switch (type) {
            case ScriptCondition.TYPE:
                return new ExecutableScriptCondition(new ScriptCondition(WatcherScript.inline("_script").build()), logger, scriptService);
            case CompareCondition.TYPE:
                return new ExecutableCompareCondition(new CompareCondition("_path", randomFrom(Op.values()), randomFrom(5, "3")), logger,
                        SystemClock.INSTANCE);
            case ArrayCompareCondition.TYPE:
                return new ExecutableArrayCompareCondition(new ArrayCompareCondition("_array_path", "_path",
                        randomFrom(ArrayCompareCondition.Op.values()), randomFrom(5, "3"), ArrayCompareCondition.Quantifier.SOME),
                        logger, SystemClock.INSTANCE);
            default:
                return new ExecutableAlwaysCondition(logger);
        }
    }

    private ConditionRegistry registry(ExecutableCondition condition) {
        Map<String, ConditionFactory> parsers = new HashMap<>();
        switch (condition.type()) {
            case ScriptCondition.TYPE:
                parsers.put(ScriptCondition.TYPE, new ScriptConditionFactory(settings, scriptService));
                return new ConditionRegistry(parsers);
            case CompareCondition.TYPE:
                parsers.put(CompareCondition.TYPE, new CompareConditionFactory(settings, SystemClock.INSTANCE));
                return new ConditionRegistry(parsers);
            case ArrayCompareCondition.TYPE:
                parsers.put(ArrayCompareCondition.TYPE, new ArrayCompareConditionFactory(settings, SystemClock.INSTANCE));
                return new ConditionRegistry(parsers);
            default:
                parsers.put(AlwaysCondition.TYPE, new AlwaysConditionFactory(settings));
                return new ConditionRegistry(parsers);
        }
    }

    private ExecutableTransform randomTransform() {
        String type = randomFrom(ScriptTransform.TYPE, SearchTransform.TYPE, ChainTransform.TYPE);
        TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(5) : null;
        DateTimeZone timeZone = randomBoolean() ? DateTimeZone.UTC : null;
        switch (type) {
            case ScriptTransform.TYPE:
                return new ExecutableScriptTransform(new ScriptTransform(WatcherScript.inline("_script").build()), logger, scriptService);
            case SearchTransform.TYPE:
                SearchTransform transform = new SearchTransform(
                        new WatcherSearchTemplateRequest(matchAllRequest(DEFAULT_INDICES_OPTIONS), null), timeout, timeZone);
                return new ExecutableSearchTransform(transform, logger, client, searchTemplateService, null);
            default: // chain
                SearchTransform searchTransform = new SearchTransform(
                        new WatcherSearchTemplateRequest(matchAllRequest(DEFAULT_INDICES_OPTIONS), null), timeout, timeZone);
                ScriptTransform scriptTransform = new ScriptTransform(WatcherScript.inline("_script").build());

                ChainTransform chainTransform = new ChainTransform(Arrays.asList(searchTransform, scriptTransform));
                return new ExecutableChainTransform(chainTransform, logger, Arrays.<ExecutableTransform>asList(
                        new ExecutableSearchTransform(new SearchTransform(
                                new WatcherSearchTemplateRequest(matchAllRequest(DEFAULT_INDICES_OPTIONS), null), timeout, timeZone),
                                logger, client, searchTemplateService, null),
                        new ExecutableScriptTransform(new ScriptTransform(WatcherScript.inline("_script").build()),
                            logger, scriptService)));
        }
    }

    private TransformRegistry transformRegistry() {
        IndicesQueriesRegistry queryRegistry = new IndicesQueriesRegistry();
        QueryParser<MatchAllQueryBuilder> queryParser = MatchAllQueryBuilder::fromXContent;
        queryRegistry.register(queryParser, MatchAllQueryBuilder.NAME);
        Map<String, TransformFactory> factories = new HashMap<>();
        factories.put(ScriptTransform.TYPE, new ScriptTransformFactory(settings, scriptService));
        factories.put(SearchTransform.TYPE, new SearchTransformFactory(settings, client, queryRegistry, null, null, scriptService));
        TransformRegistry registry = new TransformRegistry(Settings.EMPTY, unmodifiableMap(factories));
        return registry;
    }

    private ExecutableActions randomActions() {
        List<ActionWrapper> list = new ArrayList<>();
        if (randomBoolean()) {
            ExecutableTransform transform = randomTransform();
            EmailAction action = new EmailAction(EmailTemplate.builder().build(), null, null, Profile.STANDARD,
                    randomFrom(DataAttachment.JSON, DataAttachment.YAML), EmailAttachments.EMPTY_ATTACHMENTS);
            list.add(new ActionWrapper("_email_" + randomAsciiOfLength(8), randomThrottler(), transform,
                    new ExecutableEmailAction(action, logger, emailService, templateEngine, htmlSanitizer, Collections.emptyMap())));
        }
        if (randomBoolean()) {
            DateTimeZone timeZone = randomBoolean() ? DateTimeZone.UTC : null;
            TimeValue timeout = randomBoolean() ? TimeValue.timeValueSeconds(30) : null;
            IndexAction action = new IndexAction("_index", "_type", null, timeout, timeZone);
            list.add(new ActionWrapper("_index_" + randomAsciiOfLength(8), randomThrottler(), randomTransform(),
                    new ExecutableIndexAction(action, logger, client, null)));
        }
        if (randomBoolean()) {
            HttpRequestTemplate httpRequest = HttpRequestTemplate.builder("test.host", randomIntBetween(8000, 9000))
                    .method(randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT))
                    .path(TextTemplate.inline("_url").build())
                    .build();
            WebhookAction action = new WebhookAction(httpRequest);
            list.add(new ActionWrapper("_webhook_" + randomAsciiOfLength(8), randomThrottler(), randomTransform(),
                    new ExecutableWebhookAction(action, logger, httpClient, templateEngine)));
        }
        return new ExecutableActions(list);
    }

    private ActionRegistry registry(ExecutableActions actions, TransformRegistry transformRegistry) {
        Map<String, ActionFactory> parsers = new HashMap<>();
        for (ActionWrapper action : actions) {
            switch (action.action().type()) {
                case EmailAction.TYPE:
                    parsers.put(EmailAction.TYPE, new EmailActionFactory(settings, emailService, templateEngine,
                            new EmailAttachmentsParser(Collections.emptyMap())));
                    break;
                case IndexAction.TYPE:
                    parsers.put(IndexAction.TYPE, new IndexActionFactory(settings, client));
                    break;
                case WebhookAction.TYPE:
                    parsers.put(WebhookAction.TYPE, new WebhookActionFactory(settings,  httpClient,
                            new HttpRequestTemplate.Parser(authRegistry), templateEngine));
                    break;
            }
        }
        return new ActionRegistry(unmodifiableMap(parsers), transformRegistry, SystemClock.INSTANCE, licenseState);
    }

    private ActionThrottler randomThrottler() {
        return new ActionThrottler(SystemClock.INSTANCE, randomBoolean() ? null : TimeValue.timeValueMinutes(randomIntBetween(3, 5)),
                licenseState);
    }

    static class ParseOnlyScheduleTriggerEngine extends ScheduleTriggerEngine {

        public ParseOnlyScheduleTriggerEngine(Settings settings, ScheduleRegistry registry, Clock clock) {
            super(settings, registry, clock);
        }

        @Override
        public void start(Collection<Job> jobs) {
        }

        @Override
        public void stop() {
        }

        @Override
        public void register(Listener listener) {
        }

        @Override
        public void add(Job job) {
        }

        @Override
        public boolean remove(String jobId) {
            return false;
        }
    }
}
