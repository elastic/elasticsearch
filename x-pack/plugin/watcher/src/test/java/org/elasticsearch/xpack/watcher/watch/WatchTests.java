/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.core.watcher.actions.ActionRegistry;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.xpack.core.watcher.condition.ConditionFactory;
import org.elasticsearch.xpack.core.watcher.condition.ConditionRegistry;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.core.watcher.transform.chain.ChainTransform;
import org.elasticsearch.xpack.core.watcher.transform.chain.ExecutableChainTransform;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.email.EmailActionFactory;
import org.elasticsearch.xpack.watcher.actions.email.ExecutableEmailAction;
import org.elasticsearch.xpack.watcher.actions.index.ExecutableIndexAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexActionFactory;
import org.elasticsearch.xpack.watcher.actions.logging.ExecutableLoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingActionFactory;
import org.elasticsearch.xpack.watcher.actions.webhook.ExecutableWebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookActionFactory;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.condition.AlwaysConditionTests;
import org.elasticsearch.xpack.watcher.condition.ArrayCompareCondition;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.input.InputBuilders;
import org.elasticsearch.xpack.watcher.input.InputRegistry;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInput;
import org.elasticsearch.xpack.watcher.input.search.SearchInputFactory;
import org.elasticsearch.xpack.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInputFactory;
import org.elasticsearch.xpack.watcher.notification.email.DataAttachment;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.EmailTemplate;
import org.elasticsearch.xpack.watcher.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.watcher.notification.email.Profile;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachments;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentsParser;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.transform.script.ExecutableScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransformFactory;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;
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
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class WatchTests extends ESTestCase {

    private ScriptService scriptService;
    private Client client;
    private HttpClient httpClient;
    private EmailService emailService;
    private TextTemplateEngine templateEngine;
    private HtmlSanitizer htmlSanitizer;
    private XPackLicenseState licenseState;
    private Logger logger;
    private Settings settings = Settings.EMPTY;
    private WatcherSearchTemplateService searchTemplateService;

    @Before
    public void init() throws Exception {
        scriptService = mock(ScriptService.class);
        client = mock(Client.class);
        httpClient = mock(HttpClient.class);
        emailService = mock(EmailService.class);
        templateEngine = mock(TextTemplateEngine.class);
        htmlSanitizer = mock(HtmlSanitizer.class);
        licenseState = mock(XPackLicenseState.class);
        logger = LogManager.getLogger(WatchTests.class);
        searchTemplateService = mock(WatcherSearchTemplateService.class);
    }

    public void testParserSelfGenerated() throws Exception {
        Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        TransformRegistry transformRegistry = transformRegistry();
        boolean includeStatus = randomBoolean();
        Schedule schedule = randomSchedule();
        Trigger trigger = new ScheduleTrigger(schedule);
        ScheduleRegistry scheduleRegistry = registry(schedule);
        TriggerEngine<?, ?> triggerEngine = new ParseOnlyScheduleTriggerEngine(scheduleRegistry, clock);
        TriggerService triggerService = new TriggerService(singleton(triggerEngine));

        ExecutableInput<?, ?> input = randomInput();
        InputRegistry inputRegistry = registry(input.type());

        ExecutableCondition condition = AlwaysConditionTests.randomCondition(scriptService);
        ConditionRegistry conditionRegistry = conditionRegistry();

        ExecutableTransform<?, ?> transform = randomTransform();

        List<ActionWrapper> actions = randomActions();
        ActionRegistry actionRegistry = registry(actions, conditionRegistry, transformRegistry);

        Map<String, Object> metadata = singletonMap("_key", "_val");

        Map<String, ActionStatus> actionsStatuses = new HashMap<>();
        for (ActionWrapper action : actions) {
            actionsStatuses.put(action.id(), new ActionStatus(now));
        }
        WatchStatus watchStatus = new WatchStatus(now, unmodifiableMap(actionsStatuses));

        TimeValue throttlePeriod = randomBoolean() ? null : TimeValue.timeValueSeconds(randomIntBetween(5, 10000));

        final long sourceSeqNo = randomNonNegativeLong();
        final long sourcePrimaryTerm = randomLongBetween(1, 200);
        Watch watch = new Watch(
            "_name",
            trigger,
            input,
            condition,
            transform,
            throttlePeriod,
            actions,
            metadata,
            watchStatus,
            sourceSeqNo,
            sourcePrimaryTerm
        );

        BytesReference bytes = BytesReference.bytes(jsonBuilder().value(watch));
        logger.info("{}", bytes.utf8ToString());
        WatchParser watchParser = new WatchParser(triggerService, actionRegistry, inputRegistry, null, clock);

        Watch parsedWatch = watchParser.parse("_name", includeStatus, bytes, XContentType.JSON, sourceSeqNo, sourcePrimaryTerm);

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
        assertThat(parsedWatch.getSourceSeqNo(), equalTo(sourceSeqNo));
        assertThat(parsedWatch.getSourcePrimaryTerm(), equalTo(sourcePrimaryTerm));
    }

    public void testThatBothStatusFieldsCanBeRead() throws Exception {
        InputRegistry inputRegistry = mock(InputRegistry.class);
        ActionRegistry actionRegistry = mock(ActionRegistry.class);
        // a fake trigger service that advances past the trigger end object, which cannot be done with mocking
        TriggerService triggerService = new TriggerService(Collections.emptySet()) {
            @Override
            public Trigger parseTrigger(String jobName, XContentParser parser) throws IOException {
                while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                }

                return new ScheduleTrigger(randomSchedule());
            }
        };

        Clock fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

        ClockMock clock = ClockMock.frozen();
        ZonedDateTime now = Instant.ofEpochMilli(fixedClock.millis()).atZone(ZoneOffset.UTC);
        clock.setTime(now);

        List<ActionWrapper> actions = randomActions();
        Map<String, ActionStatus> actionsStatuses = new HashMap<>();
        for (ActionWrapper action : actions) {
            actionsStatuses.put(action.id(), new ActionStatus(now));
        }
        WatchStatus watchStatus = new WatchStatus(clock.instant().atZone(ZoneOffset.UTC), unmodifiableMap(actionsStatuses));

        WatchParser watchParser = new WatchParser(triggerService, actionRegistry, inputRegistry, null, clock);
        XContentBuilder builder = jsonBuilder().startObject().startObject("trigger").endObject().field("status", watchStatus).endObject();
        Watch watch = watchParser.parse("foo", true, BytesReference.bytes(builder), XContentType.JSON, 1L, 1L);
        assertThat(watch.status().state().getTimestamp().toInstant().toEpochMilli(), is(clock.millis()));
        for (ActionWrapper action : actions) {
            assertThat(watch.status().actionStatus(action.id()), is(actionsStatuses.get(action.id())));
        }
    }

    public void testParserBadActions() throws Exception {
        ClockMock clock = ClockMock.frozen();
        ScheduleRegistry scheduleRegistry = registry(randomSchedule());
        TriggerEngine<?, ?> triggerEngine = new ParseOnlyScheduleTriggerEngine(scheduleRegistry, clock);
        TriggerService triggerService = new TriggerService(singleton(triggerEngine));
        ConditionRegistry conditionRegistry = conditionRegistry();
        ExecutableInput<?, ?> input = randomInput();
        InputRegistry inputRegistry = registry(input.type());

        TransformRegistry transformRegistry = transformRegistry();

        List<ActionWrapper> actions = randomActions();
        ActionRegistry actionRegistry = registry(actions, conditionRegistry, transformRegistry);

        XContentBuilder jsonBuilder = jsonBuilder().startObject().startArray("actions").endArray().endObject();
        WatchParser watchParser = new WatchParser(triggerService, actionRegistry, inputRegistry, null, clock);
        try {
            watchParser.parse("failure", false, BytesReference.bytes(jsonBuilder), XContentType.JSON, 1L, 1L);
            fail("This watch should fail to parse as actions is an array");
        } catch (ElasticsearchParseException pe) {
            assertThat(pe.getMessage().contains("could not parse actions for watch [failure]"), is(true));
        }
    }

    public void testParserDefaults() throws Exception {
        Schedule schedule = randomSchedule();
        ScheduleRegistry scheduleRegistry = registry(schedule);
        TriggerEngine<?, ?> triggerEngine = new ParseOnlyScheduleTriggerEngine(scheduleRegistry, Clock.systemUTC());
        TriggerService triggerService = new TriggerService(singleton(triggerEngine));

        ConditionRegistry conditionRegistry = conditionRegistry();
        InputRegistry inputRegistry = registry(new ExecutableNoneInput().type());
        TransformRegistry transformRegistry = transformRegistry();
        ActionRegistry actionRegistry = registry(Collections.emptyList(), conditionRegistry, transformRegistry);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject(WatchField.TRIGGER.getPreferredName()).field(ScheduleTrigger.TYPE, schedule(schedule).build()).endObject();
        builder.endObject();
        WatchParser watchParser = new WatchParser(triggerService, actionRegistry, inputRegistry, null, Clock.systemUTC());
        Watch watch = watchParser.parse("failure", false, BytesReference.bytes(builder), XContentType.JSON, 1L, 1L);
        assertThat(watch, notNullValue());
        assertThat(watch.trigger(), instanceOf(ScheduleTrigger.class));
        assertThat(watch.input(), instanceOf(ExecutableNoneInput.class));
        assertThat(watch.condition(), instanceOf(InternalAlwaysCondition.class));
        assertThat(watch.transform(), nullValue());
        assertThat(watch.actions(), notNullValue());
        assertThat(watch.actions().size(), is(0));
    }

    public void testParseWatch_verifyScriptLangDefault() throws Exception {
        ScheduleRegistry scheduleRegistry = registry(
            new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.SECONDS))
        );
        TriggerEngine<?, ?> triggerEngine = new ParseOnlyScheduleTriggerEngine(scheduleRegistry, Clock.systemUTC());
        TriggerService triggerService = new TriggerService(singleton(triggerEngine));

        ConditionRegistry conditionRegistry = conditionRegistry();
        InputRegistry inputRegistry = registry(SearchInput.TYPE);
        TransformRegistry transformRegistry = transformRegistry();
        ActionRegistry actionRegistry = registry(Collections.emptyList(), conditionRegistry, transformRegistry);
        WatchParser watchParser = new WatchParser(triggerService, actionRegistry, inputRegistry, null, Clock.systemUTC());

        WatcherSearchTemplateService searchTemplateService = new WatcherSearchTemplateService(scriptService, xContentRegistry());

        XContentBuilder builder = jsonBuilder();
        builder.startObject();

        builder.startObject("trigger");
        builder.startObject("schedule");
        builder.field("interval", "99w");
        builder.endObject();
        builder.endObject();

        builder.startObject("input");
        builder.startObject("search");
        builder.startObject("request");
        builder.startObject("body");
        builder.startObject("query");
        builder.startObject("script");
        if (randomBoolean()) {
            builder.field("script", "return true");
        } else {
            builder.startObject("script");
            builder.field("source", "return true");
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();

        builder.startObject("condition");
        if (randomBoolean()) {
            builder.field("script", "return true");
        } else {
            builder.startObject("script");
            builder.field("source", "return true");
            builder.endObject();
        }
        builder.endObject();

        builder.endObject();

        // parse in default mode:
        Watch watch = watchParser.parse("_id", false, BytesReference.bytes(builder), XContentType.JSON, 1L, 1L);
        assertThat(((ScriptCondition) watch.condition()).getScript().getLang(), equalTo(Script.DEFAULT_SCRIPT_LANG));
        WatcherSearchTemplateRequest request = ((SearchInput) watch.input().input()).getRequest();
        SearchRequest searchRequest = searchTemplateService.toSearchRequest(request);
        assertThat(((ScriptQueryBuilder) searchRequest.source().query()).script().getLang(), equalTo(Script.DEFAULT_SCRIPT_LANG));
    }

    public void testParseWatchWithoutInput() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();

            builder.startObject("trigger").startObject("schedule").field("interval", "99w").endObject().endObject();
            builder.startObject("condition").startObject("always").endObject().endObject();
            builder.startObject("actions")
                .startObject("logme")
                .startObject("logging")
                .field("text", "foo")
                .endObject()
                .endObject()
                .endObject();
            builder.endObject();

            WatchParser parser = createWatchparser();
            Watch watch = parser.parse("_id", false, BytesReference.bytes(builder), XContentType.JSON, 1L, 1L);
            assertThat(watch, is(notNullValue()));
            assertThat(watch.input().type(), is(NoneInput.TYPE));
        }
    }

    public void testParseWatchWithoutAction() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();

            builder.startObject("trigger").startObject("schedule").field("interval", "99w").endObject().endObject();
            builder.startObject("input").startObject("simple").endObject().endObject();
            builder.startObject("condition").startObject("always").endObject().endObject();
            builder.endObject();

            WatchParser parser = createWatchparser();
            Watch watch = parser.parse("_id", false, BytesReference.bytes(builder), XContentType.JSON, 1L, 1L);
            assertThat(watch, is(notNullValue()));
            assertThat(watch.actions(), hasSize(0));
        }
    }

    public void testParseWatchWithoutTriggerDoesNotWork() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();

            builder.startObject("input").startObject("simple").endObject().endObject();
            builder.startObject("condition").startObject("always").endObject().endObject();
            builder.startObject("actions")
                .startObject("logme")
                .startObject("logging")
                .field("text", "foo")
                .endObject()
                .endObject()
                .endObject();
            builder.endObject();

            WatchParser parser = createWatchparser();
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> parser.parse("_id", false, BytesReference.bytes(builder), XContentType.JSON, 1L, 1L)
            );
            assertThat(e.getMessage(), is("could not parse watch [_id]. missing required field [trigger]"));
        }
    }

    private WatchParser createWatchparser() throws Exception {
        LoggingAction loggingAction = new LoggingAction(new TextTemplate("foo"), null, null);
        List<ActionWrapper> actions = Collections.singletonList(
            new ActionWrapper(
                "_logging_",
                randomThrottler(),
                null,
                null,
                new ExecutableLoggingAction(loggingAction, logger, new MockTextTemplateEngine()),
                null,
                null
            )
        );

        ScheduleRegistry scheduleRegistry = registry(
            new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.SECONDS))
        );
        TriggerEngine<?, ?> triggerEngine = new ParseOnlyScheduleTriggerEngine(scheduleRegistry, Clock.systemUTC());
        TriggerService triggerService = new TriggerService(singleton(triggerEngine));

        ConditionRegistry conditionRegistry = conditionRegistry();
        InputRegistry inputRegistry = registry(SimpleInput.TYPE);
        TransformRegistry transformRegistry = transformRegistry();
        ActionRegistry actionRegistry = registry(actions, conditionRegistry, transformRegistry);

        return new WatchParser(triggerService, actionRegistry, inputRegistry, null, Clock.systemUTC());
    }

    private static Schedule randomSchedule() {
        String type = randomFrom(
            CronSchedule.TYPE,
            HourlySchedule.TYPE,
            DailySchedule.TYPE,
            WeeklySchedule.TYPE,
            MonthlySchedule.TYPE,
            YearlySchedule.TYPE,
            IntervalSchedule.TYPE
        );
        return switch (type) {
            case CronSchedule.TYPE -> new CronSchedule("0/5 * * * * ? *");
            case HourlySchedule.TYPE -> HourlySchedule.builder().minutes(30).build();
            case DailySchedule.TYPE -> DailySchedule.builder().atNoon().build();
            case WeeklySchedule.TYPE -> WeeklySchedule.builder().time(WeekTimes.builder().on(DayOfWeek.FRIDAY).atMidnight()).build();
            case MonthlySchedule.TYPE -> MonthlySchedule.builder().time(MonthTimes.builder().on(1).atNoon()).build();
            case YearlySchedule.TYPE -> YearlySchedule.builder().time(YearTimes.builder().in(Month.JANUARY).on(1).atMidnight()).build();
            default -> new IntervalSchedule(IntervalSchedule.Interval.seconds(5));
        };
    }

    private static ScheduleRegistry registry(Schedule schedule) {
        return new ScheduleRegistry(Set.of(switch (schedule.type()) {
            case CronSchedule.TYPE -> new CronSchedule.Parser();
            case HourlySchedule.TYPE -> new HourlySchedule.Parser();
            case DailySchedule.TYPE -> new DailySchedule.Parser();
            case WeeklySchedule.TYPE -> new WeeklySchedule.Parser();
            case MonthlySchedule.TYPE -> new MonthlySchedule.Parser();
            case YearlySchedule.TYPE -> new YearlySchedule.Parser();
            case IntervalSchedule.TYPE -> new IntervalSchedule.Parser();
            default -> throw new IllegalArgumentException("unknown schedule [" + schedule + "]");
        }));
    }

    private ExecutableInput<?, ?> randomInput() {
        String type = randomFrom(SearchInput.TYPE, SimpleInput.TYPE);
        return switch (type) {
            case SearchInput.TYPE -> new ExecutableSearchInput(
                searchInput(WatcherTestUtils.templateRequest(searchSource(), "idx")).timeout(
                    randomBoolean() ? null : timeValueSeconds(between(1, 10000))
                ).build(),
                client,
                searchTemplateService,
                null
            );
            default -> new ExecutableSimpleInput(InputBuilders.simpleInput(singletonMap("_key", "_val")).build());
        };
    }

    private InputRegistry registry(String inputType) {
        return switch (inputType) {
            case SearchInput.TYPE -> new InputRegistry(
                Map.of(SearchInput.TYPE, new SearchInputFactory(settings, client, xContentRegistry(), scriptService))
            );
            default -> new InputRegistry(Map.of(SimpleInput.TYPE, new SimpleInputFactory()));
        };
    }

    private ConditionRegistry conditionRegistry() {
        Map<String, ConditionFactory> parsers = new HashMap<>();
        parsers.put(InternalAlwaysCondition.TYPE, (c, id, p) -> InternalAlwaysCondition.parse(id, p));
        parsers.put(NeverCondition.TYPE, (c, id, p) -> NeverCondition.parse(id, p));
        parsers.put(ArrayCompareCondition.TYPE, (c, id, p) -> ArrayCompareCondition.parse(c, id, p));
        parsers.put(CompareCondition.TYPE, (c, id, p) -> CompareCondition.parse(c, id, p));
        parsers.put(ScriptCondition.TYPE, (c, id, p) -> ScriptCondition.parse(scriptService, id, p));
        return new ConditionRegistry(parsers, ClockMock.frozen());
    }

    private ExecutableTransform<?, ?> randomTransform() {
        String type = randomFrom(ScriptTransform.TYPE, SearchTransform.TYPE, ChainTransform.TYPE);
        TimeValue timeout = randomBoolean() ? timeValueSeconds(between(1, 10000)) : null;
        ZoneOffset timeZone = randomBoolean() ? ZoneOffset.UTC : null;
        return switch (type) {
            case ScriptTransform.TYPE -> new ExecutableScriptTransform(new ScriptTransform(mockScript("_script")), logger, scriptService);
            case SearchTransform.TYPE -> new ExecutableSearchTransform(
                new SearchTransform(templateRequest(searchSource()), timeout, timeZone),
                logger,
                client,
                searchTemplateService,
                TimeValue.timeValueMinutes(1)
            );
            default -> {
                // chain
                SearchTransform searchTransform = new SearchTransform(templateRequest(searchSource()), timeout, timeZone);
                ScriptTransform scriptTransform = new ScriptTransform(mockScript("_script"));

                ChainTransform chainTransform = new ChainTransform(Arrays.asList(searchTransform, scriptTransform));
                yield new ExecutableChainTransform(
                    chainTransform,
                    logger,
                    Arrays.asList(
                        new ExecutableSearchTransform(
                            new SearchTransform(templateRequest(searchSource()), timeout, timeZone),
                            logger,
                            client,
                            searchTemplateService,
                            TimeValue.timeValueMinutes(1)
                        ),
                        new ExecutableScriptTransform(new ScriptTransform(mockScript("_script")), logger, scriptService)
                    )
                );
            }
        };
    }

    private TransformRegistry transformRegistry() {
        return new TransformRegistry(
            Map.of(
                ScriptTransform.TYPE,
                new ScriptTransformFactory(scriptService),
                SearchTransform.TYPE,
                new SearchTransformFactory(settings, client, xContentRegistry(), scriptService)
            )
        );
    }

    private List<ActionWrapper> randomActions() {
        List<ActionWrapper> list = new ArrayList<>();
        if (randomBoolean()) {
            EmailAction action = new EmailAction(
                EmailTemplate.builder().build(),
                null,
                null,
                Profile.STANDARD,
                randomFrom(DataAttachment.JSON, DataAttachment.YAML),
                EmailAttachments.EMPTY_ATTACHMENTS
            );
            list.add(
                new ActionWrapper(
                    "_email_" + randomAlphaOfLength(8),
                    randomThrottler(),
                    AlwaysConditionTests.randomCondition(scriptService),
                    randomTransform(),
                    new ExecutableEmailAction(action, logger, emailService, templateEngine, htmlSanitizer, Collections.emptyMap()),
                    null,
                    null
                )
            );
        }
        if (randomBoolean()) {
            ZoneOffset timeZone = randomBoolean() ? ZoneOffset.UTC : null;
            TimeValue timeout = randomBoolean() ? timeValueSeconds(between(1, 10000)) : null;
            WriteRequest.RefreshPolicy refreshPolicy = randomBoolean() ? null : randomFrom(WriteRequest.RefreshPolicy.values());
            IndexAction action = new IndexAction(
                "_index",
                randomBoolean() ? "123" : null,
                randomBoolean() ? DocWriteRequest.OpType.fromId(randomFrom(new Byte[] { 0, 1 })) : null,
                null,
                timeout,
                timeZone,
                refreshPolicy
            );
            list.add(
                new ActionWrapper(
                    "_index_" + randomAlphaOfLength(8),
                    randomThrottler(),
                    AlwaysConditionTests.randomCondition(scriptService),
                    randomTransform(),
                    new ExecutableIndexAction(action, logger, client, TimeValue.timeValueSeconds(30), TimeValue.timeValueSeconds(30)),
                    null,
                    null
                )
            );
        }
        if (randomBoolean()) {
            HttpRequestTemplate httpRequest = HttpRequestTemplate.builder("test.host", randomIntBetween(8000, 9000))
                .method(randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT))
                .path(new TextTemplate("_url"))
                .build();
            WebhookAction action = new WebhookAction(httpRequest);
            list.add(
                new ActionWrapper(
                    "_webhook_" + randomAlphaOfLength(8),
                    randomThrottler(),
                    AlwaysConditionTests.randomCondition(scriptService),
                    randomTransform(),
                    new ExecutableWebhookAction(action, logger, httpClient, templateEngine),
                    null,
                    null
                )
            );
        }
        return list;
    }

    private ActionRegistry registry(List<ActionWrapper> actions, ConditionRegistry conditionRegistry, TransformRegistry transformRegistry) {
        Map<String, ActionFactory> parsers = new HashMap<>();
        for (ActionWrapper action : actions) {
            switch (action.action().type()) {
                case EmailAction.TYPE -> parsers.put(
                    EmailAction.TYPE,
                    new EmailActionFactory(settings, emailService, templateEngine, new EmailAttachmentsParser(Collections.emptyMap()))
                );
                case IndexAction.TYPE -> parsers.put(IndexAction.TYPE, new IndexActionFactory(settings, client));
                case WebhookAction.TYPE -> parsers.put(WebhookAction.TYPE, new WebhookActionFactory(httpClient, templateEngine));
                case LoggingAction.TYPE -> parsers.put(LoggingAction.TYPE, new LoggingActionFactory(new MockTextTemplateEngine()));
            }
        }
        return new ActionRegistry(unmodifiableMap(parsers), conditionRegistry, transformRegistry, Clock.systemUTC(), licenseState);
    }

    private ActionThrottler randomThrottler() {
        return new ActionThrottler(Clock.systemUTC(), randomBoolean() ? null : timeValueSeconds(randomIntBetween(1, 10000)), licenseState);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    (p, c) -> MatchAllQueryBuilder.fromXContent(p)
                ),
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(ScriptQueryBuilder.NAME),
                    (p, c) -> ScriptQueryBuilder.fromXContent(p)
                )
            )
        );
    }

    public static class ParseOnlyScheduleTriggerEngine extends ScheduleTriggerEngine {

        public ParseOnlyScheduleTriggerEngine(ScheduleRegistry registry, Clock clock) {
            super(registry, clock);
        }

        @Override
        public void start(Collection<Watch> jobs) {}

        @Override
        public void stop() {}

        @Override
        public void add(Watch watch) {}

        @Override
        public void pauseExecution() {}

        @Override
        public boolean remove(String jobId) {
            return false;
        }
    }
}
