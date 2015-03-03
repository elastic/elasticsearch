/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.actions.email.EmailAction;
import org.elasticsearch.alerts.actions.email.service.Email;
import org.elasticsearch.alerts.actions.email.service.EmailService;
import org.elasticsearch.alerts.actions.email.service.Profile;
import org.elasticsearch.alerts.actions.index.IndexAction;
import org.elasticsearch.alerts.actions.webhook.HttpClient;
import org.elasticsearch.alerts.actions.webhook.WebhookAction;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionRegistry;
import org.elasticsearch.alerts.condition.script.ScriptCondition;
import org.elasticsearch.alerts.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputRegistry;
import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.alerts.input.simple.SimpleInput;
import org.elasticsearch.alerts.scheduler.schedule.*;
import org.elasticsearch.alerts.scheduler.schedule.support.*;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.support.template.ScriptTemplate;
import org.elasticsearch.alerts.support.template.Template;
import org.elasticsearch.alerts.test.AlertsTestUtils;
import org.elasticsearch.alerts.transform.*;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.alerts.test.AlertsTestUtils.matchAllRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class AlertTests extends ElasticsearchTestCase {

    private ScriptServiceProxy scriptService;
    private ClientProxy client;
    private HttpClient httpClient;
    private EmailService emailService;
    private Template.Parser templateParser;
    private ESLogger logger;
    private Settings settings = ImmutableSettings.EMPTY;

    @Before
    public void init() throws Exception {
        scriptService = mock(ScriptServiceProxy.class);
        client = mock(ClientProxy.class);
        httpClient = mock(HttpClient.class);
        emailService = mock(EmailService.class);
        templateParser = new ScriptTemplate.Parser(settings, scriptService);
        logger = Loggers.getLogger(AlertTests.class);
    }

    @Test @Repeat(iterations = 20)
    public void testParser_SelfGenerated() throws Exception {

        TransformRegistry transformRegistry = transformRegistry();

        Schedule schedule = randomSchedule();
        ScheduleRegistry scheduleRegistry = registry(schedule);

        Input input = randomInput();
        InputRegistry inputRegistry = registry(input);

        Condition condition = randomCondition();
        ConditionRegistry conditionRegistry = registry(condition);

        Transform transform = randomTransform();

        Actions actions = randomActions();
        ActionRegistry actionRegistry = registry(actions, transformRegistry);

        Map<String, Object> metadata = ImmutableMap.<String, Object>of("_key", "_val");

        Alert.Status status = new Alert.Status();

        TimeValue throttlePeriod = randomBoolean() ? null : TimeValue.timeValueSeconds(randomIntBetween(5, 10));

        Alert alert = new Alert("_name", schedule, input, condition, transform, actions, metadata, throttlePeriod, status);

        BytesReference bytes = XContentFactory.jsonBuilder().value(alert).bytes();
        logger.info(bytes.toUtf8());
        Alert.Parser alertParser = new Alert.Parser(settings, conditionRegistry, scheduleRegistry, transformRegistry, actionRegistry, inputRegistry);

        boolean includeStatus = randomBoolean();
        Alert parsedAlert = alertParser.parse("_name", includeStatus, bytes);

        if (includeStatus) {
            assertThat(parsedAlert.status(), equalTo(status));
        }
        assertThat(parsedAlert.schedule(), equalTo(schedule));
        assertThat(parsedAlert.input(), equalTo(input));
        assertThat(parsedAlert.condition(), equalTo(condition));
        if (throttlePeriod != null) {
            assertThat(parsedAlert.throttlePeriod().millis(), equalTo(throttlePeriod.millis()));
        }
        assertThat(parsedAlert.metadata(), equalTo(metadata));
        assertThat(parsedAlert.actions(), equalTo(actions));
    }

    private static Schedule randomSchedule() {
        String type = randomFrom(CronSchedule.TYPE, HourlySchedule.TYPE, DailySchedule.TYPE, WeeklySchedule.TYPE, MonthlySchedule.TYPE, YearlySchedule.TYPE, IntervalSchedule.TYPE);
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
        ImmutableMap.Builder<String, Schedule.Parser> parsers = ImmutableMap.builder();
        switch (schedule.type()) {
            case CronSchedule.TYPE:
                parsers.put(CronSchedule.TYPE, new CronSchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            case HourlySchedule.TYPE:
                parsers.put(HourlySchedule.TYPE, new HourlySchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            case DailySchedule.TYPE:
                parsers.put(DailySchedule.TYPE, new DailySchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            case WeeklySchedule.TYPE:
                parsers.put(WeeklySchedule.TYPE, new WeeklySchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            case MonthlySchedule.TYPE:
                parsers.put(MonthlySchedule.TYPE, new MonthlySchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            case YearlySchedule.TYPE:
                parsers.put(YearlySchedule.TYPE, new YearlySchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            case IntervalSchedule.TYPE:
                parsers.put(IntervalSchedule.TYPE, new IntervalSchedule.Parser());
                return new ScheduleRegistry(parsers.build());
            default:
                throw new IllegalArgumentException("unknown schedule [" + schedule + "]");
        }
    }

    private Input randomInput() {
        String type = randomFrom(SearchInput.TYPE, SimpleInput.TYPE);
        switch (type) {
            case SearchInput.TYPE:
                return new SearchInput(logger, scriptService, client, AlertsTestUtils.newInputSearchRequest("idx"));
            default:
                return new SimpleInput(logger, new Payload.Simple(ImmutableMap.<String, Object>builder().put("_key", "_val").build()));
        }
    }

    private InputRegistry registry(Input input) {
        ImmutableMap.Builder<String, Input.Parser> parsers = ImmutableMap.builder();
        switch (input.type()) {
            case SearchInput.TYPE:
                parsers.put(SearchInput.TYPE, new SearchInput.Parser(settings, scriptService, client));
                return new InputRegistry(parsers.build());
            default:
                parsers.put(SimpleInput.TYPE, new SimpleInput.Parser(settings));
                return new InputRegistry(parsers.build());
        }
    }

    private Condition randomCondition() {
        String type = randomFrom(ScriptCondition.TYPE, AlwaysTrueCondition.TYPE);
        switch (type) {
            case ScriptCondition.TYPE:
                return new ScriptCondition(logger, scriptService, new Script("_script"));
            default:
                return new AlwaysTrueCondition(logger);
        }
    }

    private ConditionRegistry registry(Condition condition) {
        ImmutableMap.Builder<String, Condition.Parser> parsers = ImmutableMap.builder();
        switch (condition.type()) {
            case ScriptCondition.TYPE:
                parsers.put(ScriptCondition.TYPE, new ScriptCondition.Parser(settings, scriptService));
                return new ConditionRegistry(parsers.build());
            default:
                parsers.put(AlwaysTrueCondition.TYPE, new AlwaysTrueCondition.Parser(settings));
                return new ConditionRegistry(parsers.build());
        }
    }

    private Transform randomTransform() {
        String type = randomFrom(ScriptTransform.TYPE, SearchTransform.TYPE, ChainTransform.TYPE);
        switch (type) {
            case ScriptTransform.TYPE:
                return new ScriptTransform(scriptService, new Script("_script"));
            case SearchTransform.TYPE:
                return new SearchTransform(logger, scriptService, client, matchAllRequest(AlertUtils.DEFAULT_INDICES_OPTIONS));
            default: // chain
                return new ChainTransform(ImmutableList.of(
                        new SearchTransform(logger, scriptService, client, matchAllRequest(AlertUtils.DEFAULT_INDICES_OPTIONS)),
                        new ScriptTransform(scriptService, new Script("_script"))));
        }
    }

    private TransformRegistry transformRegistry() {
        ImmutableMap.Builder<String, Transform.Parser> parsers = ImmutableMap.builder();
        ChainTransform.Parser parser = new ChainTransform.Parser();
        parsers.put(ChainTransform.TYPE, parser);
        parsers.put(ScriptTransform.TYPE, new ScriptTransform.Parser(scriptService));
        parsers.put(SearchTransform.TYPE, new SearchTransform.Parser(settings, scriptService, client));
        TransformRegistry registry = new TransformRegistry(parsers.build());
        parser.init(registry);
        return registry;
    }

    private Actions randomActions() {
        ImmutableList.Builder<Action> list = ImmutableList.builder();
        if (randomBoolean()) {
            Transform transform = randomTransform();
            list.add(new EmailAction(logger, transform, emailService, Email.builder().id("prototype").build(), null, Profile.STANDARD, null, null, null, null, randomBoolean()));
        }
        if (randomBoolean()) {
            list.add(new IndexAction(logger, randomTransform(), client, "_index", "_type"));
        }
        if (randomBoolean()) {
            list.add(new WebhookAction(logger, randomTransform(), httpClient, randomFrom(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT), new ScriptTemplate(scriptService, "_url"), null));
        }
        return new Actions(list.build());
    }

    private ActionRegistry registry(Actions actions, TransformRegistry transformRegistry) {
        ImmutableMap.Builder<String, Action.Parser> parsers = ImmutableMap.builder();
        for (Action action : actions) {
            switch (action.type()) {
                case EmailAction.TYPE:
                    parsers.put(EmailAction.TYPE, new EmailAction.Parser(settings, emailService, templateParser, transformRegistry));
                    break;
                case IndexAction.TYPE:
                    parsers.put(IndexAction.TYPE, new IndexAction.Parser(settings, client, transformRegistry));
                    break;
                case WebhookAction.TYPE:
                    parsers.put(WebhookAction.TYPE, new WebhookAction.Parser(settings, templateParser, httpClient, transformRegistry));
                    break;
            }
        }
        return new ActionRegistry(parsers.build());
    }

}
