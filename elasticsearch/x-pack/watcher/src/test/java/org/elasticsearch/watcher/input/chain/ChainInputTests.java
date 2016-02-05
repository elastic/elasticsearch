/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.chain;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.input.InputFactory;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.input.http.HttpInput;
import org.elasticsearch.watcher.input.simple.ExecutableSimpleInput;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.input.simple.SimpleInputFactory;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStatus;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.chainInput;
import static org.elasticsearch.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.joda.time.DateTimeZone.UTC;

public class ChainInputTests extends ESTestCase {

    /* note, first line does not need to be parsed
    "chain" : {
      "inputs" : [
        { "first" : { "simple" : { "foo" : "bar" } } },
        { "second" : { "simple" : { "spam" : "eggs" } } }
      ]
    }
     */
    public void testThatExecutionWorks() throws Exception {
        Map<String, InputFactory> factories = new HashMap<>();
        factories.put("simple", new SimpleInputFactory(Settings.EMPTY));

        // hackedy hack...
        InputRegistry inputRegistry = new InputRegistry(factories);
        ChainInputFactory chainInputFactory = new ChainInputFactory(Settings.EMPTY);
        chainInputFactory.init(inputRegistry);
        factories.put("chain", chainInputFactory);

        XContentBuilder builder = jsonBuilder().startObject().startArray("inputs")
                .startObject().startObject("first").startObject("simple").field("foo", "bar").endObject().endObject().endObject()
                .startObject().startObject("second").startObject("simple").field("spam", "eggs").endObject().endObject().endObject()
                .endArray().endObject();

        // first pass JSON and check for correct inputs
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ChainInput chainInput = chainInputFactory.parseInput("test", parser);

        assertThat(chainInput.getInputs(), hasSize(2));
        assertThat(chainInput.getInputs().get(0).v1(), is("first"));
        assertThat(chainInput.getInputs().get(0).v2(), instanceOf(SimpleInput.class));
        assertThat(chainInput.getInputs().get(1).v1(), is("second"));
        assertThat(chainInput.getInputs().get(1).v2(), instanceOf(SimpleInput.class));

        // now execute
        ExecutableChainInput executableChainInput = chainInputFactory.createExecutable(chainInput);
        ChainInput.Result result = executableChainInput.execute(createContext(), new Payload.Simple());
        Payload payload = result.payload();
        assertThat(payload.data(), hasKey("first"));
        assertThat(payload.data(), hasKey("second"));
        assertThat(payload.data().get("first"), instanceOf(Map.class));
        assertThat(payload.data().get("second"), instanceOf(Map.class));

        // final payload check
        Map<String, Object> firstPayload = (Map<String,Object>) payload.data().get("first");
        Map<String, Object> secondPayload = (Map<String,Object>) payload.data().get("second");
        assertThat(firstPayload, hasEntry("foo", "bar"));
        assertThat(secondPayload, hasEntry("spam", "eggs"));
    }

    public void testToXContent() throws Exception {
        ChainInput chainedInput = chainInput()
                .add("first", simpleInput("foo", "bar"))
                .add("second", simpleInput("spam", "eggs"))
                .build();

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        chainedInput.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(builder.bytes().toUtf8(),
                is("{\"inputs\":[{\"first\":{\"simple\":{\"foo\":\"bar\"}}},{\"second\":{\"simple\":{\"spam\":\"eggs\"}}}]}"));

        // parsing it back as well!
        Map<String, InputFactory> factories = new HashMap<>();
        factories.put("simple", new SimpleInputFactory(Settings.EMPTY));

        InputRegistry inputRegistry = new InputRegistry(factories);
        ChainInputFactory chainInputFactory = new ChainInputFactory(Settings.EMPTY);
        chainInputFactory.init(inputRegistry);
        factories.put("chain", chainInputFactory);

        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        ChainInput parsedChainInput = ChainInput.parse("testWatchId", parser, inputRegistry);
        assertThat(parsedChainInput.getInputs(), hasSize(2));
        assertThat(parsedChainInput.getInputs().get(0).v1(), is("first"));
        assertThat(parsedChainInput.getInputs().get(0).v2(), is(instanceOf(SimpleInput.class)));
        assertThat(parsedChainInput.getInputs().get(1).v1(), is("second"));
        assertThat(parsedChainInput.getInputs().get(1).v2(), is(instanceOf(SimpleInput.class)));
    }

    public void testThatWatchSourceBuilderWorksWithChainInput() throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);

        HttpInput.Builder httpInputBuilder = httpInput(HttpRequestTemplate.builder("theHost", 1234)
                .path("/index/_search")
                .body(jsonBuilder().startObject().field("size", 1).endObject())
                .auth(new BasicAuth("test", "changeme".toCharArray())));

        ChainInput.Builder chainedInputBuilder = chainInput()
                .add("foo", httpInputBuilder)
                .add("bar", simpleInput("spam", "eggs"));

        watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(chainedInputBuilder)
                .condition(scriptCondition("ctx.payload.hits.total == 1"))
                .addAction("_id", loggingAction("watch [{{ctx.watch_id}}] matched"))
                .toXContent(builder, ToXContent.EMPTY_PARAMS);

        // no exception means all good
    }

    private WatchExecutionContext createContext() {
        Watch watch = new Watch("test-watch",
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(1, IntervalSchedule.Interval.Unit.MINUTES))),
                new ExecutableSimpleInput(new SimpleInput(new Payload.Simple()), logger),
                new ExecutableAlwaysCondition(logger),
                null,
                null,
                new ExecutableActions(new ArrayList<ActionWrapper>()),
                null,
                new WatchStatus(new DateTime(0, UTC), emptyMap()));
        WatchExecutionContext ctx = new TriggeredExecutionContext(watch,
                new DateTime(0, UTC),
                new ScheduleTriggerEvent(watch.id(), new DateTime(0, UTC), new DateTime(0, UTC)),
                TimeValue.timeValueSeconds(5));

        return ctx;
    }

}
