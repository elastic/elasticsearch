/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.chain;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.input.InputFactory;
import org.elasticsearch.xpack.watcher.input.InputRegistry;
import org.elasticsearch.xpack.watcher.input.http.HttpInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInputFactory;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.chainInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

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
        Map<String, InputFactory<?, ?, ?>> factories = new HashMap<>();
        factories.put("simple", new SimpleInputFactory());

        // hackedy hack...
        InputRegistry inputRegistry = new InputRegistry(factories);
        ChainInputFactory chainInputFactory = new ChainInputFactory(inputRegistry);
        factories.put("chain", chainInputFactory);

        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("inputs")
            .startObject()
            .startObject("first")
            .startObject("simple")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject("second")
            .startObject("simple")
            .field("spam", "eggs")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        // first pass JSON and check for correct inputs
        XContentParser parser = createParser(builder);
        parser.nextToken();
        ChainInput chainInput = chainInputFactory.parseInput("test", parser);

        assertThat(chainInput.getInputs(), hasSize(2));
        assertThat(chainInput.getInputs().get(0).v1(), is("first"));
        assertThat(chainInput.getInputs().get(0).v2(), instanceOf(SimpleInput.class));
        assertThat(chainInput.getInputs().get(1).v1(), is("second"));
        assertThat(chainInput.getInputs().get(1).v2(), instanceOf(SimpleInput.class));

        // now execute
        ExecutableChainInput executableChainInput = chainInputFactory.createExecutable(chainInput);
        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        ChainInput.Result result = executableChainInput.execute(ctx, new Payload.Simple());
        Payload payload = result.payload();
        assertThat(payload.data(), hasKey("first"));
        assertThat(payload.data(), hasKey("second"));
        assertThat(payload.data().get("first"), instanceOf(Map.class));
        assertThat(payload.data().get("second"), instanceOf(Map.class));

        // final payload check
        @SuppressWarnings("unchecked")
        Map<String, Object> firstPayload = (Map<String, Object>) payload.data().get("first");
        @SuppressWarnings("unchecked")
        Map<String, Object> secondPayload = (Map<String, Object>) payload.data().get("second");
        assertThat(firstPayload, hasEntry("foo", "bar"));
        assertThat(secondPayload, hasEntry("spam", "eggs"));
    }

    public void testToXContent() throws Exception {
        ChainInput chainedInput = chainInput().add("first", simpleInput("foo", "bar")).add("second", simpleInput("spam", "eggs")).build();

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        chainedInput.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(BytesReference.bytes(builder).utf8ToString(), is("""
            {"inputs":[{"first":{"simple":{"foo":"bar"}}},{"second":{"simple":{"spam":"eggs"}}}]}"""));

        // parsing it back as well!
        Map<String, InputFactory<?, ?, ?>> factories = new HashMap<>();
        factories.put("simple", new SimpleInputFactory());

        InputRegistry inputRegistry = new InputRegistry(factories);
        ChainInputFactory chainInputFactory = new ChainInputFactory(inputRegistry);
        factories.put("chain", chainInputFactory);

        XContentParser parser = createParser(builder);
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

        HttpInput.Builder httpInputBuilder = httpInput(
            HttpRequestTemplate.builder("theHost", 1234)
                .path("/index/_search")
                .body(Strings.toString(jsonBuilder().startObject().field("size", 1).endObject()))
                .auth(new BasicAuth("test", SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
        );

        ChainInput.Builder chainedInputBuilder = chainInput().add("foo", httpInputBuilder).add("bar", simpleInput("spam", "eggs"));

        watchBuilder().trigger(schedule(interval("5s")))
            .input(chainedInputBuilder)
            .condition(new ScriptCondition(mockScript("ctx.payload.hits.total.value == 1")))
            .addAction("_id", loggingAction("watch [{{ctx.watch_id}}] matched"))
            .toXContent(builder, ToXContent.EMPTY_PARAMS);

        // no exception means all good
    }

    public void testThatSerializationOfFailedInputWorks() throws Exception {
        ChainInput.Result chainedResult = new ChainInput.Result(new ElasticsearchException("foo"));

        XContentBuilder builder = jsonBuilder();
        chainedResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(BytesReference.bytes(builder).utf8ToString(), containsString("\"type\":\"exception\""));
        assertThat(BytesReference.bytes(builder).utf8ToString(), containsString("\"reason\":\"foo\""));
    }

    /* https://github.com/elastic/x-plugins/issues/3736
       the issue here is, that first/second in this setup do not have a guaranteed order, so we have to throw an exception
    {
      "inputs" : [
        {
          "first" : { "simple" : { "foo" : "bar" } },
          "second" : { "simple" : { "spam" : "eggs" } }
        }
      ]
    }
     */
    public void testParsingShouldBeStrictWhenClosingInputs() throws Exception {
        Map<String, InputFactory<?, ?, ?>> factories = new HashMap<>();
        factories.put("simple", new SimpleInputFactory());

        InputRegistry inputRegistry = new InputRegistry(factories);
        ChainInputFactory chainInputFactory = new ChainInputFactory(inputRegistry);
        factories.put("chain", chainInputFactory);

        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("inputs")
            .startObject()
            .startObject("first")
            .startObject("simple")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .startObject("second")
            .startObject("simple")
            .field("spam", "eggs")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> chainInputFactory.parseInput("test", parser));
        assertThat(
            e.getMessage(),
            containsString("Expected closing JSON object after parsing input [simple] named [first] in watch [test]")
        );
    }

    /* https://github.com/elastic/x-plugins/issues/3736
       make sure that after the name of a chained input there is always an object
    {
      "inputs" : [
        { "first" : [ { "simple" : { "foo" : "bar" } } ] }
      ]
    }
     */
    public void testParsingShouldBeStrictWhenStartingInputs() throws Exception {
        Map<String, InputFactory<?, ?, ?>> factories = new HashMap<>();
        factories.put("simple", new SimpleInputFactory());

        InputRegistry inputRegistry = new InputRegistry(factories);
        ChainInputFactory chainInputFactory = new ChainInputFactory(inputRegistry);
        factories.put("chain", chainInputFactory);

        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("inputs")
            .startObject()
            .startArray("first")
            .startObject()
            .startObject("simple")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endArray()
            .endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> chainInputFactory.parseInput("test", parser));
        assertThat(e.getMessage(), containsString("Expected starting JSON object after [first] in watch [test]"));
    }

    public void testThatXContentParametersArePassedToInputs() throws Exception {
        ToXContent.Params randomParams = new ToXContent.MapParams(Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        ChainInput chainInput = new ChainInput(Collections.singletonList(Tuple.tuple("whatever", new Input() {
            @Override
            public String type() {
                return "test";
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                assertThat(params, sameInstance(randomParams));
                return builder;
            }
        })));

        try (XContentBuilder builder = jsonBuilder()) {
            chainInput.toXContent(builder, randomParams);
        }
    }
}
