/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.transform;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.test.WatcherMockScriptPlugin;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.transform.script.ExecutableScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransform;
import org.elasticsearch.xpack.watcher.transform.script.ScriptTransformFactory;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class TransformInputTests extends ESTestCase {

    private ScriptService scriptService;

    @Before
    public void setupScriptService() {
        scriptService = WatcherMockScriptPlugin.newMockScriptService(Collections.singletonMap("1", s -> "2"));
    }

    public void testExecute() {
        Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "1", Collections.emptyMap(), Collections.emptyMap());
        ScriptTransform scriptTransform = ScriptTransform.builder(script).build();
        TransformInput transformInput = new TransformInput(scriptTransform);

        ExecutableTransform<?, ?> executableTransform = new ExecutableScriptTransform(scriptTransform, logger, scriptService);
        ExecutableInput<?, ?> input = new ExecutableTransformInput(transformInput, executableTransform);

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", Payload.EMPTY);
        Input.Result result = input.execute(ctx, new Payload.Simple());
        assertThat(result.payload().data().size(), is(1));
        assertThat(result.payload().data(), hasEntry("_value", "2"));
    }

    public void testParserValid() throws Exception {
        Map<String,
            TransformFactory<? extends Transform, ? extends Transform.Result, ? extends ExecutableTransform<?, ?>>> transformFactories =
            Collections.singletonMap("script", new ScriptTransformFactory(scriptService));
        TransformRegistry registry = new TransformRegistry(transformFactories);
        TransformInputFactory factory = new TransformInputFactory(registry);

        // { "script" : { "lang" : "mockscript", "source" : "1" } }
        XContentBuilder builder = jsonBuilder().startObject().startObject("script")
                .field("lang", MockScriptEngine.NAME)
                .field("source", "1")
                .endObject().endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken();
        ExecutableTransformInput executableTransformInput = factory.parseExecutable("_id", parser);

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", Payload.EMPTY);
        TransformInput.Result result = executableTransformInput.execute(ctx, Payload.EMPTY);
        assertThat(result.payload().data().size(), is(1));
        assertThat(result.payload().data(), hasEntry("_value", "2"));
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder jsonBuilder = jsonBuilder().value("just a string");

        Map<String,
            TransformFactory<? extends Transform, ? extends Transform.Result, ? extends ExecutableTransform<?, ?>>> transformFactories =
            Collections.singletonMap("script", new ScriptTransformFactory(scriptService));
        TransformRegistry registry = new TransformRegistry(transformFactories);
        TransformInputFactory factory = new TransformInputFactory(registry);
        XContentParser parser = createParser(jsonBuilder);

        parser.nextToken();
        expectThrows(ParsingException.class, () -> factory.parseInput("_id", parser));
    }

    public void testTransformResultToXContent() throws Exception {
        Map<String, Object> data = Collections.singletonMap("foo", "bar");
        TransformInput.Result result = new TransformInput.Result(new Payload.Simple(data));
        try (XContentBuilder builder = jsonBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }

    public void testTransformInputToXContentIsSameAsParsing() throws Exception {
        Map<String,
            TransformFactory<? extends Transform, ? extends Transform.Result, ? extends ExecutableTransform<?, ?>>> transformFactories =
            Collections.singletonMap("script", new ScriptTransformFactory(scriptService));
        TransformRegistry registry = new TransformRegistry(transformFactories);
        TransformInputFactory factory = new TransformInputFactory(registry);

        XContentBuilder jsonBuilder = jsonBuilder().startObject().startObject("script")
                .field("source", "1")
                .field("lang", "mockscript")
                .endObject().endObject();
        XContentParser parser = createParser(jsonBuilder);

        parser.nextToken();
        TransformInput transformInput = factory.parseInput("whatever", parser);

        XContentBuilder output = jsonBuilder();
        transformInput.toXContent(output, ToXContent.EMPTY_PARAMS);

        assertThat(Strings.toString(jsonBuilder), is(Strings.toString(output)));
    }
}
