/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform.chain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.transform.TransformFactory;
import org.elasticsearch.xpack.core.watcher.transform.TransformRegistry;
import org.elasticsearch.xpack.core.watcher.transform.chain.ChainTransform;
import org.elasticsearch.xpack.core.watcher.transform.chain.ChainTransformFactory;
import org.elasticsearch.xpack.core.watcher.transform.chain.ExecutableChainTransform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class ChainTransformTests extends ESTestCase {
    public void testExecute() throws Exception {
        ChainTransform transform = new ChainTransform(
            new NamedExecutableTransform.Transform("name1"),
            new NamedExecutableTransform.Transform("name2"),
            new NamedExecutableTransform.Transform("name3")
        );
        ExecutableChainTransform executable = new ExecutableChainTransform(
            transform,
            logger,
            new NamedExecutableTransform("name1"),
            new NamedExecutableTransform("name2"),
            new NamedExecutableTransform("name3")
        );

        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Payload payload = new Payload.Simple(new HashMap<>());

        ChainTransform.Result result = executable.execute(ctx, payload);
        assertThat(result.status(), is(Transform.Result.Status.SUCCESS));
        assertThat(result.results(), hasSize(3));
        assertThat(result.results().get(0), instanceOf(NamedExecutableTransform.Result.class));
        assertThat(result.results().get(0).status(), is(Transform.Result.Status.SUCCESS));
        assertThat(getNames(result.results().get(0).payload()), hasSize(1));
        assertThat(getNames(result.results().get(0).payload()), contains("name1"));
        assertThat(result.results().get(1), instanceOf(NamedExecutableTransform.Result.class));
        assertThat(result.results().get(1).status(), is(Transform.Result.Status.SUCCESS));
        assertThat(getNames(result.results().get(1).payload()), hasSize(2));
        assertThat(getNames(result.results().get(1).payload()), contains("name1", "name2"));
        assertThat(result.results().get(2), instanceOf(NamedExecutableTransform.Result.class));
        assertThat(result.results().get(2).status(), is(Transform.Result.Status.SUCCESS));
        assertThat(getNames(result.results().get(2).payload()), hasSize(3));
        assertThat(getNames(result.results().get(2).payload()), contains("name1", "name2", "name3"));

        Map<String, Object> data = result.payload().data();
        assertThat(data, notNullValue());
        assertThat(data, hasKey("names"));
        assertThat(data.get("names"), instanceOf(List.class));
        List<String> names = getNames(result.payload());
        assertThat(names, hasSize(3));
        assertThat(names, contains("name1", "name2", "name3"));
    }

    public void testExecuteFailure() throws Exception {
        ChainTransform transform = new ChainTransform(
            new NamedExecutableTransform.Transform("name1"),
            new NamedExecutableTransform.Transform("name2"),
            new FailingExecutableTransform.Transform()
        );
        ExecutableChainTransform executable = new ExecutableChainTransform(
            transform,
            logger,
            new NamedExecutableTransform("name1"),
            new NamedExecutableTransform("name2"),
            new FailingExecutableTransform(logger)
        );

        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Payload payload = new Payload.Simple(new HashMap<>());

        ChainTransform.Result result = executable.execute(ctx, payload);
        assertThat(result.status(), is(Transform.Result.Status.FAILURE));
        assertThat(result.reason(), notNullValue());
        assertThat(result.results(), hasSize(3));
        assertThat(result.results().get(0), instanceOf(NamedExecutableTransform.Result.class));
        assertThat(result.results().get(0).status(), is(Transform.Result.Status.SUCCESS));
        assertThat(getNames(result.results().get(0).payload()), hasSize(1));
        assertThat(getNames(result.results().get(0).payload()), contains("name1"));
        assertThat(result.results().get(1), instanceOf(NamedExecutableTransform.Result.class));
        assertThat(result.results().get(1).status(), is(Transform.Result.Status.SUCCESS));
        assertThat(getNames(result.results().get(1).payload()), hasSize(2));
        assertThat(getNames(result.results().get(1).payload()), contains("name1", "name2"));
        assertThat(result.results().get(2), instanceOf(FailingExecutableTransform.Result.class));
        assertThat(result.results().get(2).status(), is(Transform.Result.Status.FAILURE));
        assertThat(result.results().get(2).reason(), containsString("_error"));

    }

    public void testParser() throws Exception {
        TransformRegistry registry = new TransformRegistry(singletonMap("named", new NamedExecutableTransform.Factory(logger)));

        ChainTransformFactory transformParser = new ChainTransformFactory(registry);

        XContentBuilder builder = jsonBuilder().startArray()
            .startObject()
            .startObject("named")
            .field("name", "name1")
            .endObject()
            .endObject()
            .startObject()
            .startObject("named")
            .field("name", "name2")
            .endObject()
            .endObject()
            .startObject()
            .startObject("named")
            .field("name", "name3")
            .endObject()
            .endObject()
            .startObject()
            .field("named", "name4")
            .endObject()
            .endArray();

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken();
        ExecutableChainTransform executable = transformParser.parseExecutable("_id", parser);
        assertThat(executable, notNullValue());
        assertThat(executable.transform().getTransforms(), notNullValue());
        assertThat(executable.transform().getTransforms(), hasSize(4));
        for (int i = 0; i < executable.transform().getTransforms().size(); i++) {
            assertThat(executable.executableTransforms().get(i), instanceOf(NamedExecutableTransform.class));
            assertThat(((NamedExecutableTransform) executable.executableTransforms().get(i)).transform().name, is("name" + (i + 1)));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> getNames(Payload payload) {
        return (List<String>) payload.data().get("names");
    }

    private static class NamedExecutableTransform extends ExecutableTransform<
        NamedExecutableTransform.Transform,
        NamedExecutableTransform.Result> {
        private static final String TYPE = "named";

        NamedExecutableTransform(String name) {
            this(new Transform(name));
        }

        NamedExecutableTransform(Transform transform) {
            super(transform, LogManager.getLogger(NamedExecutableTransform.class));
        }

        @Override
        public Result execute(WatchExecutionContext ctx, Payload payload) {
            List<String> names = getNames(payload);
            if (names == null) {
                names = new ArrayList<>();
            } else {
                names = new ArrayList<>(names);
            }
            names.add(transform.name);
            Map<String, Object> data = new HashMap<>();
            data.put("names", names);
            return new Result("named", new Payload.Simple(data));
        }

        public static class Transform implements org.elasticsearch.xpack.core.watcher.transform.Transform {

            private final String name;

            Transform(String name) {
                this.name = name;
            }

            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().field("name", name).endObject();
            }
        }

        public static class Result extends Transform.Result {

            Result(String type, Payload payload) {
                super(type, payload);
            }

            @Override
            protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
                return builder;
            }
        }

        public static class Factory extends TransformFactory<Transform, Result, NamedExecutableTransform> {
            Factory(Logger transformLogger) {
                super(transformLogger);
            }

            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public Transform parseTransform(String watchId, XContentParser parser) throws IOException {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return new Transform(parser.text());
                }
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                XContentParser.Token token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME; // the "name" field
                token = parser.nextToken();
                assert token == XContentParser.Token.VALUE_STRING;
                String name = parser.text();
                token = parser.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return new Transform(name);
            }

            @Override
            public NamedExecutableTransform createExecutable(Transform transform) {
                return new NamedExecutableTransform(transform);
            }
        }
    }

    private static class FailingExecutableTransform extends ExecutableTransform<
        FailingExecutableTransform.Transform,
        FailingExecutableTransform.Result> {
        private static final String TYPE = "throwing";

        FailingExecutableTransform(Logger logger) {
            super(new Transform(), logger);
        }

        @Override
        public Result execute(WatchExecutionContext ctx, Payload payload) {
            return new Result(TYPE);
        }

        public static class Transform implements org.elasticsearch.xpack.core.watcher.transform.Transform {
            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().endObject();
            }
        }

        public static class Result extends Transform.Result {
            Result(String type) {
                super(type, new Exception("_error"));
            }

            @Override
            protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
                return builder;
            }
        }

        public static class Factory extends TransformFactory<Transform, Result, FailingExecutableTransform> {
            Factory(Logger transformLogger) {
                super(transformLogger);
            }

            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public Transform parseTransform(String watchId, XContentParser parser) throws IOException {
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                XContentParser.Token token = parser.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return new Transform();
            }

            @Override
            public FailingExecutableTransform createExecutable(Transform transform) {
                return new FailingExecutableTransform(transformLogger);
            }
        }
    }
}
