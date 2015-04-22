/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.transform.chain.ChainTransform;
import org.elasticsearch.watcher.transform.chain.ChainTransformFactory;
import org.elasticsearch.watcher.transform.chain.ExecutableChainTransform;
import org.elasticsearch.watcher.watch.Payload;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

/**
 *
 */
public class ChainTransformTests extends ElasticsearchTestCase {

    @Test
    public void testApply() throws Exception {
        ChainTransform transform = new ChainTransform(ImmutableList.<Transform>of(
                new NamedExecutableTransform.Transform("name1"),
                new NamedExecutableTransform.Transform("name2"),
                new NamedExecutableTransform.Transform("name3")
        ));
        ExecutableChainTransform executable = new ExecutableChainTransform(transform, logger, ImmutableList.<ExecutableTransform>of(
                new NamedExecutableTransform("name1"),
                new NamedExecutableTransform("name2"),
                new NamedExecutableTransform("name3")));

        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Payload payload = new Payload.Simple(new HashMap<String, Object>());

        Transform.Result result = executable.execute(ctx, payload);

        Map<String, Object> data = result.payload().data();
        assertThat(data, notNullValue());
        assertThat(data, hasKey("names"));
        assertThat(data.get("names"), instanceOf(List.class));
        List<String> names = (List<String>) data.get("names");
        assertThat(names, hasSize(3));
        assertThat(names, contains("name1", "name2", "name3"));
    }

    @Test
    public void testParser() throws Exception {
        Map<String, TransformFactory> factories = ImmutableMap.<String, TransformFactory>builder()
                .put("named", new NamedExecutableTransform.Factory(logger))
                .build();
        TransformRegistry registry = new TransformRegistry(factories);

        ChainTransformFactory transformParser = new ChainTransformFactory(registry);

        XContentBuilder builder = jsonBuilder().startArray()
                .startObject().startObject("named").field("name", "name1").endObject().endObject()
                .startObject().startObject("named").field("name", "name2").endObject().endObject()
                .startObject().startObject("named").field("name", "name3").endObject().endObject()
                .endArray();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ExecutableChainTransform executable = transformParser.parseExecutable("_id", parser);
        assertThat(executable, notNullValue());
        assertThat(executable.transform.getTransforms(), notNullValue());
        assertThat(executable.transform.getTransforms(), hasSize(3));
        for (int i = 0; i < executable.transform.getTransforms().size(); i++) {
            assertThat(executable.executableTransforms().get(i), instanceOf(NamedExecutableTransform.class));
            assertThat(((NamedExecutableTransform) executable.executableTransforms().get(i)).transform().name, is("name" + (i + 1)));
        }
    }

    private static class NamedExecutableTransform extends ExecutableTransform<NamedExecutableTransform.Transform, NamedExecutableTransform.Result> {

        private static final String TYPE = "named";

        public NamedExecutableTransform(String name) {
            this(new Transform(name));
        }

        public NamedExecutableTransform(Transform transform) {
            super(transform, Loggers.getLogger(NamedExecutableTransform.class));
        }

        @Override
        public Result execute(WatchExecutionContext ctx, Payload payload) throws IOException {
            Map<String, Object> data = new HashMap<>(payload.data());
            List<String> names = (List<String>) data.get("names");
            if (names == null) {
                names = new ArrayList<>();
                data.put("names", names);
            }
            names.add(transform.name);
            return new Result("named", new Payload.Simple(data));
        }

        public static class Transform implements org.elasticsearch.watcher.transform.Transform {

            private final String name;

            public Transform(String name) {
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

            public Result(String type, Payload payload) {
                super(type, payload);
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder;
            }
        }

        public static class Factory extends TransformFactory<Transform, Result, NamedExecutableTransform> {

            public Factory(ESLogger transformLogger) {
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
                assert token == XContentParser.Token.FIELD_NAME; // the "name" field
                token = parser.nextToken();
                assert token == XContentParser.Token.VALUE_STRING;
                String name = parser.text();
                token = parser.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return new Transform(name);
            }

            @Override
            public Result parseResult(String watchId, XContentParser parser) throws IOException {
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                XContentParser.Token token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME; // the "payload" field
                token = parser.nextToken();
                assert token == XContentParser.Token.START_OBJECT;
                Payload payload = new Payload.XContent(parser);
                token = parser.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return new Result("named", payload);
            }

            @Override
            public NamedExecutableTransform createExecutable(Transform transform) {
                return new NamedExecutableTransform(transform);
            }
        }

    }
}
