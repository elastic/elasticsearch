/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformFactory;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 */
public class TransformMocks {

    public static class ExecutableTransformMock extends ExecutableTransform {

        private static final String TYPE = "mock";

        public ExecutableTransformMock() {
            super(new Transform() {
                @Override
                public String type() {
                    return TYPE;
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return builder.startObject().endObject();
                }
            }, Loggers.getLogger(ExecutableTransformMock.class));
        }

        @Override
        public Transform.Result execute(WatchExecutionContext ctx, Payload payload) throws IOException {
            return new Result(TYPE, new Payload.Simple("_key", "_value"));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
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
    }

    public static class TransformRegistryMock extends TransformRegistry {

        public TransformRegistryMock(final ExecutableTransform executable) {
            super(ImmutableMap.<String, TransformFactory>of("_transform", new TransformFactory(Loggers.getLogger(TransformRegistryMock.class)) {
                @Override
                public String type() {
                    return executable.type();
                }

                @Override
                public Transform parseTransform(String watchId, XContentParser parser) throws IOException {
                    parser.nextToken();
                    assertThat(parser.currentToken(), is(XContentParser.Token.END_OBJECT));
                    return null;
                }

                @Override
                public Transform.Result parseResult(String watchId, XContentParser parser) throws IOException {
                    return null;
                }

                @Override
                public ExecutableTransform createExecutable(Transform transform) {
                    return executable;
                }
            }));
        }

        public TransformRegistryMock(final Transform.Result result) {
            super(ImmutableMap.<String, TransformFactory>of("_transform_type", new TransformFactory(Loggers.getLogger(TransformRegistryMock.class)) {
                @Override
                public String type() {
                    return result.type();
                }

                @Override
                public Transform parseTransform(String watchId, XContentParser parser) throws IOException {
                    return null;
                }

                @Override
                public ExecutableTransform createExecutable(Transform transform) {
                    return null;
                }

                @Override
                public Transform.Result parseResult(String watchId, XContentParser parser) throws IOException {
                    assertThat(parser.currentToken(), is(XContentParser.Token.START_OBJECT));
                    parser.nextToken();
                    assertThat(parser.currentToken(), is(XContentParser.Token.FIELD_NAME));
                    assertThat(parser.currentName(), is("payload"));
                    parser.nextToken();
                    assertThat(parser.currentToken(), is(XContentParser.Token.START_OBJECT));
                    Map<String, Object> data = parser.map();
                    assertThat(data, equalTo(result.payload().data()));
                    parser.nextToken();
                    assertThat(parser.currentToken(), is(XContentParser.Token.END_OBJECT));
                    return result;
                }
            }));
        }
    }

}
