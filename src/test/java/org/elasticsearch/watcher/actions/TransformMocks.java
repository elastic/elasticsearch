/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.transform.Transform;
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

    public static class TransformMock extends Transform<TransformMock.Result> {

        @Override
        public String type() {
            return "_transform";
        }

        @Override
        public Result apply(WatchExecutionContext ctx, Payload payload) throws IOException {
            return new Result("_transform", new Payload.Simple("_key", "_value"));
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

        public TransformRegistryMock(final Transform transform) {
            super(ImmutableMap.<String, Transform.Parser>of("_transform", new Transform.Parser() {
                @Override
                public String type() {
                    return transform.type();
                }

                @Override
                public Transform parse(XContentParser parser) throws IOException {
                    parser.nextToken();
                    assertThat(parser.currentToken(), is(XContentParser.Token.END_OBJECT));
                    return transform;
                }

                @Override
                public Transform.Result parseResult(XContentParser parser) throws IOException {
                    return null; // should not be called when this ctor is used
                }
            }));
        }

        public TransformRegistryMock(final Transform.Result result) {
            super(ImmutableMap.<String, Transform.Parser>of("_transform_type", new Transform.Parser() {
                @Override
                public String type() {
                    return result.type();
                }

                @Override
                public Transform parse(XContentParser parser) throws IOException {
                    return null; // should not be called when this ctor is used.
                }

                @Override
                public Transform.Result parseResult(XContentParser parser) throws IOException {
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
