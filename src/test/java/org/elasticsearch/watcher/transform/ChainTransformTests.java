/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
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
                new NamedTransform("name1"),
                new NamedTransform("name2"),
                new NamedTransform("name3")));

        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Payload payload = new Payload.Simple(new HashMap<String, Object>());

        Transform.Result result = transform.apply(ctx, payload);

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
        Map<String, Transform.Parser> parsers = ImmutableMap.<String, Transform.Parser>builder()
                .put("named", new NamedTransform.Parser())
                .build();
        TransformRegistry registry = new TransformRegistry(parsers);

        ChainTransform.Parser transformParser = new ChainTransform.Parser(registry);

        XContentBuilder builder = jsonBuilder().startArray()
                .startObject().startObject("named").field("name", "name1").endObject().endObject()
                .startObject().startObject("named").field("name", "name2").endObject().endObject()
                .startObject().startObject("named").field("name", "name3").endObject().endObject()
                .endArray();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ChainTransform transform = transformParser.parse(parser);
        assertThat(transform, notNullValue());
        assertThat(transform.transforms(), notNullValue());
        assertThat(transform.transforms(), hasSize(3));
        for (int i = 0; i < transform.transforms().size(); i++) {
            assertThat(transform.transforms().get(i), instanceOf(NamedTransform.class));
            assertThat(((NamedTransform) transform.transforms().get(i)).name, is("name" + (i + 1)));
        }
    }

    private static class NamedTransform extends Transform<NamedTransform.Result> {

        private final String name;

        public NamedTransform(String name) {
            this.name = name;
        }

        @Override
        public String type() {
            return "noop";
        }

        @Override
        public Result apply(WatchExecutionContext ctx, Payload payload) throws IOException {

            Map<String, Object> data = new HashMap<>(payload.data());
            List<String> names = (List<String>) data.get("names");
            if (names == null) {
                names = new ArrayList<>();
                data.put("names", names);
            }
            names.add(name);
            return new Result("named", new Payload.Simple(data));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("name", name).endObject();
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

        public static class Parser implements Transform.Parser<Result, NamedTransform> {

            @Override
            public String type() {
                return "named";
            }

            @Override
            public NamedTransform parse(XContentParser parser) throws IOException {
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                XContentParser.Token token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME; // the "name" field
                token = parser.nextToken();
                assert token == XContentParser.Token.VALUE_STRING;
                String name = parser.text();
                token = parser.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return new NamedTransform(name);
            }

            @Override
            public Result parseResult(XContentParser parser) throws IOException {
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
        }

    }
}
