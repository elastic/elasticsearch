/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.condition.CompareCondition.Op;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Locale;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CompareConditionTests extends ESTestCase {
    public void testOpEvalEQ() throws Exception {
        assertThat(CompareCondition.Op.EQ.eval(null, null), is(true));
        assertThat(CompareCondition.Op.EQ.eval(4, 3.0), is(false));
        assertThat(CompareCondition.Op.EQ.eval(3, 3.0), is(true));
        assertThat(CompareCondition.Op.EQ.eval(2, Float.valueOf((float) 3.0)), is(false));
        assertThat(CompareCondition.Op.EQ.eval(3, null), is(false));
        assertThat(CompareCondition.Op.EQ.eval(2, "2"), is(true));     // comparing as strings
        assertThat(CompareCondition.Op.EQ.eval(3, "4"), is(false));    // comparing as strings
        assertThat(CompareCondition.Op.EQ.eval(3, "a"), is(false));    // comparing as strings
        assertThat(CompareCondition.Op.EQ.eval("3", 3), is(true));     // comparing as numbers
        assertThat(CompareCondition.Op.EQ.eval("a", "aa"), is(false));
        assertThat(CompareCondition.Op.EQ.eval("a", "a"), is(true));
        assertThat(CompareCondition.Op.EQ.eval("aa", "ab"), is(false));
        assertThat(CompareCondition.Op.EQ.eval(singletonMap("k", "v"), singletonMap("k", "v")), is(true));
        assertThat(CompareCondition.Op.EQ.eval(singletonMap("k", "v"), singletonMap("k1", "v1")), is(false));
        assertThat(CompareCondition.Op.EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k", "v")), is(true));
        assertThat(CompareCondition.Op.EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k1", "v1")), is(false));
        assertThat(CompareCondition.Op.EQ.eval(Double.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.EQ.eval(250000, Double.NaN), is(false));
        assertThat(CompareCondition.Op.EQ.eval(Double.NaN, Double.NaN), is(true));
        assertThat(CompareCondition.Op.EQ.eval(Float.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.EQ.eval(250000, Float.NaN), is(false));
        assertThat(CompareCondition.Op.EQ.eval(Float.NaN, Float.NaN), is(true));

    }

    public void testOpEvalNotEQ() throws Exception {
        assertThat(CompareCondition.Op.NOT_EQ.eval(null, null), is(false));
        assertThat(CompareCondition.Op.NOT_EQ.eval(4, 3.0), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(3, 3.0), is(false));
        assertThat(CompareCondition.Op.NOT_EQ.eval(2, Float.valueOf((float) 3.0)), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(3, null), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(2, "2"), is(false));     // comparing as strings
        assertThat(CompareCondition.Op.NOT_EQ.eval(3, "4"), is(true));    // comparing as strings
        assertThat(CompareCondition.Op.NOT_EQ.eval(3, "a"), is(true));    // comparing as strings
        assertThat(CompareCondition.Op.NOT_EQ.eval("3", 3), is(false));     // comparing as numbers
        assertThat(CompareCondition.Op.NOT_EQ.eval("a", "aa"), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval("a", "a"), is(false));
        assertThat(CompareCondition.Op.NOT_EQ.eval("aa", "ab"), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(singletonMap("k", "v"), singletonMap("k", "v")), is(false));
        assertThat(CompareCondition.Op.NOT_EQ.eval(singletonMap("k", "v"), singletonMap("k1", "v1")), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k", "v")), is(false));
        assertThat(CompareCondition.Op.NOT_EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k1", "v1")), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(Double.NaN, 250000), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(250000, Double.NaN), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(Double.NaN, Double.NaN), is(false));
        assertThat(CompareCondition.Op.NOT_EQ.eval(Float.NaN, 250000), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(250000, Float.NaN), is(true));
        assertThat(CompareCondition.Op.NOT_EQ.eval(Float.NaN, Float.NaN), is(false));
    }

    public void testOpEvalGTE() throws Exception {
        assertThat(CompareCondition.Op.GTE.eval(4, 3.0), is(true));
        assertThat(CompareCondition.Op.GTE.eval(3, 3.0), is(true));
        assertThat(CompareCondition.Op.GTE.eval(2, Float.valueOf((float) 3.0)), is(false));
        assertThat(CompareCondition.Op.GTE.eval(3, null), is(false));
        assertThat(CompareCondition.Op.GTE.eval(3, "2"), is(true));     // comparing as strings
        assertThat(CompareCondition.Op.GTE.eval(3, "4"), is(false));    // comparing as strings
        assertThat(CompareCondition.Op.GTE.eval(3, "a"), is(false));    // comparing as strings
        assertThat(CompareCondition.Op.GTE.eval("4", 3), is(true));     // comparing as numbers
        assertThat(CompareCondition.Op.GTE.eval("a", "aa"), is(false));
        assertThat(CompareCondition.Op.GTE.eval("a", "a"), is(true));
        assertThat(CompareCondition.Op.GTE.eval("aa", "ab"), is(false));
        assertThat(CompareCondition.Op.GTE.eval(Double.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.GTE.eval(250000, Double.NaN), is(false));
        assertThat(CompareCondition.Op.GTE.eval(Double.NaN, Double.NaN), is(true));
        assertThat(CompareCondition.Op.GTE.eval(Float.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.GTE.eval(250000, Float.NaN), is(false));
        assertThat(CompareCondition.Op.GTE.eval(Float.NaN, Float.NaN), is(true));
    }

    public void testOpEvalGT() throws Exception {
        assertThat(CompareCondition.Op.GT.eval(4, 3.0), is(true));
        assertThat(CompareCondition.Op.GT.eval(3, 3.0), is(false));
        assertThat(CompareCondition.Op.GT.eval(2, Float.valueOf((float) 3.0)), is(false));
        assertThat(CompareCondition.Op.GT.eval(3, null), is(false));
        assertThat(CompareCondition.Op.GT.eval(3, "2"), is(true));     // comparing as strings
        assertThat(CompareCondition.Op.GT.eval(3, "4"), is(false));    // comparing as strings
        assertThat(CompareCondition.Op.GT.eval(3, "a"), is(false));    // comparing as strings
        assertThat(CompareCondition.Op.GT.eval("4", 3), is(true));     // comparing as numbers
        assertThat(CompareCondition.Op.GT.eval("a", "aa"), is(false));
        assertThat(CompareCondition.Op.GT.eval("a", "a"), is(false));
        assertThat(CompareCondition.Op.GT.eval("aa", "ab"), is(false));
        assertThat(CompareCondition.Op.GT.eval(Double.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.GT.eval(250000, Double.NaN), is(false));
        assertThat(CompareCondition.Op.GT.eval(Double.NaN, Double.NaN), is(false));
        assertThat(CompareCondition.Op.GT.eval(Float.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.GT.eval(250000, Float.NaN), is(false));
        assertThat(CompareCondition.Op.GT.eval(Float.NaN, Float.NaN), is(false));

    }

    public void testOpEvalLTE() throws Exception {
        assertThat(CompareCondition.Op.LTE.eval(4, 3.0), is(false));
        assertThat(CompareCondition.Op.LTE.eval(3, 3.0), is(true));
        assertThat(CompareCondition.Op.LTE.eval(2, Float.valueOf((float) 3.0)), is(true));
        assertThat(CompareCondition.Op.LTE.eval(3, null), is(false));
        assertThat(CompareCondition.Op.LTE.eval(3, "2"), is(false));     // comparing as strings
        assertThat(CompareCondition.Op.LTE.eval(3, "4"), is(true));    // comparing as strings
        assertThat(CompareCondition.Op.LTE.eval(3, "a"), is(true));    // comparing as strings
        assertThat(CompareCondition.Op.LTE.eval("4", 3), is(false));     // comparing as numbers
        assertThat(CompareCondition.Op.LTE.eval("a", "aa"), is(true));
        assertThat(CompareCondition.Op.LTE.eval("a", "a"), is(true));
        assertThat(CompareCondition.Op.LTE.eval("aa", "ab"), is(true));
        assertThat(CompareCondition.Op.LTE.eval(Double.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.LTE.eval(250000, Double.NaN), is(false));
        assertThat(CompareCondition.Op.LTE.eval(Double.NaN, Double.NaN), is(true));
        assertThat(CompareCondition.Op.LTE.eval(Float.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.LTE.eval(250000, Float.NaN), is(false));
        assertThat(CompareCondition.Op.LTE.eval(Float.NaN, Float.NaN), is(true));
    }

    public void testOpEvalLT() throws Exception {
        assertThat(CompareCondition.Op.LT.eval(4, 3.0), is(false));
        assertThat(CompareCondition.Op.LT.eval(3, 3.0), is(false));
        assertThat(CompareCondition.Op.LT.eval(2, Float.valueOf((float) 3.0)), is(true));
        assertThat(CompareCondition.Op.LT.eval(3, null), is(false));
        assertThat(CompareCondition.Op.LT.eval(3, "2"), is(false));     // comparing as strings
        assertThat(CompareCondition.Op.LT.eval(3, "4"), is(true));    // comparing as strings
        assertThat(CompareCondition.Op.LT.eval(3, "a"), is(true));    // comparing as strings
        assertThat(CompareCondition.Op.LT.eval("4", 3), is(false));     // comparing as numbers
        assertThat(CompareCondition.Op.LT.eval("a", "aa"), is(true));
        assertThat(CompareCondition.Op.LT.eval("a", "a"), is(false));
        assertThat(CompareCondition.Op.LT.eval("aa", "ab"), is(true));
        assertThat(CompareCondition.Op.LT.eval(Double.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.LT.eval(250000, Double.NaN), is(false));
        assertThat(CompareCondition.Op.LT.eval(Double.NaN, Double.NaN), is(false));
        assertThat(CompareCondition.Op.LT.eval(Float.NaN, 250000), is(false));
        assertThat(CompareCondition.Op.LT.eval(250000, Float.NaN), is(false));
        assertThat(CompareCondition.Op.LT.eval(Float.NaN, Float.NaN), is(false));
    }

    public void testExecute() throws Exception {
        Op op = randomFrom(CompareCondition.Op.values());
        int value = randomInt(10);
        int payloadValue = randomInt(10);
        boolean met = op.eval(payloadValue, value);

        CompareCondition condition = new CompareCondition("ctx.payload.value", op, value, Clock.systemUTC());
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.Simple("value", payloadValue));
        assertThat(condition.execute(ctx).met(), is(met));
    }

    public void testExecuteDateMath() throws Exception {
        ClockMock clock = ClockMock.frozen();
        boolean met = randomBoolean();
        Op op = met
            ? randomFrom(CompareCondition.Op.GT, CompareCondition.Op.GTE, CompareCondition.Op.NOT_EQ)
            : randomFrom(CompareCondition.Op.LT, CompareCondition.Op.LTE, CompareCondition.Op.EQ);
        String value = "<{now-1d}>";
        ZonedDateTime payloadValue = clock.instant().atZone(ZoneId.systemDefault());

        CompareCondition condition = new CompareCondition("ctx.payload.value", op, value, clock);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.Simple("value", payloadValue));
        assertThat(condition.execute(ctx).met(), is(met));
    }

    public void testExecutePath() throws Exception {
        ClockMock clock = ClockMock.frozen();
        boolean met = randomBoolean();
        Op op = met ? CompareCondition.Op.EQ : CompareCondition.Op.NOT_EQ;
        String value = "{{ctx.payload.value}}";
        Object payloadValue = new Object();

        CompareCondition condition = new CompareCondition("ctx.payload.value", op, value, clock);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.Simple("value", payloadValue));
        assertThat(condition.execute(ctx).met(), is(met));
    }

    public void testParseValid() throws Exception {
        Op op = randomFrom(CompareCondition.Op.values());
        Object value = randomFrom("value", 1, null);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.field(op.name().toLowerCase(Locale.ROOT), value);
        builder.endObject();
        builder.endObject();

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken();

        CompareCondition condition = CompareCondition.parse(ClockMock.frozen(), "_id", parser);

        assertThat(condition, notNullValue());
        assertThat(condition.getPath(), is("key1.key2"));
        assertThat(condition.getOp(), is(op));
        assertThat(condition.getValue(), is(value));
    }

    public void testParseInvalidNoOperationBody() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.endObject();
        builder.endObject();

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        try {
            CompareCondition.parse(ClockMock.frozen(), "_id", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an object but found [null] instead"));
        }
    }

    public void testParseInvalidUnknownOp() throws Exception {
        Object value = randomFrom("value", 1, null);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.field("foobar", value);
        builder.endObject();
        builder.endObject();

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken();
        try {
            CompareCondition.parse(ClockMock.frozen(), "_id", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("unknown comparison operator [foobar]"));
        }
    }

    public void testParseInvalidWrongValueForOp() throws Exception {
        Object value = randomFrom(Arrays.asList("1", "2"), singletonMap("key", "value"));
        String op = randomFrom("lt", "lte", "gt", "gte");
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.field(op, value);
        builder.endObject();
        builder.endObject();

        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken();
        try {
            CompareCondition.parse(ClockMock.frozen(), "_id", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("must either be a numeric, string, boolean or null value, but found ["));
        }
    }
}
