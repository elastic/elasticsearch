/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.condition.compare.CompareCondition.Op;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Locale;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class CompareConditionTests extends ESTestCase {
    public void testOpEvalEQ() throws Exception {
        assertThat(Op.EQ.eval(null, null), is(true));
        assertThat(Op.EQ.eval(4, 3.0), is(false));
        assertThat(Op.EQ.eval(3, 3.0), is(true));
        assertThat(Op.EQ.eval(2, new Float(3.0)), is(false));
        assertThat(Op.EQ.eval(3, null), is(false));
        assertThat(Op.EQ.eval(2, "2"), is(true));     // comparing as strings
        assertThat(Op.EQ.eval(3, "4"), is(false));    // comparing as strings
        assertThat(Op.EQ.eval(3, "a"), is(false));    // comparing as strings
        assertThat(Op.EQ.eval("3", 3), is(true));     // comparing as numbers
        assertThat(Op.EQ.eval("a", "aa"), is(false));
        assertThat(Op.EQ.eval("a", "a"), is(true));
        assertThat(Op.EQ.eval("aa", "ab"), is(false));
        assertThat(Op.EQ.eval(singletonMap("k", "v"), singletonMap("k", "v")), is(true));
        assertThat(Op.EQ.eval(singletonMap("k", "v"), singletonMap("k1", "v1")), is(false));
        assertThat(Op.EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k", "v")), is(true));
        assertThat(Op.EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k1", "v1")), is(false));
    }

    public void testOpEvalNotEQ() throws Exception {
        assertThat(Op.NOT_EQ.eval(null, null), is(false));
        assertThat(Op.NOT_EQ.eval(4, 3.0), is(true));
        assertThat(Op.NOT_EQ.eval(3, 3.0), is(false));
        assertThat(Op.NOT_EQ.eval(2, new Float(3.0)), is(true));
        assertThat(Op.NOT_EQ.eval(3, null), is(true));
        assertThat(Op.NOT_EQ.eval(2, "2"), is(false));     // comparing as strings
        assertThat(Op.NOT_EQ.eval(3, "4"), is(true));    // comparing as strings
        assertThat(Op.NOT_EQ.eval(3, "a"), is(true));    // comparing as strings
        assertThat(Op.NOT_EQ.eval("3", 3), is(false));     // comparing as numbers
        assertThat(Op.NOT_EQ.eval("a", "aa"), is(true));
        assertThat(Op.NOT_EQ.eval("a", "a"), is(false));
        assertThat(Op.NOT_EQ.eval("aa", "ab"), is(true));
        assertThat(Op.NOT_EQ.eval(singletonMap("k", "v"), singletonMap("k", "v")), is(false));
        assertThat(Op.NOT_EQ.eval(singletonMap("k", "v"), singletonMap("k1", "v1")), is(true));
        assertThat(Op.NOT_EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k", "v")), is(false));
        assertThat(Op.NOT_EQ.eval(Arrays.asList("k", "v"), Arrays.asList("k1", "v1")), is(true));
    }

    public void testOpEvalGTE() throws Exception {
        assertThat(Op.GTE.eval(4, 3.0), is(true));
        assertThat(Op.GTE.eval(3, 3.0), is(true));
        assertThat(Op.GTE.eval(2, new Float(3.0)), is(false));
        assertThat(Op.GTE.eval(3, null), is(false));
        assertThat(Op.GTE.eval(3, "2"), is(true));     // comparing as strings
        assertThat(Op.GTE.eval(3, "4"), is(false));    // comparing as strings
        assertThat(Op.GTE.eval(3, "a"), is(false));    // comparing as strings
        assertThat(Op.GTE.eval("4", 3), is(true));     // comparing as numbers
        assertThat(Op.GTE.eval("a", "aa"), is(false));
        assertThat(Op.GTE.eval("a", "a"), is(true));
        assertThat(Op.GTE.eval("aa", "ab"), is(false));
    }

    public void testOpEvalGT() throws Exception {
        assertThat(Op.GT.eval(4, 3.0), is(true));
        assertThat(Op.GT.eval(3, 3.0), is(false));
        assertThat(Op.GT.eval(2, new Float(3.0)), is(false));
        assertThat(Op.GT.eval(3, null), is(false));
        assertThat(Op.GT.eval(3, "2"), is(true));     // comparing as strings
        assertThat(Op.GT.eval(3, "4"), is(false));    // comparing as strings
        assertThat(Op.GT.eval(3, "a"), is(false));    // comparing as strings
        assertThat(Op.GT.eval("4", 3), is(true));     // comparing as numbers
        assertThat(Op.GT.eval("a", "aa"), is(false));
        assertThat(Op.GT.eval("a", "a"), is(false));
        assertThat(Op.GT.eval("aa", "ab"), is(false));
    }

    public void testOpEvalLTE() throws Exception {
        assertThat(Op.LTE.eval(4, 3.0), is(false));
        assertThat(Op.LTE.eval(3, 3.0), is(true));
        assertThat(Op.LTE.eval(2, new Float(3.0)), is(true));
        assertThat(Op.LTE.eval(3, null), is(false));
        assertThat(Op.LTE.eval(3, "2"), is(false));     // comparing as strings
        assertThat(Op.LTE.eval(3, "4"), is(true));    // comparing as strings
        assertThat(Op.LTE.eval(3, "a"), is(true));    // comparing as strings
        assertThat(Op.LTE.eval("4", 3), is(false));     // comparing as numbers
        assertThat(Op.LTE.eval("a", "aa"), is(true));
        assertThat(Op.LTE.eval("a", "a"), is(true));
        assertThat(Op.LTE.eval("aa", "ab"), is(true));
    }

    public void testOpEvalLT() throws Exception {
        assertThat(Op.LT.eval(4, 3.0), is(false));
        assertThat(Op.LT.eval(3, 3.0), is(false));
        assertThat(Op.LT.eval(2, new Float(3.0)), is(true));
        assertThat(Op.LT.eval(3, null), is(false));
        assertThat(Op.LT.eval(3, "2"), is(false));     // comparing as strings
        assertThat(Op.LT.eval(3, "4"), is(true));    // comparing as strings
        assertThat(Op.LT.eval(3, "a"), is(true));    // comparing as strings
        assertThat(Op.LT.eval("4", 3), is(false));     // comparing as numbers
        assertThat(Op.LT.eval("a", "aa"), is(true));
        assertThat(Op.LT.eval("a", "a"), is(false));
        assertThat(Op.LT.eval("aa", "ab"), is(true));
    }

    public void testExecute() throws Exception {
        Op op = randomFrom(Op.values());
        int value = randomInt(10);
        int payloadValue = randomInt(10);
        boolean met = op.eval(payloadValue, value);

        ExecutableCompareCondition condition = new ExecutableCompareCondition(
                new CompareCondition("ctx.payload.value", op, value), logger, SystemClock.INSTANCE);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.Simple("value", payloadValue));
        assertThat(condition.execute(ctx).met(), is(met));
    }

    public void testExecuteDateMath() throws Exception {
        ClockMock clock = new ClockMock();
        boolean met = randomBoolean();
        Op op = met ? randomFrom(Op.GT, Op.GTE, Op.NOT_EQ) : randomFrom(Op.LT, Op.LTE, Op.EQ);
        String value = "<{now-1d}>";
        DateTime payloadValue = clock.nowUTC();

        ExecutableCompareCondition condition = new ExecutableCompareCondition(
                new CompareCondition("ctx.payload.value", op, value), logger, clock);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.Simple("value", payloadValue));
        assertThat(condition.execute(ctx).met(), is(met));
    }

    public void testExecutePath() throws Exception {
        ClockMock clock = new ClockMock();
        boolean met = randomBoolean();
        Op op = met ? Op.EQ : Op.NOT_EQ;
        String value = "{{ctx.payload.value}}";
        Object payloadValue = new Object();

        ExecutableCompareCondition condition = new ExecutableCompareCondition(
                new CompareCondition("ctx.payload.value", op, value), logger, clock);
        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.Simple("value", payloadValue));
        assertThat(condition.execute(ctx).met(), is(met));
    }

    public void testParseValid() throws Exception {
        Op op = randomFrom(Op.values());
        Object value = randomFrom("value", 1, null);
        CompareConditionFactory factory = new CompareConditionFactory(Settings.EMPTY, SystemClock.INSTANCE);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.field(op.name().toLowerCase(Locale.ROOT), value);
        builder.endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        CompareCondition condition = factory.parseCondition("_id", parser);

        assertThat(condition, notNullValue());
        assertThat(condition.getPath(), is("key1.key2"));
        assertThat(condition.getOp(), is(op));
        assertThat(condition.getValue(), is(value));
    }

    public void testParseInvalidNoOperationBody() throws Exception {
        CompareConditionFactory factory = new CompareConditionFactory(Settings.EMPTY, SystemClock.INSTANCE);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        try {
            factory.parseCondition("_id", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an object but found [null] instead"));
        }
    }

    public void testParseInvalidUnknownOp() throws Exception {
        Object value = randomFrom("value", 1, null);
        CompareConditionFactory factory = new CompareConditionFactory(Settings.EMPTY, SystemClock.INSTANCE);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.field("foobar", value);
        builder.endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        try {
            factory.parseCondition("_id", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("unknown comparison operator [foobar]"));
        }
    }

    public void testParseInvalidWrongValueForOp() throws Exception {
        Object value = randomFrom(Arrays.asList("1", "2"), singletonMap("key", "value"));
        String op = randomFrom("lt", "lte", "gt", "gte");
        CompareConditionFactory factory = new CompareConditionFactory(Settings.EMPTY, SystemClock.INSTANCE);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("key1.key2");
        builder.field(op, value);
        builder.endObject();
        builder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        try {
            factory.parseCondition("_id", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("must either be a numeric, string, boolean or null value, but found ["));
        }
    }
}
