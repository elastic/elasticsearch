/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.simple;

import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class TestAlwaysFalseCondition extends ElasticsearchTestCase {


    @Test
    public void testExecute() throws Exception {
        Condition alwaysTrue = new AlwaysTrueCondition(logger);
        assertTrue(alwaysTrue.execute(null).met());
    }

    @Test
    public void testParser_Valid() throws Exception {
        Condition.Parser p = new AlwaysTrueCondition.Parser(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser xp = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        xp.nextToken();

        Condition alwaysTrue = p.parse(xp);
        assertTrue(alwaysTrue.execute(null).met());
    }

    @Test(expected = ConditionException.class)
    public void testParser_Invalid() throws Exception {
        Condition.Parser p = new AlwaysTrueCondition.Parser(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field("foo", "bar");
        builder.endObject();
        XContentParser xp = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        xp.nextToken();

        p.parse(xp);
        fail("expected a condition exception trying to parse an invalid condition XContent, ["
                + AlwaysTrueCondition.TYPE + "] condition should not parse with a body");
    }


    @Test
    public void testResultParser_Valid() throws Exception {
        Condition.Parser p = new AlwaysTrueCondition.Parser(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser xp = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        xp.nextToken();

        Condition.Result alwaysTrueResult = p.parseResult(xp);
        assertTrue(alwaysTrueResult.met());
    }

    @Test(expected = ConditionException.class)
    public void testResultParser_Invalid() throws Exception {
        Condition.Parser p = new AlwaysTrueCondition.Parser(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field("met", false );
        builder.endObject();
        XContentParser xp = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        xp.nextToken();

        p.parseResult(xp);
        fail("expected a condition exception trying to parse an invalid condition result XContent, ["
                + AlwaysTrueCondition.TYPE + "] condition result should not parse with a [met] field");
    }
}
