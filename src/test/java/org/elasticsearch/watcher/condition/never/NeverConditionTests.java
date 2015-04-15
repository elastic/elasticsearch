/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.never;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ConditionFactory;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.always.AlwaysConditionException;
import org.elasticsearch.watcher.condition.always.AlwaysConditionFactory;
import org.elasticsearch.watcher.condition.never.ExecutableNeverCondition;
import org.elasticsearch.watcher.condition.never.NeverCondition;
import org.elasticsearch.watcher.condition.never.NeverConditionException;
import org.elasticsearch.watcher.condition.never.NeverConditionFactory;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class NeverConditionTests extends ElasticsearchTestCase {

    @Test
    public void testExecute() throws Exception {
        ExecutableCondition executable = new ExecutableNeverCondition(logger);
        assertFalse(executable.execute(null).met());
    }

    @Test
    public void testParser_Valid() throws Exception {
        NeverConditionFactory factory = new NeverConditionFactory(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();

        NeverCondition condition = factory.parseCondition("_id", parser);
        ExecutableNeverCondition executable = factory.createExecutable(condition);
        assertFalse(executable.execute(null).met());
    }

    @Test(expected = NeverConditionException.class)
    public void testParser_Invalid() throws Exception {
        ConditionFactory factory = new NeverConditionFactory(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field("foo", "bar");
        builder.endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();

        factory.parseCondition("_id", parser);
        fail("expected a condition exception trying to parse an invalid condition XContent, ["
                + AlwaysCondition.TYPE + "] condition should not parse with a body");
    }


    @Test
    public void testResultParser_Valid() throws Exception {
        ConditionFactory factory = new NeverConditionFactory(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();

        Condition.Result result = factory.parseResult("_id", parser);
        assertFalse(result.met());
    }

    @Test(expected = NeverConditionException.class)
    public void testResultParser_Invalid() throws Exception {
        ConditionFactory factory = new NeverConditionFactory(ImmutableSettings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field("met", false);
        builder.endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();

        factory.parseResult("_id", parser);
        fail("expected a condition exception trying to parse an invalid condition result XContent, ["
                + NeverCondition.TYPE + "] condition result should not parse with a [met] field");
    }
}
