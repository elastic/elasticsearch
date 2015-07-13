/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.never;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.condition.ConditionFactory;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
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
        NeverConditionFactory factory = new NeverConditionFactory(Settings.settingsBuilder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();

        NeverCondition condition = factory.parseCondition("_id", parser);
        ExecutableNeverCondition executable = factory.createExecutable(condition);
        assertFalse(executable.execute(null).met());
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testParser_Invalid() throws Exception {
        ConditionFactory factory = new NeverConditionFactory(Settings.settingsBuilder().build());
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

}
