/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition.always;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.condition.ConditionFactory;
import org.elasticsearch.xpack.watcher.condition.ExecutableCondition;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class AlwaysConditionTests extends ESTestCase {
    public void testExecute() throws Exception {
        ExecutableCondition alwaysTrue = new ExecutableAlwaysCondition(logger);
        assertTrue(alwaysTrue.execute(null).met());
    }

    public void testParserValid() throws Exception {
        AlwaysConditionFactory factory = new AlwaysConditionFactory(Settings.builder().build());
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        AlwaysCondition condition = factory.parseCondition("_id", parser, false);
        ExecutableAlwaysCondition executable = factory.createExecutable(condition);
        assertTrue(executable.execute(null).met());
    }

    public void testParserInvalid() throws Exception {
        ConditionFactory factor = new AlwaysConditionFactory(Settings.builder().build());
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("foo", "bar")
                .endObject();
        XContentParser parser = XContentFactory.xContent(builder.bytes()).createParser(builder.bytes());
        parser.nextToken();
        try {
            factor.parseCondition("_id", parser, false);
            fail("expected a condition exception trying to parse an invalid condition XContent, ["
                    + AlwaysCondition.TYPE + "] condition should not parse with a body");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an empty object but found [foo]"));
        }
    }
}
