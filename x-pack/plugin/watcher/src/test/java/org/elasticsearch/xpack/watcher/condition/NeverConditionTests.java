/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class NeverConditionTests extends ESTestCase {
    public void testExecute() throws Exception {
        ExecutableCondition executable = NeverCondition.INSTANCE;
        assertFalse(executable.execute(null).met());
    }

    public void testParserValid() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();

        ExecutableCondition executable = NeverCondition.parse("_id", parser);
        assertFalse(executable.execute(null).met());
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field("foo", "bar");
        builder.endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            NeverCondition.parse("_id", parser);
            fail("expected a condition exception trying to parse an invalid condition XContent, ["
                    + InternalAlwaysCondition.TYPE + "] condition should not parse with a body");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an empty object but found [foo]"));
        }
    }
}
