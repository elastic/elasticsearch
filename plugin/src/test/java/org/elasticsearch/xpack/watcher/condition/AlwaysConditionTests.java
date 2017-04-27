/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class AlwaysConditionTests extends ESTestCase {
    public void testExecute() throws Exception {
        Condition alwaysTrue = AlwaysCondition.INSTANCE;
        assertTrue(alwaysTrue.execute(null).met());
    }

    public void testParserValid() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        Condition executable = AlwaysCondition.parse("_id", parser);
        assertTrue(executable.execute(null).met());
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("foo", "bar")
                .endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            AlwaysCondition.parse( "_id", parser);
            fail("expected a condition exception trying to parse an invalid condition XContent, ["
                    + AlwaysCondition.TYPE + "] condition should not parse with a body");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an empty object but found [foo]"));
        }
    }

    public static Condition randomCondition(ScriptService scriptService) {
        String type = randomFrom(ScriptCondition.TYPE, AlwaysCondition.TYPE, CompareCondition.TYPE, ArrayCompareCondition.TYPE);
        switch (type) {
            case ScriptCondition.TYPE:
                return new ScriptCondition(mockScript("_script"), scriptService);
            case CompareCondition.TYPE:
                return new CompareCondition("_path", randomFrom(CompareCondition.Op.values()), randomFrom(5, "3"),
                        Clock.systemUTC());
            case ArrayCompareCondition.TYPE:
                return new ArrayCompareCondition("_array_path", "_path",
                        randomFrom(ArrayCompareCondition.Op.values()), randomFrom(5, "3"), ArrayCompareCondition.Quantifier.SOME,
                        Clock.systemUTC());
            default:
                return AlwaysCondition.INSTANCE;
        }
    }
}
