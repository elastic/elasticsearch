/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;

import java.time.Clock;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class AlwaysConditionTests extends ESTestCase {
    public void testExecute() throws Exception {
        ExecutableCondition alwaysTrue = InternalAlwaysCondition.INSTANCE;
        assertTrue(alwaysTrue.execute(null).met());
    }

    public void testParserValid() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        ExecutableCondition executable = InternalAlwaysCondition.parse("_id", parser);
        assertTrue(executable.execute(null).met());
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("foo", "bar").endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            InternalAlwaysCondition.parse("_id", parser);
            fail(
                "expected a condition exception trying to parse an invalid condition XContent, ["
                    + InternalAlwaysCondition.TYPE
                    + "] condition should not parse with a body"
            );
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("expected an empty object but found [foo]"));
        }
    }

    public static ExecutableCondition randomCondition(ScriptService scriptService) {
        String type = randomFrom(ScriptCondition.TYPE, InternalAlwaysCondition.TYPE, CompareCondition.TYPE, ArrayCompareCondition.TYPE);
        switch (type) {
            case ScriptCondition.TYPE:
                Script mockScript = mockScript("_script");
                return new ScriptCondition(mockScript, scriptService);
            case CompareCondition.TYPE:
                return new CompareCondition("_path", randomFrom(CompareCondition.Op.values()), randomFrom(5, "3"), Clock.systemUTC());
            case ArrayCompareCondition.TYPE:
                return new ArrayCompareCondition(
                    "_array_path",
                    "_path",
                    randomFrom(ArrayCompareCondition.Op.values()),
                    randomFrom(5, "3"),
                    ArrayCompareCondition.Quantifier.SOME,
                    Clock.systemUTC()
                );
            default:
                return InternalAlwaysCondition.INSTANCE;
        }
    }
}
