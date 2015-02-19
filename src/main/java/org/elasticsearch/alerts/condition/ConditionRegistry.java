/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ConditionRegistry {

    private final ImmutableMap<String, Condition.Parser> parsers;

    @Inject
    public ConditionRegistry(Map<String, Condition.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    /**
     * Reads the contents of parser to create the correct Condition
     *
     * @param parser    The parser containing the condition definition
     * @return          A new condition instance from the parser
     * @throws IOException
     */
    public Condition parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Condition condition = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                Condition.Parser conditionParser = parsers.get(type);
                if (conditionParser == null) {
                    throw new ConditionException("unknown condition type [" + type + "]");
                }
                condition = conditionParser.parse(parser);
            }
        }
        return condition;
    }

    /**
     * Reads the contents of parser to create the correct Condition.Result
     *
     * @param parser    The parser containing the condition result definition
     * @return          A new condition result instance from the parser
     * @throws IOException
     */
    public Condition.Result parseResult(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Condition.Result conditionResult = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                Condition.Parser conditionParser = parsers.get(type);
                if (conditionParser == null) {
                    throw new ConditionException("unknown condition type [" + type + "]");
                }
                conditionResult = conditionParser.parseResult(parser);
            }
        }
        return conditionResult;
    }

}
