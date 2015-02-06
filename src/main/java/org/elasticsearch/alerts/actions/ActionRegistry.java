/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class ActionRegistry  {

    private final ImmutableMap<String, Action.Parser> parsers;

    @Inject
    public ActionRegistry(Map<String, Action.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    /**
     * Reads the contents of parser to create the correct Action
     * @param parser The parser containing the trigger definition
     * @return a new Action instance from the parser
     * @throws IOException
     */
    public Action parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Action action = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                Action.Parser triggerParser = parsers.get(type);
                if (triggerParser == null) {
                    throw new ActionException("unknown action type [" + type + "]");
                }
                action = triggerParser.parse(parser);
            }
        }
        return action;
    }

    /**
     * Reads the contents of parser to create the correct Action.Result
     * @param parser The parser containing the trigger definition
     * @return a new Action.Result instance from the parser
     * @throws IOException
     */
    public Action.Result parseResult(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Action.Result result = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                Action.Parser triggerParser = parsers.get(type);
                if (triggerParser == null) {
                    throw new ActionException("unknown action type [" + type + "]");
                }
                result = triggerParser.parseResult(parser);
            }
        }
        return result;
    }


    public Actions parseActions(XContentParser parser) throws IOException {
        List<Action> actions = new ArrayList<>();

        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            actions.add(parse(parser));
        }

        return new Actions(actions);
    }


}
