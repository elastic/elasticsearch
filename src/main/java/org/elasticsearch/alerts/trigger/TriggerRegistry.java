/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger;

import org.elasticsearch.alerts.trigger.search.SearchTrigger;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class TriggerRegistry {

    private final ImmutableMap<String, SearchTrigger.Parser> parsers;

    @Inject
    public TriggerRegistry(Map<String, SearchTrigger.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    /**
     * Reads the contents of parser to create the correct Trigger
     * @param parser The parser containing the trigger definition
     * @return a new AlertTrigger instance from the parser
     * @throws IOException
     */
    public Trigger parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Trigger trigger = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                SearchTrigger.Parser triggerParser = parsers.get(type);
                if (triggerParser == null) {
                    throw new TriggerException("unknown trigger type [" + type + "]");
                }
                trigger = triggerParser.parse(parser);
            }
        }
        return trigger;
    }

    public Trigger.Result parseResult(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Trigger.Result triggerResult = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                SearchTrigger.Parser triggerParser = parsers.get(type);
                if (triggerParser == null) {
                    throw new TriggerException("unknown trigger type [" + type + "]");
                }
                triggerResult = triggerParser.parseResult(parser);
            }
        }
        return triggerResult;
    }

}
