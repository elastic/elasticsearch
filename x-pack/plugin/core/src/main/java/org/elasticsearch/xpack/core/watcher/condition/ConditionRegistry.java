/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;

public class ConditionRegistry {

    private final Map<String, ConditionFactory> factories;
    private final Clock clock;

    public ConditionRegistry(Map<String, ConditionFactory> factories, Clock clock) {
        this.clock = clock;
        this.factories = factories;
    }

    /**
     * Parses the xcontent and returns the appropriate executable condition. Expecting the following format:
     * <pre><code>
     *     {
     *         "condition_type" : {
     *             ...              //condition body
     *         }
     *     }
     * </code></pre>
     *
     * @param watchId                   The id of the watch
     * @param parser                    The parsing that contains the condition content
     */
    public ExecutableCondition parseExecutable(String watchId, XContentParser parser) throws IOException {
        ExecutableCondition condition = null;
        ConditionFactory factory;

        String type = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type == null) {
                throw new ElasticsearchParseException("could not parse condition for watch [{}]. invalid definition. expected a field " +
                        "indicating the condition type, but found", watchId, token);
            } else {
                factory = factories.get(type);
                if (factory == null) {
                    throw new ElasticsearchParseException("could not parse condition for watch [{}]. unknown condition type [{}]",
                            watchId, type);
                }
                condition = factory.parse(clock, watchId, parser);
            }
        }
        if (condition == null) {
            throw new ElasticsearchParseException("could not parse condition for watch [{}]. missing required condition type field",
                    watchId);
        }
        return condition;
    }
}
