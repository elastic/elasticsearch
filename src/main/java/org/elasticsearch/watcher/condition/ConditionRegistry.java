/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ConditionRegistry {

    private final ImmutableMap<String, ConditionFactory> factories;

    @Inject
    public ConditionRegistry(Map<String, ConditionFactory> factories) {
        this.factories = ImmutableMap.copyOf(factories);
    }

    public Set<String> types() {
        return factories.keySet();
    }

    /**
     * Parses the xcontent and returns the appropriate executable condition. Expecting the following format:
     * <code><pre>
     *     {
     *         "condition_type" : {
     *             ...              //condition body
     *         }
     *     }
     * </pre></code>
     */
    public ExecutableCondition parseExecutable(String watchId, XContentParser parser) throws IOException {
        Condition condition = parseCondition(watchId, parser);
        return factories.get(condition.type()).createExecutable(condition);
    }

    /**
     * Parses the xcontent and returns the appropriate condition. Expecting the following format:
     * <code><pre>
     *     {
     *         "condition_type" : {
     *             ...              //condition body
     *         }
     *     }
     * </pre></code>
     */
    public Condition parseCondition(String watchId, XContentParser parser) throws IOException {
        Condition condition = null;
        ConditionFactory factory = null;

        String type = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type == null) {
                throw new ConditionException("could not parse condition for watch [{}]. invalid definition. expected a field indicating the condition type, but found", watchId, token);
            } else {
                factory = factories.get(type);
                if (factory == null) {
                    throw new ConditionException("could not parse condition for watch [{}]. unknown condition type [{}]", watchId, type);
                }
                condition = factory.parseCondition(watchId, parser);
            }
        }
        if (condition == null) {
            throw new ConditionException("could not parse condition for watch [{}]. missing required condition type field", watchId);
        }
        return condition;
    }

    /**
     * Parses the xcontent and returns the appropriate condition result. Expecting the following format:
     * <code><pre>
     *     {
     *         "condition_type" : {
     *             ...              // result body
     *         }
     *     }
     * </pre></code>
     */
    public Condition.Result parseResult(String watchId, XContentParser parser) throws IOException {
        Condition.Result result = null;

        String type = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type == null) {
                throw new ConditionException("could not parse condition result for watch [{}]. invalid definition. expected a field indicating the condition type, but found", watchId, token);
            } else {
                ConditionFactory factory = factories.get(type);
                if (factory == null) {
                    throw new ConditionException("could not parse condition result for watch [{}]. un known condition type [{}]", watchId, type);
                }
                result = factory.parseResult(watchId, parser);
            }
        }
        if (result == null) {
            throw new ConditionException("could not parse condition result for watch [{}]. missing required condition type field", watchId);
        }
        return result;
    }

}
