/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class ConditionRegistry {

    private final Map<String, ConditionFactory> factories;

    @Inject
    public ConditionRegistry(Map<String, ConditionFactory> factories) {
        this.factories = factories;
    }

    public Set<String> types() {
        return factories.keySet();
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
     * @param upgradeConditionSource    Whether to upgrade the source related to condition if in legacy format
     *                                  Note: depending on the version, only conditions implementations that have a
     *                                  known legacy format will support this option, otherwise this is a noop.
     */
    public ExecutableCondition parseExecutable(String watchId, XContentParser parser, boolean upgradeConditionSource) throws IOException {
        Condition condition = parseCondition(watchId, parser, upgradeConditionSource);
        return factories.get(condition.type()).createExecutable(condition);
    }

    /**
     * Parses the xcontent and returns the appropriate condition. Expecting the following format:
     * <pre><code>
     *     {
     *         "condition_type" : {
     *             ...              //condition body
     *         }
     *     }
     * </code></pre>
     */
    public Condition parseCondition(String watchId, XContentParser parser, boolean upgradeConditionSource) throws IOException {
        Condition condition = null;
        ConditionFactory factory = null;

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
                condition = factory.parseCondition(watchId, parser, upgradeConditionSource);
            }
        }
        if (condition == null) {
            throw new ElasticsearchParseException("could not parse condition for watch [{}]. missing required condition type field",
                    watchId);
        }
        return condition;
    }

    public static void writeResult(Condition.Result result, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject()
                .field(Condition.Field.MET.getPreferredName(), result.met())
                .field(result.type(), result, params)
                .endObject();
    }
}
