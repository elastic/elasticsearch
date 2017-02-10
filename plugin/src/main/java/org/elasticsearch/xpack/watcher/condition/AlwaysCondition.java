/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;

import java.io.IOException;

public final class AlwaysCondition extends Condition {

    public static final String TYPE = "always";
    public static final Result RESULT_INSTANCE = new Result(null, TYPE, true);
    public static final Condition INSTANCE = new AlwaysCondition();

    private AlwaysCondition() {
        super(TYPE);
    }

    public static Condition parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("unable to parse [{}] condition for watch [{}]. expected an empty object but found [{}]",
                    TYPE, watchId, parser.currentName());
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("unable to parse [{}] condition for watch [{}]. expected an empty object but found [{}]",
                    TYPE, watchId, parser.currentName());
        }
        return INSTANCE;
    }

    @Override
    public Result execute(WatchExecutionContext ctx) {
        return RESULT_INSTANCE;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AlwaysCondition;
    }

    @Override
    public int hashCode() {
        // All instances has to produce the same hashCode because they are all equal
        return 0;
    }
}
