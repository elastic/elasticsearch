/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;

import java.io.IOException;

public final class InternalAlwaysCondition extends AlwaysCondition implements ExecutableCondition {

    public static final Result RESULT_INSTANCE = new Result(null, TYPE, true);
    public static final InternalAlwaysCondition INSTANCE = new InternalAlwaysCondition();

    private InternalAlwaysCondition() {}

    public static InternalAlwaysCondition parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "unable to parse [{}] condition for watch [{}]. expected an empty object but found [{}]",
                TYPE,
                watchId,
                parser.currentName()
            );
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException(
                "unable to parse [{}] condition for watch [{}]. expected an empty object but found [{}]",
                TYPE,
                watchId,
                parser.currentName()
            );
        }
        return INSTANCE;
    }

    @Override
    public Result execute(WatchExecutionContext ctx) {
        return RESULT_INSTANCE;
    }

}
