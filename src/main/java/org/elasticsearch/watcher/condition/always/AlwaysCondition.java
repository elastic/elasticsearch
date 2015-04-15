/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.always;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.Condition;

import java.io.IOException;

/**
 *
 */
public class AlwaysCondition implements Condition {

    public static final String TYPE = "always";
    public static final AlwaysCondition INSTANCE = new AlwaysCondition();

    @Override
    public final String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    public static AlwaysCondition parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new AlwaysConditionException("unable to parse [{}] condition for watch [{}]. expected an empty object but found [{}]", TYPE, watchId, parser.currentName());
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new AlwaysConditionException("unable to parse [{}] condition for watch [{}]. expected an empty object but found [{}]", TYPE, watchId, parser.currentName());
        }
        return INSTANCE;
    }

    public static class Result extends Condition.Result {

        public static final Result INSTANCE = new Result();

        private Result() {
            super(TYPE, true);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

        public static Result parse(String watchId, XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new AlwaysConditionException("unable to parse [{}] condition result for watch [{}]. expected an empty object but found [{}]", TYPE, watchId, parser.currentName());
            }
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new AlwaysConditionException("unable to parse [{}] condition result for watch [{}]. expected an empty object but found [{}]", TYPE, watchId, parser.currentName());
            }
            return INSTANCE;
        }
    }

    public static class Builder implements Condition.Builder<AlwaysCondition> {

        public static final Builder INSTANCE = new Builder();

        public AlwaysCondition build() {
            return AlwaysCondition.INSTANCE;
        }
    }
}
