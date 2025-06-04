/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.manual;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;

import java.io.IOException;

public class ManualTrigger implements Trigger {

    @Override
    public String type() {
        return ManualTriggerEngine.TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    static ManualTrigger parse(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "unable to parse ["
                    + ManualTriggerEngine.TYPE
                    + "] trigger. expected a start object token, found ["
                    + parser.currentToken()
                    + "]"
            );
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException(
                "unable to parse ["
                    + ManualTriggerEngine.TYPE
                    + "] trigger. expected an empty object, but found an object with ["
                    + token
                    + "]"
            );
        }
        return new ManualTrigger();
    }
}
