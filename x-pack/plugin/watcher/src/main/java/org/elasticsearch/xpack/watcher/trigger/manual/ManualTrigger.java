/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.manual;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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

    static ManualTrigger parse(XContentParser parser) throws IOException{
        if (parser.currentToken() != XContentParser.Token.START_OBJECT){
            throw new ElasticsearchParseException("unable to parse [" + ManualTriggerEngine.TYPE +
                    "] trigger. expected a start object token, found [" + parser.currentToken() + "]");
        }
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("unable to parse [" + ManualTriggerEngine.TYPE +
                    "] trigger. expected an empty object, but found an object with [" + token + "]");
        }
        return new ManualTrigger();
    }
}
