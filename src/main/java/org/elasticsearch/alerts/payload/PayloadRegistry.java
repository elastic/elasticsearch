/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.payload;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class PayloadRegistry {

    private final ImmutableMap<String, Payload.Parser> parsers;

    @Inject
    public PayloadRegistry(Map<String, Payload.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    public Payload parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Payload payload = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                Payload.Parser payloadParser = parsers.get(type);
                if (payloadParser == null) {
                    throw new AlertsSettingsException("unknown payload type [" + type + "]");
                }
                payload = payloadParser.parse(parser);
            }
        }
        return payload;
    }

    public Payload parse(String type, XContentParser parser) throws IOException {
        Payload.Parser payloadParser = parsers.get(type);
        if (payloadParser == null) {
            throw new AlertsSettingsException("unknown payload type [" + type + "]");
        }
        return payloadParser.parse(parser);
    }
}
