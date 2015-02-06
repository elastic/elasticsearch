/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class TransformRegistry {

    private final ImmutableMap<String, Transform.Parser> parsers;

    @Inject
    public TransformRegistry(Map<String, Transform.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    public Transform parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Transform transform = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                Transform.Parser payloadParser = parsers.get(type);
                if (payloadParser == null) {
                    throw new AlertsSettingsException("unknown payload type [" + type + "]");
                }
                transform = payloadParser.parse(parser);
            }
        }
        return transform;
    }

    public Transform parse(String type, XContentParser parser) throws IOException {
        Transform.Parser payloadParser = parsers.get(type);
        if (payloadParser == null) {
            throw new AlertsSettingsException("unknown payload type [" + type + "]");
        }
        return payloadParser.parse(parser);
    }
}
