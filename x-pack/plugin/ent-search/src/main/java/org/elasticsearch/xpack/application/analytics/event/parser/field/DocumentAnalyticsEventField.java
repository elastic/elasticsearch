/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.Strings.requireNonBlank;

public class DocumentAnalyticsEventField {

    public static ParseField DOCUMENT_FIELD = new ParseField("document");

    public static ParseField DOCUMENT_ID_FIELD = new ParseField("id");

    public static ParseField DOCUMENT_INDEX_FIELD = new ParseField("index");

    private static final ObjectParser<Map<String, String>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        DOCUMENT_FIELD.getPreferredName(),
        HashMap::new
    );

    static {
        PARSER.declareString(
            (b, v) -> b.put(DOCUMENT_ID_FIELD.getPreferredName(), requireNonBlank(v, "field [id] can't be blank")),
            DOCUMENT_ID_FIELD
        );
        PARSER.declareString((b, v) -> b.put(DOCUMENT_INDEX_FIELD.getPreferredName(), v), DOCUMENT_INDEX_FIELD);

        PARSER.declareRequiredFieldSet(DOCUMENT_ID_FIELD.getPreferredName());
    }

    private DocumentAnalyticsEventField() {}

    public static Map<String, String> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }
}
