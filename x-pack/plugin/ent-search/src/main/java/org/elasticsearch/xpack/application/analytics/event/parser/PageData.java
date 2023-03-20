/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser;

import joptsimple.internal.Strings;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class PageData {
    public static final ParseField PAGE_FIELD = new ParseField("page");
    private static final ParseField URL_FIELD = new ParseField("url");
    private static final ParseField TITLE_FIELD = new ParseField("title");
    private static final ParseField REFERRER_FIELD = new ParseField("referrer");
    private static final ConstructingObjectParser<Map<String, Object>, Void> PARSER = new ConstructingObjectParser<>(
        "event_page_data",
        false,
        params -> {
            MapBuilder<String, Object> pageDataBuilder = new MapBuilder<>();

            pageDataBuilder.put(URL_FIELD.getPreferredName(), params[0]);

            if (Strings.isNullOrEmpty((String) params[1]) == false) {
                pageDataBuilder.put(TITLE_FIELD.getPreferredName(), params[1]);
            }

            if (Strings.isNullOrEmpty((String) params[2]) == false) {
                pageDataBuilder.put(REFERRER_FIELD.getPreferredName(), params[2]);
            }

            return pageDataBuilder.immutableMap();
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), URL_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TITLE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REFERRER_FIELD);
    }

    public static Map<String, Object> parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
