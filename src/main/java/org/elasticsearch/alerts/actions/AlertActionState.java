/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public enum AlertActionState implements ToXContent {
    NO_ACTION_NEEDED,
    ACTION_NEEDED,
    ACTION_UNDERWAY,
    ACTION_PERFORMED;

    public static final String FIELD_NAME = "state";


    @Override
    public String toString(){
        switch (this) {
            case NO_ACTION_NEEDED:
                return "NO_ACTION_NEEDED";
            case ACTION_NEEDED:
                return "ACTION_NEEDED";
            case ACTION_UNDERWAY:
                return "ACTION_UNDERWAY";
            case ACTION_PERFORMED:
                return "ACTION_PERFORMED";
            default:
                return "NO_ACTION_NEEDED";
        }
    }

    public static AlertActionState fromString(String s) {
        switch(s.toUpperCase()) {
            case "NO_ACTION_NEEDED":
                return NO_ACTION_NEEDED;
            case "ACTION_NEEDED":
                return ACTION_NEEDED;
            case "ACTION_UNDERWAY":
                return ACTION_UNDERWAY;
            case "ACTION_PERFORMED":
                return ACTION_PERFORMED;
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown value [" + s + "] for AlertHistoryState" );
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_NAME);
        builder.value(this.toString());
        builder.endObject();
        return builder;
    }
}
