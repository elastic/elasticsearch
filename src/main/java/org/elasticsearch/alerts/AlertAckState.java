/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * @TODO add jdocs
 */
public enum AlertAckState implements ToXContent {
    NOT_ACKABLE, ///@TODO perhaps null
    NEEDS_ACK,
    ACKED,
    NOT_TRIGGERED;

    public static final String FIELD_NAME = "ack_state";

    @Override
    public String toString() {
        switch (this) {
            case NOT_ACKABLE:
                return "NOT_ACKABLE";
            case NEEDS_ACK:
                return "NEEDS_ACK";
            case ACKED:
                return "ACKED";
            case NOT_TRIGGERED:
                return "NOT_TRIGGERED";
            default:
                return "NOT_ACKABLE";
        }
    }

    public static AlertAckState fromString(String s) {
        switch (s.toUpperCase()) {
            case "NOT_ACKABLE":
                return NOT_ACKABLE;
            case "NEEDS_ACK":
                return NEEDS_ACK;
            case "ACKED":
                return ACKED;
            case "NOT_TRIGGERED":
                return NOT_TRIGGERED;
            default:
                return NOT_ACKABLE;
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
