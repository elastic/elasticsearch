/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public interface Transform extends ToXContent {

    static final Transform NOOP = new Transform() {
        @Override
        public String type() {
            return "noop";
        }

        @Override
        public Payload apply(Alert alert, Trigger.Result result, Payload payload, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
            return payload;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    };

    String type();

    Payload apply(Alert alert, Trigger.Result result, Payload payload, DateTime scheduledFireTime, DateTime fireTime) throws IOException;

    static interface Parser<P extends Transform> {

        String type();

        P parse(XContentParser parser) throws IOException;

    }

}
