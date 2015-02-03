/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.payload;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface Payload extends ToXContent {

    static final Payload NOOP = new Payload() {
        @Override
        public String type() {
            return "noop";
        }

        @Override
        public Map<String, Object> execute(Alert alert, Trigger.Result result, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
            return result.data();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    };

    String type();

    Map<String, Object> execute(Alert alert, Trigger.Result result, DateTime scheduledFireTime, DateTime fireTime) throws IOException;

    static interface Parser<P extends Payload> {

        String type();

        P parse(XContentParser parser) throws IOException;

    }
}
