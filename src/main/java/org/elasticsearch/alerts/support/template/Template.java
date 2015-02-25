/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.template;

import org.elasticsearch.alerts.AlertsException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface Template extends ToXContent {

    String render(Map<String, Object> model);

    interface Parser<T extends Template> {

        T parse(XContentParser parser) throws IOException, ParseException;

        public static class ParseException extends AlertsException {

            public ParseException(String msg) {
                super(msg);
            }

            public ParseException(String msg, Throwable cause) {
                super(msg, cause);
            }
        }

    }
}
