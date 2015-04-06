/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public enum LoggingLevel implements ToXContent {

    ERROR() {
        @Override
        void log(ESLogger logger, String text) {
            logger.error(text);
        }
    },
    WARN() {
        @Override
        void log(ESLogger logger, String text) {
            logger.warn(text);
        }
    },
    INFO() {
        @Override
        void log(ESLogger logger, String text) {
            logger.info(text);
        }
    },
    DEBUG() {
        @Override
        void log(ESLogger logger, String text) {
            logger.debug(text);
        }
    },
    TRACE() {
        @Override
        void log(ESLogger logger, String text) {
            logger.trace(text);
        }
    };

    abstract void log(ESLogger logger, String text);


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(name().toLowerCase(Locale.ROOT));
    }
}
