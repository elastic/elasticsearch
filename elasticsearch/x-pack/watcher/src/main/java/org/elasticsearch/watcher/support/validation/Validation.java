/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.validation;

import org.elasticsearch.common.logging.LoggerMessageFormat;

import java.util.regex.Pattern;

/**
 *
 */
public class Validation {

    private static final Pattern NO_WS_PATTERN = Pattern.compile("\\S+");

    public static Error watchId(String id) {
        if (!NO_WS_PATTERN.matcher(id).matches()) {
            return new Error("Invalid watch id [{}]. Watch id cannot have white spaces", id);
        }
        return null;
    }

    public static Error actionId(String id) {
        if (!NO_WS_PATTERN.matcher(id).matches()) {
            return new Error("Invalid action id [{}]. Action id cannot have white spaces", id);
        }
        return null;
    }

    public static class Error {

        private final String message;

        public Error(String message, Object... args) {
            this.message = LoggerMessageFormat.format(message, args);
        }

        public String message() {
            return message;
        }

        @Override
        public String toString() {
            return message;
        }
    }
}
