/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

public class CannotParseTimestampException extends Exception {

    public CannotParseTimestampException(String message, Throwable cause) {
        super(message, cause);
    }
}
