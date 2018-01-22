/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

/**
 * An interface for transforming a String timestamp into epoch_millis.
 */
public interface DateTransformer {
    /**
     *
     * @param timestamp A String representing a timestamp
     * @return Milliseconds since the epoch that the timestamp corresponds to
     * @throws CannotParseTimestampException If the timestamp cannot be parsed
     */
    long transform(String timestamp) throws CannotParseTimestampException;
}
