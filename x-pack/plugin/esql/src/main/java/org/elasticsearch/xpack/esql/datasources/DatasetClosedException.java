/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.VerificationException;

/**
 * A query named a dataset that an admin closed, or whose data source is closed. It is a client error (400), like the
 * error for a closed index. It is a type of its own so failure telemetry can file it apart from a user's query mistakes
 * and from {@code other}, and the message names the dataset only: reading a dataset needs {@code read} on the dataset, so
 * the caller may not be allowed to learn the name of its data source.
 */
public final class DatasetClosedException extends VerificationException {
    public DatasetClosedException(String dataset) {
        super("dataset [{}] is closed", dataset);
    }
}
