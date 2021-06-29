/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;

import java.util.Locale;

/**
 * Exception thrown when a problem is encountered while initialising an ILM policy for an index.
 */
public class InitializePolicyException extends ElasticsearchException {

    public InitializePolicyException(String policy, String index, Throwable cause) {
        super(String.format(Locale.ROOT, "unable to initialize policy [%s] for index [%s]", policy, index), cause);
    }
}
