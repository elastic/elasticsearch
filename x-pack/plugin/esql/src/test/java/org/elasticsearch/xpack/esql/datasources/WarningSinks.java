/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.function.Consumer;

/**
 * Warning sinks for tests of resolve-time code, which never writes to response headers and always takes a sink.
 */
public final class WarningSinks {

    /** For tests that expect no notice: the first one fails the test with its text. */
    public static final Consumer<String> FAILING = warning -> {
        throw new AssertionError("unexpected warning: " + warning);
    };

    private WarningSinks() {}
}
