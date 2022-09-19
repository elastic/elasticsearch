/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analytics.ttest;

import org.elasticsearch.common.io.stream.NamedWriteable;

import java.util.stream.Stream;

/**
 * Base class for t-test aggregation state
 */
public interface TTestState extends NamedWriteable {
    double getValue();

    TTestState reduce(Stream<TTestState> states);
}
