/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.io.stream.NamedWriteable;

import java.util.stream.Stream;

/**
 * Base class for t-test aggregation state
 */
public interface TTestState extends NamedWriteable {
    double getValue();

    TTestState reduce(Stream<TTestState> states);
}
