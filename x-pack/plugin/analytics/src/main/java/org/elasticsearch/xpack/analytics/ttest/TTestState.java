/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorReducer;

import java.util.Map;

/**
 * Base class for t-test aggregation state
 */
public interface TTestState extends NamedWriteable {
    double getValue();

    AggregatorReducer getReducer(String name, DocValueFormat format, Map<String, Object> metadata);
}
