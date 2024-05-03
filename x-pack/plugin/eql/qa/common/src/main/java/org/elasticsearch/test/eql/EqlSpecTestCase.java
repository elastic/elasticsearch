/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.List;

import static org.elasticsearch.test.eql.DataLoader.TEST_INDEX;

public abstract class EqlSpecTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {

        // Load EQL validation specs
        return asArray(EqlSpecLoader.load("/test_queries.toml", "/additional_test_queries.toml", "/test_queries_date.toml"));
    }

    @Override
    protected String tiebreaker() {
        return "serial_event_id";
    }

    // constructor for "local" rest tests
    public EqlSpecTestCase(String query, String name, List<long[]> eventIds, String[] joinKeys, Integer size, Integer maxSamplesPerKey) {
        this(TEST_INDEX, query, name, eventIds, joinKeys, size, maxSamplesPerKey);
    }

    // constructor for multi-cluster tests
    public EqlSpecTestCase(
        String index,
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey
    ) {
        super(index, query, name, eventIds, joinKeys, size, maxSamplesPerKey);
    }
}
