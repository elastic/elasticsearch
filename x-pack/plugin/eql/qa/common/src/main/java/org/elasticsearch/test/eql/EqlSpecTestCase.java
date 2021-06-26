/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.eql.DataLoader.TEST_INDEX;

public abstract class EqlSpecTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {

        // Load EQL validation specs
        Set<String> uniqueTestNames = new HashSet<>();
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", uniqueTestNames);
        specs.addAll(EqlSpecLoader.load("/additional_test_queries.toml", uniqueTestNames));
        specs.addAll(EqlSpecLoader.load("/test_queries_date.toml", uniqueTestNames));

        return asArray(specs);
    }

    @Override
    protected String tiebreaker() {
        return "serial_event_id";
    }

    // constructor for "local" rest tests
    public EqlSpecTestCase(String query, String name, long[] eventIds) {
        this(TEST_INDEX, query, name, eventIds);
    }

    // constructor for multi-cluster tests
    public EqlSpecTestCase(String index, String query, String name, long[] eventIds) {
        super(index, query, name, eventIds);
    }
}
