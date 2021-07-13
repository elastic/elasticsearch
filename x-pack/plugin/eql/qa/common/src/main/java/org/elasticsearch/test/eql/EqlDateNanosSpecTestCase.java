/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import static org.elasticsearch.test.eql.DataLoader.DATE_NANOS_INDEX;

import java.util.HashSet;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public abstract class EqlDateNanosSpecTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        return asArray(EqlSpecLoader.load("/test_queries_date_nanos.toml", new HashSet<>()));
    }

    // constructor for "local" rest tests
    public EqlDateNanosSpecTestCase(String query, String name, long[] eventIds) {
        this(DATE_NANOS_INDEX, query, name, eventIds);
    }

    // constructor for multi-cluster tests
    public EqlDateNanosSpecTestCase(String index, String query, String name, long[] eventIds) {
        super(index, query, name, eventIds);
    }

    @Override
    protected String tiebreaker() {
        return "serial_event_id";
    }
}
