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
import static org.elasticsearch.test.eql.DataLoader.TEST_SHARD_FAILURES_INDEX;

public abstract class EqlSpecFailingShardsTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {

        // Load EQL validation specs
        return asArray(EqlSpecLoader.load("/test_failing_shards.toml"));
    }

    @Override
    protected String tiebreaker() {
        return "serial_event_id";
    }

    // constructor for "local" rest tests
    public EqlSpecFailingShardsTestCase(
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey,
        Boolean allowPartialSearchResults,
        Boolean allowPartialSequenceResults,
        Boolean expectShardFailures
    ) {
        this(
            TEST_INDEX + "," + TEST_SHARD_FAILURES_INDEX,
            query,
            name,
            eventIds,
            joinKeys,
            size,
            maxSamplesPerKey,
            allowPartialSearchResults,
            allowPartialSequenceResults,
            expectShardFailures
        );
    }

    // constructor for multi-cluster tests
    public EqlSpecFailingShardsTestCase(
        String index,
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey,
        Boolean allowPartialSearch,
        Boolean allowPartialSequenceResults,
        Boolean expectShardFailures
    ) {
        super(
            index,
            query,
            name,
            eventIds,
            joinKeys,
            size,
            maxSamplesPerKey,
            allowPartialSearch,
            allowPartialSequenceResults,
            expectShardFailures
        );
    }
}
