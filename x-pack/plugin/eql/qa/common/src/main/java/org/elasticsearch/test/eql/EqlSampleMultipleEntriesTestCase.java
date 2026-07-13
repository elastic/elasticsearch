/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.List;

import static org.elasticsearch.test.eql.DataLoader.TEST_SAMPLE_MULTI;

public abstract class EqlSampleMultipleEntriesTestCase extends BaseEqlSpecTestCase {

    public EqlSampleMultipleEntriesTestCase(
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
            TEST_SAMPLE_MULTI,
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

    public EqlSampleMultipleEntriesTestCase(
        String index,
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
        super(
            index,
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

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        return asArray(EqlSpecLoader.load("/test_sample_multi.toml"));
    }

    @Override
    protected String tiebreaker() {
        return null;
    }

    @Override
    protected String idField() {
        return "id";
    }

    @Override
    protected int requestFetchSize() {
        // a more relevant fetch_size value for Samples, from algorithm point of view, so we'll mostly test this value
        return frequently() ? 2 : super.requestFetchSize();
    }
}
