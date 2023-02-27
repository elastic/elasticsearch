/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlSampleMultipleEntriesTestCase;

import java.util.List;

import static org.elasticsearch.test.eql.DataLoader.TEST_SAMPLE_MULTI;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterPattern;

public class EqlSampleMultipleEntriesIT extends EqlSampleMultipleEntriesTestCase {

    public EqlSampleMultipleEntriesIT(
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey
    ) {
        super(remoteClusterPattern(TEST_SAMPLE_MULTI), query, name, eventIds, joinKeys, size, maxSamplesPerKey);
    }
}
