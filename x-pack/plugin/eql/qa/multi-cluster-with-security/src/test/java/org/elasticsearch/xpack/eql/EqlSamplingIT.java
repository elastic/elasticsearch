/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlSamplingTestCase;

import static org.elasticsearch.test.eql.DataLoader.TEST_SAMPLING;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterIndex;

public class EqlSamplingIT extends EqlSamplingTestCase {

    public EqlSamplingIT(String query, String name, long[] eventIds, String[] joinKeys) {
        super(remoteClusterIndex(TEST_SAMPLING), query, name, eventIds, joinKeys);
    }
}
