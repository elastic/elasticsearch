/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlSampleTestCase;

import static org.elasticsearch.test.eql.DataLoader.TEST_SAMPLE;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterPattern;

public class EqlSampleIT extends EqlSampleTestCase {

    public EqlSampleIT(String query, String name, long[] eventIds, String[] joinKeys) {
        super(remoteClusterPattern(TEST_SAMPLE), query, name, eventIds, joinKeys);
    }
}
