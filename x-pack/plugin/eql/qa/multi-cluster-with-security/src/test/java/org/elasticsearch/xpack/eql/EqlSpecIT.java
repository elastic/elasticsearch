/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlSpecTestCase;

import static org.elasticsearch.test.eql.DataLoader.TEST_INDEX;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterIndex;

public class EqlSpecIT extends EqlSpecTestCase {

    public EqlSpecIT(String query, String name, long[] eventIds, String[] joinKeys) {
        super(remoteClusterIndex(TEST_INDEX), query, name, eventIds, joinKeys);
    }
}
