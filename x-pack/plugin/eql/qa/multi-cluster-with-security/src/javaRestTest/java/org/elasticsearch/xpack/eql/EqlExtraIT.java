/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlExtraSpecTestCase;

import static org.elasticsearch.test.eql.DataLoader.TEST_EXTRA_INDEX;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterIndex;

public class EqlExtraIT extends EqlExtraSpecTestCase {

    public EqlExtraIT(String query, String name, long[] eventIds, String[] joinKeys) {
        super(remoteClusterIndex(TEST_EXTRA_INDEX), query, name, eventIds, joinKeys);
    }
}
