/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlDateNanosSpecTestCase;

import static org.elasticsearch.test.eql.DataLoader.DATE_NANOS_INDEX;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterIndex;

public class EqlDateNanosIT extends EqlDateNanosSpecTestCase {

    public EqlDateNanosIT(String query, String name, long[] eventIds) {
        super(remoteClusterIndex(DATE_NANOS_INDEX), query, name, eventIds);
    }
}
