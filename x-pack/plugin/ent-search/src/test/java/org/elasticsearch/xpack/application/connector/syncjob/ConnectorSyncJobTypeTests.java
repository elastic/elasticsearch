/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ConnectorSyncJobTypeTests extends ESTestCase {

    public void testFromString_WithValidSyncJobTypeString() {
        ConnectorSyncJobType syncJobType = ConnectorSyncJobTestUtils.getRandomConnectorJobType();

        assertThat(ConnectorSyncJobType.fromString(syncJobType.toString()), equalTo(syncJobType));
    }

    public void testFromString_WithInvalidSyncJobTypeString_ExpectException() {
        expectThrows(IllegalArgumentException.class, () -> ConnectorSyncJobType.fromString("invalid sync job type"));
    }
}
