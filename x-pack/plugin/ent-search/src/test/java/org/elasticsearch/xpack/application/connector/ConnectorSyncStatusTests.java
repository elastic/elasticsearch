/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ConnectorSyncStatusTests extends ESTestCase {

    public void testFromString_WithValidSyncStatusString() {
        ConnectorSyncStatus syncStatus = ConnectorTestUtils.getRandomSyncStatus();

        assertThat(ConnectorSyncStatus.fromString(syncStatus.toString()), equalTo(syncStatus));
    }

    public void testFromString_WithInvalidSyncStatusString_ExpectException() {
        expectThrows(IllegalArgumentException.class, () -> ConnectorSyncStatus.fromString("invalid sync status"));
    }

}
