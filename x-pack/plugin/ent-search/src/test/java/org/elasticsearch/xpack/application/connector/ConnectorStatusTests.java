/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ConnectorStatusTests extends ESTestCase {

    public void testConnectorStatus_WithValidConnectorStatusString() {
        ConnectorStatus connectorStatus = ConnectorTestUtils.getRandomConnectorStatus();

        assertThat(ConnectorStatus.connectorStatus(connectorStatus.toString()), equalTo(connectorStatus));
    }

    public void testConnectorStatus_WithInvalidConnectorStatusString_ExpectIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> ConnectorStatus.connectorStatus("invalid connector status"));
    }

}
