/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.Connector;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ConnectorSyncJobTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        namedWriteableRegistry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(Connector.class, Connector.NAME, Connector::new))
        );
    }

    public final void testRandomSerialization() throws IOException {
        for (int run = 0; run < 10; run++) {
            ConnectorSyncJob syncJob = ConnectorSyncJobTestUtils.getRandomConnectorSyncJob();
            assertTransportSerialization(syncJob);
        }
    }

    private void assertTransportSerialization(ConnectorSyncJob testInstance) throws IOException {
        ConnectorSyncJob deserializedInstance = copyInstance(testInstance);
        assertNotSame(testInstance, deserializedInstance);
        assertThat(testInstance, equalTo(deserializedInstance));
    }

    private ConnectorSyncJob copyInstance(ConnectorSyncJob instance) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, ConnectorSyncJob::new);
    }
}
