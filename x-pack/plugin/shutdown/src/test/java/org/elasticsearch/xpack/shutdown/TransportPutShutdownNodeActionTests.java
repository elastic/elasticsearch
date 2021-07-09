/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.test.ESTestCase;

public class TransportPutShutdownNodeActionTests extends ESTestCase {

    public void testTypeCannotChangeFromRemoveToRestart() {
        assertFalse(
            TransportPutShutdownNodeAction.isTypeChangeAllowed(
                SingleNodeShutdownMetadata.Type.REMOVE,
                SingleNodeShutdownMetadata.Type.RESTART
            )
        );
    }

    public void testTypeCanChangeFromRestartToRemove() {
        assertTrue(
            TransportPutShutdownNodeAction.isTypeChangeAllowed(
                SingleNodeShutdownMetadata.Type.RESTART,
                SingleNodeShutdownMetadata.Type.REMOVE
            )
        );
    }

    public void testSameTypeIsAlwaysAllowed() {
        assertTrue(
            TransportPutShutdownNodeAction.isTypeChangeAllowed(
                SingleNodeShutdownMetadata.Type.RESTART,
                SingleNodeShutdownMetadata.Type.RESTART
            )
        );
        assertTrue(
            TransportPutShutdownNodeAction.isTypeChangeAllowed(
                SingleNodeShutdownMetadata.Type.REMOVE,
                SingleNodeShutdownMetadata.Type.REMOVE
            )
        );
    }
}
