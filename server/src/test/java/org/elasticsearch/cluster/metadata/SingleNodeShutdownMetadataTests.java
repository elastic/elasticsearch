/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.test.ESTestCase;

public class SingleNodeShutdownMetadataTests extends ESTestCase {
    public void testStatusComination() {
        SingleNodeShutdownMetadata.Status status;

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.STALLED
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.STALLED);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.NOT_STARTED
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.IN_PROGRESS);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.NOT_STARTED,
            SingleNodeShutdownMetadata.Status.NOT_STARTED
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.NOT_STARTED);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.IN_PROGRESS,
            SingleNodeShutdownMetadata.Status.COMPLETE
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.IN_PROGRESS);

        status = SingleNodeShutdownMetadata.Status.combine(
            SingleNodeShutdownMetadata.Status.COMPLETE,
            SingleNodeShutdownMetadata.Status.COMPLETE,
            SingleNodeShutdownMetadata.Status.COMPLETE
        );
        assertEquals(status, SingleNodeShutdownMetadata.Status.COMPLETE);

        status = SingleNodeShutdownMetadata.Status.combine();
        assertEquals(status, SingleNodeShutdownMetadata.Status.COMPLETE);
    }
}
